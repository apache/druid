/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.CountingOutputStream;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.TruncatedResponseContextException;
import org.apache.druid.query.context.ResponseContext;

import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.Closeable;
import java.io.IOException;

public class StreamingHttpResponseAccumulator implements Accumulator<Response, Object>, Closeable
{
  private final HttpServletResponse response;
  private final String queryId;
  private final ResponseContext responseContext;
  private final ObjectMapper jsonMapper;
  private final ResponseContextConfig responseContextConfig;
  private final ResultPusher.ResultsWriter resultsWriter;
  private final DruidNode self;
  private final MediaType contentType;

  private boolean closed = false;
  private boolean initialized = false;
  private CountingOutputStream out = null;
  private ResultPusher.Writer writer = null;

  public StreamingHttpResponseAccumulator(
      HttpServletResponse response,
      String queryId,
      ResponseContext responseContext,
      ObjectMapper jsonMapper,
      ResponseContextConfig responseContextConfig,
      ResultPusher.ResultsWriter resultsWriter,
      DruidNode selfNode,
      MediaType contentType
  )
  {
    this.response = response;
    this.queryId = queryId;
    this.responseContext = responseContext;
    this.jsonMapper = jsonMapper;
    this.responseContextConfig = responseContextConfig;
    this.resultsWriter = resultsWriter;
    this.self = selfNode;
    this.contentType = contentType;
  }

  public long getNumBytesSent()
  {
    return out == null ? 0 : out.getCount();
  }

  public boolean isInitialized()
  {
    return initialized;
  }

  /**
   * Initializes the response.  This is done lazily so that we can put various metadata that we only get once
   * we have some of the response stream into the result.
   * <p>
   * This is called once for each result object, but should only actually happen once.
   *
   * @return boolean if initialization occurred.  False most of the team because initialization only happens once.
   */
  public void initialize()
  {
    if (closed) {
      throw new ISE("Cannot reinitialize after closing.");
    }

    if (!initialized) {
      response.setStatus(HttpServletResponse.SC_OK);

      Object entityTag = responseContext.remove(ResponseContext.Keys.ETAG);
      if (entityTag != null) {
        response.setHeader(QueryResource.HEADER_ETAG, entityTag.toString());
      }

      DirectDruidClient.removeMagicResponseContextFields(responseContext);

      // Limit the response-context header, see https://github.com/apache/druid/issues/2331
      // Note that Response.ResponseBuilder.header(String key,Object value).build() calls value.toString()
      // and encodes the string using ASCII, so 1 char is = 1 byte
      ResponseContext.SerializationResult serializationResult;
      try {
        serializationResult = responseContext.serializeWith(
            jsonMapper,
            responseContextConfig.getMaxResponseContextHeaderSize()
        );
      }
      catch (JsonProcessingException e) {
        QueryResource.log.info(e, "Problem serializing to JSON!?");
        serializationResult = new ResponseContext.SerializationResult("Could not serialize", "Could not serialize");
      }

      if (serializationResult.isTruncated()) {
        final String logToPrint = StringUtils.format(
            "Response Context truncated for id [%s]. Full context is [%s].",
            queryId,
            serializationResult.getFullResult()
        );
        if (responseContextConfig.shouldFailOnTruncatedResponseContext()) {
          QueryResource.log.error(logToPrint);
          throw new QueryInterruptedException(
              new TruncatedResponseContextException(
                  "Serialized response context exceeds the max size[%s]",
                  responseContextConfig.getMaxResponseContextHeaderSize()
              ),
              self.getHostAndPortToUse()
          );
        } else {
          QueryResource.log.warn(logToPrint);
        }
      }

      response.setHeader(QueryResource.HEADER_RESPONSE_CONTEXT, serializationResult.getResult());
      response.setHeader("Content-Type", contentType.toString());

      try {
        out = new CountingOutputStream(response.getOutputStream());
        writer = resultsWriter.makeWriter(out);
      }
      catch (IOException e) {
        throw new RE(e, "Problems setting up response stream for query[%s]!?", queryId);
      }

      try {
        writer.writeResponseStart();
      }
      catch (IOException e) {
        throw new RE(e, "Could not start the response for query[%s]!?", queryId);
      }

      initialized = true;
    }
  }

  @Override
  public Response accumulate(Response retVal, Object in)
  {
    if (!initialized) {
      initialize();
    }

    try {
      writer.writeRow(in);
    }
    catch (IOException ex) {
      QueryResource.NO_STACK_LOGGER.warn(ex, "Unable to write query response.");
      throw new RuntimeException(ex);
    }
    return null;
  }

  public void flush() throws IOException
  {
    if (!initialized) {
      initialize();
    }
    writer.writeResponseEnd();
  }

  @Override
  public void close() throws IOException
  {
    if (closed) {
      return;
    }
    if (initialized && writer != null) {
      writer.close();
    }
    closed = true;
  }
}
