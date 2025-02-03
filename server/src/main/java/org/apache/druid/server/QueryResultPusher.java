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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ErrorResponse;
import org.apache.druid.error.QueryExceptionCompat;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.TruncatedResponseContextException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.ForbiddenException;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public abstract class QueryResultPusher
{
  private static final Logger log = new Logger(QueryResultPusher.class);

  private final HttpServletRequest request;
  private final String queryId;
  private final ObjectMapper jsonMapper;
  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;
  private final QueryResource.QueryMetricCounter counter;
  private final MediaType contentType;
  private final Map<String, String> extraHeaders;

  private StreamingHttpResponseAccumulator accumulator;
  private AsyncContext asyncContext;
  private HttpServletResponse response;

  public QueryResultPusher(
      HttpServletRequest request,
      ObjectMapper jsonMapper,
      ResponseContextConfig responseContextConfig,
      DruidNode selfNode,
      QueryResource.QueryMetricCounter counter,
      String queryId,
      MediaType contentType,
      Map<String, String> extraHeaders
  )
  {
    this.request = request;
    this.queryId = queryId;
    this.jsonMapper = jsonMapper;
    this.responseContextConfig = responseContextConfig;
    this.selfNode = selfNode;
    this.counter = counter;
    this.contentType = contentType;
    this.extraHeaders = extraHeaders;
  }

  /**
   * Builds a ResultsWriter to start the lifecycle of the QueryResultPusher.  The ResultsWriter encapsulates the logic
   * to run the query, serialize it and also report success/failure.
   * <p>
   * This response must not be null.  The job of this ResultsWriter is largely to provide lifecycle management to
   * the query running and reporting, so this object must never be null.
   * <p>
   * This start() method should do as little work as possible, it should really just make the ResultsWriter and return.
   *
   * @return a new ResultsWriter
   */
  public abstract ResultsWriter start();

  public abstract void writeException(Exception e, OutputStream out) throws IOException;

  /**
   * Pushes results out.  Can sometimes return a JAXRS Response object instead of actually pushing to the output
   * stream, primarily for error handling that occurs before switching the servlet to asynchronous mode.
   *
   * @return null if the response has already been handled and pushed out, or a non-null Response object if it expects
   * the container to put the bytes on the wire.
   */
  @Nullable
  public Response push()
  {
    ResultsWriter resultsWriter = null;
    try {
      resultsWriter = start();

      final Response.ResponseBuilder startResponse = resultsWriter.start();
      if (startResponse != null) {
        startResponse.header(QueryResource.QUERY_ID_RESPONSE_HEADER, queryId);
        for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
          startResponse.header(entry.getKey(), entry.getValue());
        }
        return startResponse.build();
      }

      final QueryResponse<Object> queryResponse = resultsWriter.getQueryResponse();
      final Sequence<Object> results = queryResponse.getResults();

      // We use an async context not because we are actually going to run this async, but because we want to delay
      // the decision of what the response code should be until we have gotten the first few data points to return.
      // Returning a Response object from this point forward requires that object to know the status code, which we
      // don't actually know until we are in the accumulator, but if we try to return a Response object from the
      // accumulator, we cannot properly stream results back, because the accumulator won't release control of the
      // Response until it has consumed the underlying Sequence.
      asyncContext = request.startAsync();
      response = (HttpServletResponse) asyncContext.getResponse();
      response.setHeader(QueryResource.QUERY_ID_RESPONSE_HEADER, queryId);
      for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
        response.setHeader(entry.getKey(), entry.getValue());
      }

      accumulator = new StreamingHttpResponseAccumulator(queryResponse.getResponseContext(), resultsWriter);

      results.accumulate(null, accumulator);
      accumulator.flush();

      counter.incrementSuccess();
      accumulator.close();
      resultsWriter.recordSuccess(accumulator.getNumBytesSent(), accumulator.rowsScanned, accumulator.cpuConsumedMillis);
    }
    catch (DruidException e) {
      // Less than ideal. But, if we return the result as JSON, this is
      // the only way for the security filter to know that, yes, it is OK
      // to show the user this error even if we didn't get to the step where
      // we did a security check.
      request.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
      return handleDruidException(resultsWriter, e);
    }
    catch (QueryException e) {
      return handleQueryException(resultsWriter, e);
    }
    catch (RuntimeException re) {
      if (re instanceof ForbiddenException) {
        // Forbidden exceptions are special, they get thrown instead of serialized.  They happen before the response
        // has been committed because the response is committed after results are returned.  And, if we started
        // returning results before a ForbiddenException gets thrown, that means that we've already leaked stuff
        // that should not have been leaked.  I.e. it means, we haven't validated the authorization early enough.
        if (response != null && response.isCommitted()) {
          log.error(re, "Got a forbidden exception for query [%s] after the response was already committed.", queryId);
        }
        throw re;
      }
      return handleQueryException(resultsWriter, new QueryInterruptedException(re));
    }
    catch (IOException ioEx) {
      return handleQueryException(resultsWriter, new QueryInterruptedException(ioEx));
    }
    finally {
      if (accumulator != null) {
        try {
          accumulator.close();
        }
        catch (IOException e) {
          log.warn(e, "Suppressing exception closing accumulator for query [%s]", queryId);
        }
      }
      if (resultsWriter == null) {
        log.warn("resultsWriter was null for query [%s], work was maybe done in start() that shouldn't be.", queryId);
      } else {
        try {
          resultsWriter.close();
        }
        catch (IOException e) {
          log.warn(e, "Suppressing exception closing accumulator for query [%s]", queryId);
        }
      }
      if (asyncContext != null) {
        asyncContext.complete();
      }
    }
    return null;
  }

  @Nullable
  private Response handleQueryException(ResultsWriter resultsWriter, QueryException e)
  {
    return handleDruidException(resultsWriter, DruidException.fromFailure(new QueryExceptionCompat(e)));
  }

  private Response handleDruidException(ResultsWriter resultsWriter, DruidException e)
  {
    if (resultsWriter != null) {
      resultsWriter.recordFailure(e);
      counter.incrementFailed();

      if (accumulator != null && accumulator.isInitialized()) {
        // We already started sending a response when we got the error message.  In this case we just give up
        // and hope that the partial stream generates a meaningful failure message for our client.  We could consider
        // also throwing the exception body into the response to make it easier for the client to choke if it manages
        // to parse a meaningful object out, but that's potentially an API change so we leave that as an exercise for
        // the future.
        return null;
      }
    }

    switch (e.getCategory()) {
      case INVALID_INPUT:
      case UNAUTHORIZED:
      case RUNTIME_FAILURE:
      case CANCELED:
        counter.incrementInterrupted();
        break;
      case CAPACITY_EXCEEDED:
      case UNSUPPORTED:
      case UNCATEGORIZED:
      case DEFENSIVE:
        counter.incrementFailed();
        break;
      case TIMEOUT:
        counter.incrementTimedOut();
        break;
    }

    if (response == null) {
      final Response.ResponseBuilder bob = Response
          .status(e.getStatusCode())
          .type(contentType)
          .entity(new ErrorResponse(e));

      bob.header(QueryResource.QUERY_ID_RESPONSE_HEADER, queryId);
      for (Map.Entry<String, String> entry : extraHeaders.entrySet()) {
        bob.header(entry.getKey(), entry.getValue());
      }

      return bob.build();
    } else {
      if (response.isCommitted()) {
        QueryResource.NO_STACK_LOGGER.warn(e, "Response was committed without the accumulator writing anything!?");
      }

      response.setStatus(e.getStatusCode());
      response.setHeader("Content-Type", contentType.toString());
      try (ServletOutputStream out = response.getOutputStream()) {
        writeException(e, out);
      }
      catch (IOException ioException) {
        log.warn(
            ioException,
            "Suppressing IOException thrown sending error response for query [%s]",
            queryId
        );
      }
      return null;
    }
  }

  public interface ResultsWriter extends Closeable
  {
    /**
     * Runs the query and prepares the QueryResponse to be returned
     * <p>
     * This also serves as a hook for any logic that runs on the metadata from a QueryResponse.  If this method
     * returns {@code null} then the Pusher can continue with normal logic.  If this method chooses to return
     * a ResponseBuilder, then the Pusher will attach any extra metadata it has to the Response and return
     * the response built from the Builder without attempting to process the results of the query.
     * <p>
     * In all cases, {@link #close()} should be called on this object.
     *
     * @return QueryResponse or null if no more work to do.
     */
    @Nullable
    Response.ResponseBuilder start();

    /**
     * Gets the results of running the query.  {@link #start} must be called before this method is called.
     *
     * @return the results of running the query as prepared by the {@link #start()} method
     */
    QueryResponse<Object> getQueryResponse();

    Writer makeWriter(OutputStream out) throws IOException;

    void recordSuccess(long numBytes);

    void recordSuccess(long numBytes, long numRowsScanned, long cpuTimeInMillis);

    void recordFailure(Exception e);
  }

  public interface Writer extends Closeable
  {
    /**
     * Start of the response, called once per writer.
     */
    void writeResponseStart() throws IOException;

    /**
     * Write a row
     *
     * @param obj object representing the row
     */
    void writeRow(Object obj) throws IOException;

    /**
     * End of the response. Must allow the user to know that they have read all data successfully.
     */
    void writeResponseEnd() throws IOException;
  }

  public class StreamingHttpResponseAccumulator implements Accumulator<Response, Object>, Closeable
  {
    private final ResponseContext responseContext;
    private final ResultsWriter resultsWriter;

    private boolean closed = false;
    private boolean initialized = false;
    private CountingOutputStream out = null;
    private Writer writer = null;

    private Long rowsScanned;
    private Long cpuConsumedMillis;
    private Long querySegmentCount;
    private Long brokerQueryTime;

    public StreamingHttpResponseAccumulator(
        ResponseContext responseContext,
        ResultsWriter resultsWriter
    )
    {
      this.responseContext = responseContext;
      this.resultsWriter = resultsWriter;
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
     * It is okay for this to be called multiple times.
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
          log.info(e, "Problem serializing to JSON!?");
          serializationResult = new ResponseContext.SerializationResult("Could not serialize", "Could not serialize");
        }

        if (serializationResult.isTruncated()) {
          final String logToPrint = StringUtils.format(
              "Response Context truncated for id [%s]. Full context is [%s].",
              queryId,
              serializationResult.getFullResult()
          );
          if (responseContextConfig.shouldFailOnTruncatedResponseContext()) {
            log.error(logToPrint);
            throw new QueryInterruptedException(
                new TruncatedResponseContextException(
                    "Serialized response context exceeds the max size[%s]",
                    responseContextConfig.getMaxResponseContextHeaderSize()
                ),
                selfNode.getHostAndPortToUse()
            );
          } else {
            log.warn(logToPrint);
          }
        }

        Object startTime = request.getAttribute(QueryResource.QUERY_START_TIME_ATTRIBUTE);

        rowsScanned = responseContext.getValueOrDefaultZero(ResponseContext::getRowScanCount);
        cpuConsumedMillis = TimeUnit.NANOSECONDS.toMillis(responseContext.getValueOrDefaultZero(ResponseContext::getCpuNanos));
        querySegmentCount = responseContext.getValueOrDefaultZero(ResponseContext::getQuerySegmentCount);
        brokerQueryTime = TimeUnit.NANOSECONDS.toMillis(Objects.nonNull(startTime) ? System.nanoTime() - (Long) startTime : -1L);

        response.setHeader(QueryResource.NUM_SCANNED_ROWS, String.valueOf(rowsScanned));
        // Emit Cpu time as a response header. Note that it doesn't include Cpu spent on serializing the response.
        // Druid streams the output which results in the header being sent out before the response is fully serialized and sent to the client.
        response.setHeader(QueryResource.QUERY_CPU_TIME, String.valueOf(cpuConsumedMillis));
        response.setHeader(QueryResource.QUERY_SEGMENT_COUNT_HEADER, String.valueOf(querySegmentCount));
        response.setHeader(QueryResource.BROKER_QUERY_TIME_RESPONSE_HEADER, String.valueOf(brokerQueryTime));
        response.setHeader(QueryResource.HEADER_RESPONSE_CONTEXT, serializationResult.getResult());
        response.setContentType(contentType.toString());

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
    @Nullable
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
}
