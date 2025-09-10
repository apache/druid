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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.query.context.ResponseContext;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Factory for creating instances of {@link QueryResourceQueryResultPusher}.
 */
public class QueryResourceQueryResultPusherFactory
{
  protected final ObjectMapper jsonMapper;
  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;

  @Inject
  public QueryResourceQueryResultPusherFactory(
      @Json ObjectMapper jsonMapper,
      ResponseContextConfig responseContextConfig,
      @Self DruidNode selfNode
  )
  {
    this.jsonMapper = jsonMapper;
    this.responseContextConfig = responseContextConfig;
    this.selfNode = selfNode;
  }

  /**
   * Creates a new instance of {@link QueryResourceQueryResultPusher}.
   */
  public QueryResourceQueryResultPusher factorize(
      final QueryResource.QueryMetricCounter counter,
      final HttpServletRequest req,
      final QueryLifecycle queryLifecycle,
      final ResourceIOReaderWriterFactory.ResourceIOReaderWriter io
  )
  {
    return new QueryResourceQueryResultPusher(
        jsonMapper,
        responseContextConfig,
        selfNode,
        counter,
        req,
        queryLifecycle,
        io
    );
  }

  /**
   * Handles query results for {@link QueryResource}, pushing the results to the client.
   * <p>
   * It uses the provided {@link QueryLifecycle} to execute the query and writes the results via {@link QueryResource.NativeQueryWriter}.
   */
  public static class QueryResourceQueryResultPusher extends QueryResultPusher
  {
    private final HttpServletRequest req;
    private final QueryLifecycle queryLifecycle;
    private final ResourceIOReaderWriterFactory.ResourceIOReaderWriter io;
    private final QueryResource.QueryMetricCounter counter;

    public QueryResourceQueryResultPusher(
        final ObjectMapper jsonMapper,
        final ResponseContextConfig responseContextConfig,
        final DruidNode selfNode,
        final QueryResource.QueryMetricCounter counter,
        final HttpServletRequest req,
        final QueryLifecycle queryLifecycle,
        final ResourceIOReaderWriterFactory.ResourceIOReaderWriter io
    )
    {
      super(
          req,
          jsonMapper,
          responseContextConfig,
          selfNode,
          counter,
          queryLifecycle.getQueryId(),
          MediaType.valueOf(io.getResponseWriter().getResponseType()),
          ImmutableMap.of()
      );
      this.req = req;
      this.queryLifecycle = queryLifecycle;
      this.io = io;
      this.counter = counter;
    }

    @Override
    public ResultsWriter start()
    {
      return new ResultsWriter()
      {
        private QueryResponse<Object> queryResponse;

        @Override
        public Response.ResponseBuilder start()
        {
          queryResponse = queryLifecycle.execute();
          final ResponseContext responseContext = queryResponse.getResponseContext();
          final String prevEtag = getPreviousEtag(req);

          if (prevEtag != null && prevEtag.equals(responseContext.getEntityTag())) {
            queryLifecycle.emitLogsAndMetrics(null, req.getRemoteAddr(), -1, -1, -1);
            counter.incrementSuccess();
            return Response.status(Response.Status.NOT_MODIFIED);
          }

          return null;
        }

        @Override
        public QueryResponse<Object> getQueryResponse()
        {
          return queryResponse;
        }

        @Override
        public Writer makeWriter(OutputStream out) throws IOException
        {
          return new QueryResource.NativeQueryWriter(queryLifecycle.newOutputWriter(io), out);
        }

        @Override
        public void recordSuccess(long numBytes, long numRowsScanned, long cpuConsumedMillis)
        {
          queryLifecycle.emitLogsAndMetrics(null, req.getRemoteAddr(), numBytes, numRowsScanned, cpuConsumedMillis);
        }

        @Override
        public void recordFailure(Exception e)
        {
          queryLifecycle.emitLogsAndMetrics(e, req.getRemoteAddr(), -1, -1, -1);
        }

        @Override
        public void close()
        {

        }
      };
    }

    @Override
    public void writeException(Exception e, OutputStream out) throws IOException
    {
      final ObjectMapper objectMapper = queryLifecycle.newOutputWriter(io);
      out.write(objectMapper.writeValueAsBytes(e));
    }
  }

  private static String getPreviousEtag(final HttpServletRequest req)
  {
    return req.getHeader(QueryResource.HEADER_IF_NONE_MATCH);
  }
}
