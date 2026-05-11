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

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.BadJsonQueryException;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Keys;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.AsyncContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;

@LazySingleton
@Path("/druid/v2/")
public class QueryResource implements QueryCountStatsProvider
{
  protected static final EmittingLogger log = new EmittingLogger(QueryResource.class);
  public static final EmittingLogger NO_STACK_LOGGER = log.noStackTrace();

  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  protected static final String APPLICATION_SMILE = "application/smile";

  /**
   * HTTP response header name containing {@link ResponseContext} serialized string
   */
  public static final String HEADER_RESPONSE_CONTEXT = "X-Druid-Response-Context";
  public static final String HEADER_IF_NONE_MATCH = "If-None-Match";
  public static final String QUERY_ID_RESPONSE_HEADER = "X-Druid-Query-Id";
  public static final String QUERY_SEGMENT_COUNT_HEADER = "X-Druid-Query-Segment-Count";
  public static final String BROKER_QUERY_TIME_RESPONSE_HEADER = "X-Broker-Query-Time";
  public static final String QUERY_CPU_TIME = "X-Druid-Query-Cpu-Time";
  public static final String NUM_SCANNED_ROWS = "X-Num-Scanned-Rows";
  public static final String QUERY_START_TIME_ATTRIBUTE = "queryStartTime";
  public static final String HEADER_ETAG = "ETag";

  protected final QueryLifecycleFactory queryLifecycleFactory;
  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  protected final ObjectMapper serializeDateTimeAsLongJsonMapper;
  protected final ObjectMapper serializeDateTimeAsLongSmileMapper;
  protected final QueryScheduler queryScheduler;
  protected final AuthorizerMapper authorizerMapper;

  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;

  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();
  private final AtomicLong timedOutQueryCount = new AtomicLong();
  private final QueryResourceQueryMetricCounter counter = new QueryResourceQueryMetricCounter();

  @Inject
  public QueryResource(
      QueryLifecycleFactory queryLifecycleFactory,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QueryScheduler queryScheduler,
      AuthConfig authConfig,
      AuthorizerMapper authorizerMapper,
      ResponseContextConfig responseContextConfig,
      @Self DruidNode selfNode
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.serializeDateTimeAsLongJsonMapper = serializeDataTimeAsLong(jsonMapper);
    this.serializeDateTimeAsLongSmileMapper = serializeDataTimeAsLong(smileMapper);
    this.queryScheduler = queryScheduler;
    this.authorizerMapper = authorizerMapper;
    this.responseContextConfig = responseContextConfig;
    this.selfNode = selfNode;
  }

  @DELETE
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response cancelQuery(@PathParam("id") String queryId, @Context final HttpServletRequest req)
  {
    if (log.isDebugEnabled()) {
      log.debug("Received cancel request for query [%s]", queryId);
    }
    Set<String> datasources = queryScheduler.getQueryDatasources(queryId);
    if (datasources == null) {
      log.warn("QueryId [%s] not registered with QueryScheduler, cannot cancel", queryId);
      datasources = new TreeSet<>();
    }

    Access authResult = AuthorizationUtils.authorizeAllResourceActions(
        req,
        Iterables.transform(datasources, AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR),
        authorizerMapper
    );

    if (!authResult.isAllowed()) {
      throw new ForbiddenException(authResult.toString());
    }

    queryScheduler.cancelQuery(queryId);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  @Nullable
  public Response doPost(
      final InputStream in,
      @QueryParam("pretty") final String pretty,
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();

    final ResourceIOReaderWriter io = createResourceIOReaderWriter(req, pretty != null);

    final String currThreadName = Thread.currentThread().getName();
    final long queryStartTime = System.nanoTime();
    try {
      final Query<?> query;
      try {
        req.setAttribute(QUERY_START_TIME_ATTRIBUTE, queryStartTime);
        query = readQuery(req, in, io);
      }
      catch (QueryException e) {
        return io.getResponseWriter().buildNonOkResponse(e.getFailType().getExpectedStatus(), e);
      }

      queryLifecycle.initialize(query);
      final String queryThreadName = queryLifecycle.threadName(currThreadName);
      Thread.currentThread().setName(queryThreadName);

      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", queryLifecycle.getQuery());
      }

      final Access authResult;
      try {
        authResult = queryLifecycle.authorize(req);
      }
      catch (RuntimeException e) {
        final QueryException qe;

        if (e instanceof QueryException) {
          qe = (QueryException) e;
        } else {
          qe = new QueryInterruptedException(e);
        }

        return io.getResponseWriter().buildNonOkResponse(qe.getFailType().getExpectedStatus(), qe);
      }

      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.toString());
      }

      final QueryResourceQueryResultPusher pusher = new QueryResourceQueryResultPusher(req, queryLifecycle, io);
      return pusher.push();
    }
    catch (Exception e) {
      if (e instanceof ForbiddenException && !req.isAsyncStarted()) {
        // We can only pass through the Forbidden exception if we haven't started async yet.
        throw e;
      }
      log.warn(e, "Uncaught exception from query processing.  This should be caught and handled directly.");

      // Just fall back to the async context.
      AsyncContext asyncContext = req.startAsync();
      try {
        final HttpServletResponse response = (HttpServletResponse) asyncContext.getResponse();
        // If the response is committed, we actually processed and started doing things with the request,
        // so the best we can do is just complete in the finally and hope for the best.
        if (!response.isCommitted()) {
          response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
          response.setContentType(MediaType.APPLICATION_JSON);
          try (OutputStream out = response.getOutputStream()) {
            final QueryException responseException = new QueryException(
                QueryException.UNKNOWN_EXCEPTION_ERROR_CODE,
                "Unhandled exception made it to the top",
                e.getClass().getName(),
                req.getRemoteHost()
            );
            out.write(jsonMapper.writeValueAsBytes(responseException));
          }
        }
        return null;
      }
      finally {
        asyncContext.complete();
      }
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  public interface QueryMetricCounter
  {
    void incrementSuccess();

    void incrementFailed();

    void incrementInterrupted();

    void incrementTimedOut();
  }

  private Query<?> readQuery(
      final HttpServletRequest req,
      final InputStream in,
      final ResourceIOReaderWriter ioReaderWriter
  ) throws IOException
  {
    final Query<?> baseQuery;
    try {
      baseQuery = ioReaderWriter.getRequestMapper().readValue(in, Query.class);
    }
    catch (JsonParseException e) {
      throw new BadJsonQueryException(e);
    }

    String prevEtag = getPreviousEtag(req);
    if (prevEtag == null) {
      return baseQuery;
    }

    return baseQuery.withOverriddenContext(
        QueryContexts.override(
            baseQuery.getContext(),
            HEADER_IF_NONE_MATCH,
            prevEtag
        )
    );
  }

  private static String getPreviousEtag(final HttpServletRequest req)
  {
    return req.getHeader(HEADER_IF_NONE_MATCH);
  }

  protected ObjectMapper serializeDataTimeAsLong(ObjectMapper mapper)
  {
    return mapper.copy().registerModule(new SimpleModule().addSerializer(DateTime.class, new DateTimeSerializer()));
  }

  protected ResourceIOReaderWriter createResourceIOReaderWriter(HttpServletRequest req, boolean pretty)
  {
    String requestType = req.getContentType();
    String acceptHeader = req.getHeader("Accept");

    // response type defaults to Content-Type if 'Accept' header not provided
    String responseType = Strings.isNullOrEmpty(acceptHeader) ? requestType : acceptHeader;

    boolean isRequestSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType) || APPLICATION_SMILE.equals(
        requestType);
    boolean isResponseSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(responseType)
                              || APPLICATION_SMILE.equals(responseType);

    return new ResourceIOReaderWriter(
        isRequestSmile ? smileMapper : jsonMapper,
        new ResourceIOWriter(
            isResponseSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON,
            isResponseSmile ? smileMapper : jsonMapper,
            isResponseSmile ? serializeDateTimeAsLongSmileMapper : serializeDateTimeAsLongJsonMapper,
            pretty
        )
    );
  }

  protected static class ResourceIOReaderWriter
  {
    private final ObjectMapper requestMapper;
    private final ResourceIOWriter writer;

    public ResourceIOReaderWriter(ObjectMapper requestMapper, ResourceIOWriter writer)
    {
      this.requestMapper = requestMapper;
      this.writer = writer;
    }

    public ObjectMapper getRequestMapper()
    {
      return requestMapper;
    }

    public ResourceIOWriter getResponseWriter()
    {
      return writer;
    }
  }

  protected static class ResourceIOWriter
  {
    private final String responseType;
    private final ObjectMapper inputMapper;
    private final ObjectMapper serializeDateTimeAsLongInputMapper;
    private final boolean isPretty;

    ResourceIOWriter(
        String responseType,
        ObjectMapper inputMapper,
        ObjectMapper serializeDateTimeAsLongInputMapper,
        boolean isPretty
    )
    {
      this.responseType = responseType;
      this.inputMapper = inputMapper;
      this.serializeDateTimeAsLongInputMapper = serializeDateTimeAsLongInputMapper;
      this.isPretty = isPretty;
    }

    String getResponseType()
    {
      return responseType;
    }

    ObjectWriter newOutputWriter(
        @Nullable QueryToolChest<?, Query<?>> toolChest,
        @Nullable Query<?> query,
        boolean serializeDateTimeAsLong
    )
    {
      final ObjectMapper mapper = serializeDateTimeAsLong ? serializeDateTimeAsLongInputMapper : inputMapper;
      final ObjectMapper decoratedMapper;
      if (toolChest != null) {
        decoratedMapper = toolChest.decorateObjectMapper(mapper, Preconditions.checkNotNull(query, "query"));
      } else {
        decoratedMapper = mapper;
      }
      return isPretty ? decoratedMapper.writerWithDefaultPrettyPrinter() : decoratedMapper.writer();
    }

    Response ok(Object object) throws IOException
    {
      return Response.ok(newOutputWriter(null, null, false).writeValueAsString(object), responseType).build();
    }

    Response gotError(Exception e) throws IOException
    {
      return buildNonOkResponse(
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(e)
      );
    }

    Response buildNonOkResponse(int status, Exception e) throws JsonProcessingException
    {
      return Response.status(status)
                     .type(responseType)
                     .entity(newOutputWriter(null, null, false).writeValueAsBytes(e))
                     .build();
    }
  }

  @Override
  public long getSuccessfulQueryCount()
  {
    return successfulQueryCount.get();
  }

  @Override
  public long getFailedQueryCount()
  {
    return failedQueryCount.get();
  }

  @Override
  public long getInterruptedQueryCount()
  {
    return interruptedQueryCount.get();
  }

  @Override
  public long getTimedOutQueryCount()
  {
    return timedOutQueryCount.get();
  }

  @VisibleForTesting
  public static void transferEntityTag(ResponseContext context, Response.ResponseBuilder builder)
  {
    Object entityTag = context.remove(Keys.ETAG);
    if (entityTag != null) {
      builder.header(HEADER_ETAG, entityTag);
    }
  }

  private class QueryResourceQueryMetricCounter implements QueryMetricCounter
  {
    @Override
    public void incrementSuccess()
    {
      successfulQueryCount.incrementAndGet();
    }

    @Override
    public void incrementFailed()
    {
      failedQueryCount.incrementAndGet();
    }

    @Override
    public void incrementInterrupted()
    {
      interruptedQueryCount.incrementAndGet();
    }

    @Override
    public void incrementTimedOut()
    {
      timedOutQueryCount.incrementAndGet();
    }
  }

  private class QueryResourceQueryResultPusher extends QueryResultPusher
  {
    private final HttpServletRequest req;
    private final QueryLifecycle queryLifecycle;
    private final ResourceIOReaderWriter io;

    public QueryResourceQueryResultPusher(
        HttpServletRequest req,
        QueryLifecycle queryLifecycle,
        ResourceIOReaderWriter io
    )
    {
      super(
          req,
          QueryResource.this.jsonMapper,
          QueryResource.this.responseContextConfig,
          QueryResource.this.selfNode,
          QueryResource.this.counter,
          queryLifecycle.getQueryId(),
          MediaType.valueOf(io.getResponseWriter().getResponseType()),
          ImmutableMap.of()
      );
      this.req = req;
      this.queryLifecycle = queryLifecycle;
      this.io = io;
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
            return Response.status(Status.NOT_MODIFIED);
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
          final ObjectWriter objectWriter = queryLifecycle.newOutputWriter(io);
          final SequenceWriter sequenceWriter = objectWriter.writeValuesAsArray(out);
          return new Writer()
          {

            @Override
            public void writeResponseStart()
            {
              // Do nothing
            }

            @Override
            public void writeRow(Object obj) throws IOException
            {
              sequenceWriter.write(obj);
            }

            @Override
            public void writeResponseEnd()
            {
              // Do nothing
            }

            @Override
            public void close() throws IOException
            {
              sequenceWriter.close();
            }
          };
        }

        @Override
        public void recordSuccess(long numBytes)
        {

        }

        @Override
        public void recordSuccess(long numBytes, long numRowsScanned, long cpuTimeInMillis)
        {
          queryLifecycle.emitLogsAndMetrics(null, req.getRemoteAddr(), numBytes, numRowsScanned, cpuTimeInMillis);
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
      final ObjectWriter objectWriter = queryLifecycle.newOutputWriter(io);
      out.write(objectWriter.writeValueAsBytes(e));
    }
  }
}
