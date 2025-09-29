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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.BadJsonQueryException;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Keys;
import org.apache.druid.server.metrics.QueryCountStatsProvider;
import org.apache.druid.server.security.AuthorizationResult;
import org.apache.druid.server.security.AuthorizationUtils;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ForbiddenException;

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
  public static final String ERROR_MESSAGE_TRAILER_HEADER = "X-Error-Message";
  public static final String RESPONSE_COMPLETE_TRAILER_HEADER = "X-Druid-Response-Complete";
  public static final String HEADER_ETAG = "ETag";
  public static final String WRITE_EXCEPTION_BODY_AS_RESPONSE_ROW = "writeExceptionBodyAsResponseRow";

  protected final QueryLifecycleFactory queryLifecycleFactory;
  protected final ObjectMapper jsonMapper;
  protected final QueryScheduler queryScheduler;
  protected final AuthorizerMapper authorizerMapper;

  private final QueryResourceQueryResultPusherFactory queryResultPusherFactory;
  protected final ResourceIOReaderWriterFactory resourceIOReaderWriterFactory;

  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();
  private final AtomicLong timedOutQueryCount = new AtomicLong();
  private final QueryResourceQueryMetricCounter counter = new QueryResourceQueryMetricCounter();

  @Inject
  public QueryResource(
      QueryLifecycleFactory queryLifecycleFactory,
      @Json ObjectMapper jsonMapper,
      QueryScheduler queryScheduler,
      AuthorizerMapper authorizerMapper,
      QueryResourceQueryResultPusherFactory queryResultPusherFactory,
      ResourceIOReaderWriterFactory resourceIOReaderWriterFactory
  )
  {
    this.queryLifecycleFactory = queryLifecycleFactory;
    this.jsonMapper = jsonMapper;
    this.queryScheduler = queryScheduler;
    this.authorizerMapper = authorizerMapper;
    this.queryResultPusherFactory = queryResultPusherFactory;
    this.resourceIOReaderWriterFactory = resourceIOReaderWriterFactory;
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

    AuthorizationResult authResult = AuthorizationUtils.authorizeAllResourceActions(
        req,
        Iterables.transform(datasources, AuthorizationUtils.DATASOURCE_WRITE_RA_GENERATOR),
        authorizerMapper
    );

    if (!authResult.allowAccessWithNoRestriction()) {
      throw new ForbiddenException(authResult.getErrorMessage());
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
    final ResourceIOReaderWriterFactory.ResourceIOReaderWriter io = resourceIOReaderWriterFactory.factorize(req, pretty != null);

    final String currThreadName = Thread.currentThread().getName();
    try {
      final Query<?> query;
      try {
        query = readQuery(req, in, io);
      }
      catch (QueryException e) {
        return io.getResponseWriter().buildNonOkResponse(e.getFailType().getExpectedStatus(), e);
      }

      final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();
      queryLifecycle.initialize(query);
      final String queryThreadName = queryLifecycle.threadName(currThreadName);
      Thread.currentThread().setName(queryThreadName);

      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", queryLifecycle.getQuery());
      }

      final AuthorizationResult authResult;
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

      if (!authResult.allowBasicAccess()) {
        log.info("Query[%s] forbidden due to reason[%s]", query.getId(), authResult.getErrorMessage());
        throw new ForbiddenException(authResult.getErrorMessage());
      }

      final QueryResourceQueryResultPusherFactory.QueryResourceQueryResultPusher pusher =
          queryResultPusherFactory.factorize(counter, req, queryLifecycle, io);
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
      final ResourceIOReaderWriterFactory.ResourceIOReaderWriter ioReaderWriter
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

  static class NativeQueryWriter implements QueryResultPusher.Writer
  {
    private final SerializerProvider serializers;
    private final JsonGenerator jsonGenerator;

    public NativeQueryWriter(final ObjectMapper responseMapper, final OutputStream out) throws IOException
    {
      // Don't use objectWriter.writeValuesAsArray(out), because that causes an end array ] to be written when the
      // writer is closed, even if it's closed in case of an exception. This causes valid JSON to be emitted in case
      // of an exception, which makes it difficult for callers to detect problems. Note: this means that if an error
      // occurs on a Historical (or other data server) after it started to push results to the Broker, the Broker
      // will experience that as "JsonEOFException: Unexpected end-of-input: expected close marker for Array".
      this.serializers = responseMapper.getSerializerProviderInstance();
      this.jsonGenerator = responseMapper.createGenerator(out);
    }

    @Override
    public void writeResponseStart() throws IOException
    {
      jsonGenerator.writeStartArray();
    }

    @Override
    public void writeRow(Object obj) throws IOException
    {
      JacksonUtils.writeObjectUsingSerializerProvider(jsonGenerator, serializers, obj);
    }

    @Override
    public void writeResponseEnd() throws IOException
    {
      jsonGenerator.writeEndArray();
    }

    @Override
    public void close() throws IOException
    {
      jsonGenerator.close();
    }
  }
}
