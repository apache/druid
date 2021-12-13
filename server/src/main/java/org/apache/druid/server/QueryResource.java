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
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.BadJsonQueryException;
import org.apache.druid.query.BadQueryException;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryCapacityExceededException;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryException;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryUnsupportedException;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.TruncatedResponseContextException;
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
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayOutputStream;
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
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  protected static final String APPLICATION_SMILE = "application/smile";

  /**
   * HTTP response header name containing {@link ResponseContext} serialized string
   */
  public static final String HEADER_RESPONSE_CONTEXT = "X-Druid-Response-Context";

  public static final String HEADER_IF_NONE_MATCH = "If-None-Match";
  public static final String HEADER_QUERY_ID = "X-Druid-Query-Id";
  public static final String HEADER_ETAG = "ETag";
  public static final String YES_VALUE = "Yes";

  protected final QueryLifecycleFactory queryLifecycleFactory;
  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  protected final ObjectMapper serializeDateTimeAsLongJsonMapper;
  protected final ObjectMapper serializeDateTimeAsLongSmileMapper;
  protected final QueryScheduler queryScheduler;
  protected final AuthConfig authConfig;
  protected final AuthorizerMapper authorizerMapper;

  private final ResponseContextConfig responseContextConfig;
  private final DruidNode selfNode;

  private final AtomicLong successfulQueryCount = new AtomicLong();
  private final AtomicLong failedQueryCount = new AtomicLong();
  private final AtomicLong interruptedQueryCount = new AtomicLong();
  private final AtomicLong timedOutQueryCount = new AtomicLong();

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
    this.authConfig = authConfig;
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
  public Response doPost(
      final InputStream in,
      @QueryParam("pretty") final String pretty,

      // used to get request content-type, Accept header, remote address and auth-related headers
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();
    Query<?> query = null;

    final ResourceIOReaderWriter ioReaderWriter = createResourceIOReaderWriter(req, pretty != null);

    final String currThreadName = Thread.currentThread().getName();
    try {
      // Read and preserve the query as received from the client.
      final Query<?> receivedQuery = readQuery(req, in, ioReaderWriter);

      // Rewrite the query with the etag, if available.
      final Query<?> initQuery;
      final String prevEtag = getPreviousEtag(req);
      if (prevEtag == null) {
        initQuery = receivedQuery;
      } else {
        initQuery = receivedQuery.withOverriddenContext(
            ImmutableMap.of(HEADER_IF_NONE_MATCH, prevEtag)
        );
      }

      // Initialize the lifecycle, which rewrites the query with context.
      queryLifecycle.initialize(initQuery);

      // Initialize the query stats using the original received query.
      QueryLifecycle.LifecycleStats stats = queryLifecycle.stats();
      stats.onStart(selfNode, req.getRemoteAddr(), receivedQuery);

      // Obtained the rewritten query.
      query = queryLifecycle.getQuery();
      final String queryId = query.getId();
      final String queryThreadName = StringUtils.format(
          "%s[%s_%s_%s]",
          currThreadName,
          query.getType(),
          query.getDataSource().getTableNames(),
          queryId
      );

      Thread.currentThread().setName(queryThreadName);

      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", query);
      }

      final Access authResult = queryLifecycle.authorize(req);
      if (!authResult.isAllowed()) {
        throw new ForbiddenException(authResult.toString());
      }

      final QueryResponse<?> queryResponse = queryLifecycle.execute();
      final ResponseContext responseContext = queryResponse.getResponseContextEarly();
      stats.setResponseContext(responseContext);

      if (prevEtag != null && prevEtag.equals(responseContext.getEntityTag())) {
        queryLifecycle.emitLogsAndMetrics(null);
        successfulQueryCount.incrementAndGet();
        return Response.notModified().build();
      }

      final QueryOutput queryOutput = new QueryOutput(
          queryLifecycle,
          ioReaderWriter,
          query,
          queryResponse.getResults(),
          currThreadName
      );
      try {
        Response.ResponseBuilder responseBuilder = Response
            .ok(
                queryOutput,
                ioReaderWriter.getResponseWriter().getResponseType()
            )
            .header(HEADER_QUERY_ID, queryId);

        transferEntityTag(responseContext, responseBuilder);

        // Limit the response-context header, see https://github.com/apache/druid/issues/2331
        // Note that Response.ResponseBuilder.header(String key,Object value).build() calls value.toString()
        // and encodes the string using ASCII, so 1 char is = 1 byte
        final ResponseContext.SerializationResult serializationResult = responseContext.toHeader(
            jsonMapper,
            responseContextConfig.getMaxResponseContextHeaderSize()
        );

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

        return responseBuilder
            .header(HEADER_RESPONSE_CONTEXT, serializationResult.getResult())
            .build();
      }
      catch (QueryException e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        queryOutput.close();
        throw e;
      }
      catch (Exception e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        queryOutput.close();
        throw new RuntimeException(e);
      }
      finally {
        // do not close yielder here, since we do not want to close the yielder prior to
        // StreamingOutput having iterated over all the results
      }
    }
    catch (QueryInterruptedException e) {
      interruptedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(e);
      return ioReaderWriter.getResponseWriter().gotError(e);
    }
    catch (QueryTimeoutException timeout) {
      timedOutQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(timeout);
      return ioReaderWriter.getResponseWriter().gotTimeout(timeout);
    }
    catch (QueryCapacityExceededException cap) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(cap);
      return ioReaderWriter.getResponseWriter().gotLimited(cap);
    }
    catch (QueryUnsupportedException unsupported) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(unsupported);
      return ioReaderWriter.getResponseWriter().gotUnsupported(unsupported);
    }
    catch (BadJsonQueryException | ResourceLimitExceededException e) {
      interruptedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(e);
      return ioReaderWriter.getResponseWriter().gotBadQuery(e);
    }
    catch (ForbiddenException e) {
      // don't do anything for an authorization failure, ForbiddenExceptionMapper will catch this later and
      // send an error response if this is thrown.
      throw e;
    }
    catch (Exception e) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(e);

      log.noStackTrace()
         .makeAlert(e, "Exception handling request")
         .addData("query", query != null ? jsonMapper.writeValueAsString(query) : "unparseable query")
         .addData("peer", req.getRemoteAddr())
         .emit();

      return ioReaderWriter.getResponseWriter().gotError(e);
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  /**
   * Streaming output to run the query and deliver results to the HTTP client.
   * This class continues to run after the POST request returns: it is
   * responsible for cleaning up for any errors that occur during reading
   * output, and for finishing up upon completion.
   * <p>
   * The {@code yielder} is of critical importance. Is is created in the
   * constructor. If the query fails before returning control to Jetty,
   * then the handler code *must* call {#link #close()} to close the
   * yielder. During the {#link write()} call, some setup occurs. If an
   * error occurs, then this class will close the yielder. Once the
   * JSON generator starts returning results, then it will close the
   * yielder, and this class will not do so.
   */
  private class QueryOutput implements StreamingOutput
  {
    final QueryLifecycle queryLifecycle;
    final ResourceIOReaderWriter ioReaderWriter;
    final String currThreadName;
    final boolean serializeDateTimeAsLong;
    final AtomicLong resultRowCount = new AtomicLong();
    Yielder<?> yielder;

    private QueryOutput(
        final QueryLifecycle queryLifecycle,
        final ResourceIOReaderWriter ioReaderWriter,
        final Query<?> query,
        final Sequence<?> results,
        final String currThreadName
    )
    {
      this.queryLifecycle = queryLifecycle;
      this.ioReaderWriter = ioReaderWriter;
      this.currThreadName = currThreadName;
      final boolean shouldFinalize = QueryContexts.isFinalize(query, true);
      this.serializeDateTimeAsLong =
          QueryContexts.isSerializeDateTimeAsLong(query, false)
          || (!shouldFinalize && QueryContexts.isSerializeDateTimeAsLongInner(query, false));
      this.yielder = Yielders.each(
          results.map(
              r -> {
                // The returned values may be batches of rows held within
                // a wrapper. If so, get the actual row count. For all others,
                // treat each item as one row.
                resultRowCount.addAndGet(query.getRowCount(r));
                return r;
              }
          )
      );
    }

    @SuppressWarnings("unchecked")
    @Override
    public void write(OutputStream outputStream) throws IOException, WebApplicationException
    {
      QueryLifecycle.LifecycleStats stats = queryLifecycle.stats();
      // Safe to include outside "try", since it doesn't do anything that can throw an exception.
      CountingOutputStream countingOutputStream = new CountingOutputStream(outputStream);

      Exception e = null;

      try {
        final JsonGenerator jsonGenerator = ioReaderWriter.getResponseWriter().newOutputGenerator(
            countingOutputStream,
            queryLifecycle.getToolChest(),
            queryLifecycle.getQuery(),
            serializeDateTimeAsLong
        );

        if (stats.includeTrailer()) {
          jsonGenerator.writeStartObject();
          jsonGenerator.writeFieldName(JsonParserIterator.FIELD_RESULTS);
        }

        // json serializer for Yielder will always close it
        jsonGenerator.writeObject(yielder);
        yielder = null; // We won't close the yielder

        stats.onResultsSent(resultRowCount.get(), countingOutputStream.getCount());
        if (stats.includeTrailer()) {
          jsonGenerator.writeObjectField(JsonParserIterator.FIELD_CONTEXT, stats.trailer().toMap());
        }

        jsonGenerator.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
        jsonGenerator.close();
      }

      // Handle exceptions that occur while producing results.
      catch (QueryInterruptedException ex) {
        e = ex;
        interruptedQueryCount.incrementAndGet();
      }
      catch (QueryTimeoutException timeout) {
        e = timeout;
        timedOutQueryCount.incrementAndGet();
      }
      catch (QueryCapacityExceededException cap) {
        e = cap;
        failedQueryCount.incrementAndGet();
      }
      catch (QueryUnsupportedException unsupported) {
        e = unsupported;
        failedQueryCount.incrementAndGet();
      }
      catch (BadJsonQueryException | ResourceLimitExceededException ex) {
        e = ex;
        interruptedQueryCount.incrementAndGet();
      }
      catch (Exception ex) {
        e = ex;
        log.error(ex, "Unable to send query response.");
        Throwables.propagateIfInstanceOf(ex, IOException.class);
        throw new RuntimeException(ex);
      }
      finally {
        Thread.currentThread().setName(currThreadName);

        try {
          close();
        }
        catch (Exception ex2) {
          if (e == null) {
            e = ex2;
          } else {
            e.addSuppressed(ex2);
          }
        }
        if (e == null) {
          successfulQueryCount.incrementAndGet();
        } else {
          failedQueryCount.incrementAndGet();
        }
        queryLifecycle.emitLogsAndMetrics(e);
      }
    }

    private void close() throws IOException
    {
      if (yielder == null) {
        return;
      }
      try {
        yielder.close();
      }
      finally {
        yielder = null;
      }
    }
  }

  private Query<?> readQuery(
      final HttpServletRequest req,
      final InputStream in,
      final ResourceIOReaderWriter ioReaderWriter
  ) throws IOException
  {
    try {
      return ioReaderWriter.getRequestMapper().readValue(in, Query.class);
    }
    catch (JsonParseException e) {
      throw new BadJsonQueryException(e);
    }
  }

  private static String getPreviousEtag(final HttpServletRequest req)
  {
    return req.getHeader(HEADER_IF_NONE_MATCH);
  }

  protected ObjectMapper serializeDataTimeAsLong(ObjectMapper mapper)
  {
    return mapper.copy().registerModule(new SimpleModule().addSerializer(DateTime.class, new DateTimeSerializer()));
  }

  protected ResourceIOReaderWriter createResourceIOReaderWriter(
      final HttpServletRequest req,
      final boolean pretty)
  {
    final String requestType = req.getContentType();
    final String acceptHeader = req.getHeader("Accept");

    // response type defaults to Content-Type if 'Accept' header not provided
    String responseType = Strings.isNullOrEmpty(acceptHeader) ? requestType : acceptHeader;

    boolean isRequestSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType) ||
                             APPLICATION_SMILE.equals(requestType);
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

    JsonGenerator newOutputGenerator(
        OutputStream outputStream,
        @Nullable QueryToolChest<?, Query<?>> toolChest,
        @Nullable Query<?> query,
        boolean serializeDateTimeAsLong
    ) throws IOException
    {
      final ObjectMapper mapper = serializeDateTimeAsLong ? serializeDateTimeAsLongInputMapper : inputMapper;
      final ObjectMapper decoratedMapper;

      if (toolChest != null) {
        decoratedMapper = toolChest.decorateObjectMapper(mapper, Preconditions.checkNotNull(query, "query"));
      } else {
        decoratedMapper = mapper;
      }

      final JsonGenerator jsonGenerator = decoratedMapper.getFactory().createGenerator(outputStream);

      return isPretty ? jsonGenerator.useDefaultPrettyPrinter() : jsonGenerator;
    }

    Response ok(Object object) throws IOException
    {
      return Response.ok(writeAsBytes(object), responseType).build();
    }

    Response gotError(Exception e) throws IOException
    {
      return buildNonOkResponse(
          Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(e)
      );
    }

    Response gotTimeout(QueryTimeoutException e) throws IOException
    {
      return buildNonOkResponse(QueryTimeoutException.STATUS_CODE, e);
    }

    Response gotLimited(QueryCapacityExceededException e) throws IOException
    {
      return buildNonOkResponse(QueryCapacityExceededException.STATUS_CODE, e);
    }

    Response gotUnsupported(QueryUnsupportedException e) throws IOException
    {
      return buildNonOkResponse(QueryUnsupportedException.STATUS_CODE, e);
    }

    Response gotBadQuery(BadQueryException e) throws IOException
    {
      return buildNonOkResponse(BadQueryException.STATUS_CODE, e);
    }

    Response buildNonOkResponse(int status, Exception e) throws IOException
    {
      return Response.status(status)
                     .type(responseType)
                     .entity(writeAsBytes(errorResponse(e)))
                     .build();
    }

    private Object errorResponse(final Exception e)
    {
      return e instanceof QueryException ? e : QueryInterruptedException.wrapIfNeeded(e);
    }

    private byte[] writeAsBytes(final Object o) throws IOException
    {
      final ByteArrayOutputStream baos = new ByteArrayOutputStream();

      try (final JsonGenerator generator = newOutputGenerator(baos, null, null, false)) {
        generator.writeObject(o);
      }

      return baos.toByteArray();
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
}
