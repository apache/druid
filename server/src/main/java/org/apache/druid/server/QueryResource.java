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
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import org.apache.druid.client.DirectDruidClient;
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
  public static final String HEADER_ETAG = "ETag";

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

      // used to get request content-type,Accept header, remote address and auth-related headers
      @Context final HttpServletRequest req
  ) throws IOException
  {
    final QueryLifecycle queryLifecycle = queryLifecycleFactory.factorize();
    Query<?> query = null;

    String acceptHeader = req.getHeader("Accept");
    if (Strings.isNullOrEmpty(acceptHeader)) {
      //default to content-type
      acceptHeader = req.getContentType();
    }

    final ResourceIOReaderWriter ioReaderWriter = createResourceIOReaderWriter(acceptHeader, pretty != null);

    final String currThreadName = Thread.currentThread().getName();
    try {
      queryLifecycle.initialize(readQuery(req, in, ioReaderWriter));
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

      final QueryLifecycle.QueryResponse queryResponse = queryLifecycle.execute();
      final Sequence<?> results = queryResponse.getResults();
      final ResponseContext responseContext = queryResponse.getResponseContext();
      final String prevEtag = getPreviousEtag(req);

      if (prevEtag != null && prevEtag.equals(responseContext.get(ResponseContext.Key.ETAG))) {
        queryLifecycle.emitLogsAndMetrics(null, req.getRemoteAddr(), -1);
        successfulQueryCount.incrementAndGet();
        return Response.notModified().build();
      }

      final Yielder<?> yielder = Yielders.each(results);

      try {
        boolean shouldFinalize = QueryContexts.isFinalize(query, true);
        boolean serializeDateTimeAsLong =
            QueryContexts.isSerializeDateTimeAsLong(query, false)
            || (!shouldFinalize && QueryContexts.isSerializeDateTimeAsLongInner(query, false));

        final ObjectWriter jsonWriter = ioReaderWriter.newOutputWriter(
            queryLifecycle.getToolChest(),
            queryLifecycle.getQuery(),
            serializeDateTimeAsLong
        );

        Response.ResponseBuilder responseBuilder = Response
            .ok(
                new StreamingOutput()
                {
                  @Override
                  public void write(OutputStream outputStream) throws WebApplicationException
                  {
                    Exception e = null;

                    CountingOutputStream os = new CountingOutputStream(outputStream);
                    try {
                      // json serializer will always close the yielder
                      jsonWriter.writeValue(os, yielder);

                      os.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
                      os.close();
                    }
                    catch (Exception ex) {
                      e = ex;
                      log.noStackTrace().error(ex, "Unable to send query response.");
                      throw new RuntimeException(ex);
                    }
                    finally {
                      Thread.currentThread().setName(currThreadName);

                      queryLifecycle.emitLogsAndMetrics(e, req.getRemoteAddr(), os.getCount());

                      if (e == null) {
                        successfulQueryCount.incrementAndGet();
                      } else {
                        failedQueryCount.incrementAndGet();
                      }
                    }
                  }
                },
                ioReaderWriter.getContentType()
            )
            .header("X-Druid-Query-Id", queryId);

        Object entityTag = responseContext.remove(ResponseContext.Key.ETAG);
        if (entityTag != null) {
          responseBuilder.header(HEADER_ETAG, entityTag);
        }

        DirectDruidClient.removeMagicResponseContextFields(responseContext);

        //Limit the response-context header, see https://github.com/apache/druid/issues/2331
        //Note that Response.ResponseBuilder.header(String key,Object value).build() calls value.toString()
        //and encodes the string using ASCII, so 1 char is = 1 byte
        final ResponseContext.SerializationResult serializationResult = responseContext.serializeWith(
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
        yielder.close();
        throw e;
      }
      catch (Exception e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder.close();
        throw new RuntimeException(e);
      }
      finally {
        // do not close yielder here, since we do not want to close the yielder prior to
        // StreamingOutput having iterated over all the results
      }
    }
    catch (QueryInterruptedException e) {
      interruptedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(e, req.getRemoteAddr(), -1);
      return ioReaderWriter.gotError(e);
    }
    catch (QueryTimeoutException timeout) {
      timedOutQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(timeout, req.getRemoteAddr(), -1);
      return ioReaderWriter.gotTimeout(timeout);
    }
    catch (QueryCapacityExceededException cap) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(cap, req.getRemoteAddr(), -1);
      return ioReaderWriter.gotLimited(cap);
    }
    catch (QueryUnsupportedException unsupported) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(unsupported, req.getRemoteAddr(), -1);
      return ioReaderWriter.gotUnsupported(unsupported);
    }
    catch (BadJsonQueryException | ResourceLimitExceededException e) {
      interruptedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(e, req.getRemoteAddr(), -1);
      return ioReaderWriter.gotBadQuery(e);
    }
    catch (ForbiddenException e) {
      // don't do anything for an authorization failure, ForbiddenExceptionMapper will catch this later and
      // send an error response if this is thrown.
      throw e;
    }
    catch (Exception e) {
      failedQueryCount.incrementAndGet();
      queryLifecycle.emitLogsAndMetrics(e, req.getRemoteAddr(), -1);

      log.noStackTrace()
         .makeAlert(e, "Exception handling request")
         .addData("query", query != null ? jsonMapper.writeValueAsString(query) : "unparseable query")
         .addData("peer", req.getRemoteAddr())
         .emit();

      return ioReaderWriter.gotError(e);
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  private Query<?> readQuery(
      final HttpServletRequest req,
      final InputStream in,
      final ResourceIOReaderWriter ioReaderWriter
  ) throws IOException
  {
    Query baseQuery;
    try {
      baseQuery = ioReaderWriter.getInputMapper().readValue(in, Query.class);
    }
    catch (JsonParseException e) {
      throw new BadJsonQueryException(e);
    }
    String prevEtag = getPreviousEtag(req);

    if (prevEtag != null) {
      baseQuery = baseQuery.withOverriddenContext(
          ImmutableMap.of(HEADER_IF_NONE_MATCH, prevEtag)
      );
    }

    return baseQuery;
  }

  private static String getPreviousEtag(final HttpServletRequest req)
  {
    return req.getHeader(HEADER_IF_NONE_MATCH);
  }

  protected ObjectMapper serializeDataTimeAsLong(ObjectMapper mapper)
  {
    return mapper.copy().registerModule(new SimpleModule().addSerializer(DateTime.class, new DateTimeSerializer()));
  }

  protected ResourceIOReaderWriter createResourceIOReaderWriter(String requestType, boolean pretty)
  {
    boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType) ||
                      APPLICATION_SMILE.equals(requestType);
    String contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;
    return new ResourceIOReaderWriter(
        contentType,
        isSmile ? smileMapper : jsonMapper,
        isSmile ? serializeDateTimeAsLongSmileMapper : serializeDateTimeAsLongJsonMapper,
        pretty
    );
  }

  protected static class ResourceIOReaderWriter
  {
    private final String contentType;
    private final ObjectMapper inputMapper;
    private final ObjectMapper serializeDateTimeAsLongInputMapper;
    private final boolean isPretty;

    ResourceIOReaderWriter(
        String contentType,
        ObjectMapper inputMapper,
        ObjectMapper serializeDateTimeAsLongInputMapper,
        boolean isPretty
    )
    {
      this.contentType = contentType;
      this.inputMapper = inputMapper;
      this.serializeDateTimeAsLongInputMapper = serializeDateTimeAsLongInputMapper;
      this.isPretty = isPretty;
    }

    String getContentType()
    {
      return contentType;
    }

    ObjectMapper getInputMapper()
    {
      return inputMapper;
    }

    ObjectWriter newOutputWriter(
        @Nullable QueryToolChest toolChest,
        @Nullable Query query,
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
      return Response.ok(newOutputWriter(null, null, false).writeValueAsString(object), contentType).build();
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

    Response buildNonOkResponse(int status, Exception e) throws JsonProcessingException
    {
      return Response.status(status)
                     .type(contentType)
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
}
