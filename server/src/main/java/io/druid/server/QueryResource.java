/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.guava.Yielder;
import io.druid.java.util.common.guava.YieldingAccumulator;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.Access;
import io.druid.server.security.Action;
import io.druid.server.security.AuthConfig;
import io.druid.server.security.AuthorizationInfo;
import io.druid.server.security.Resource;
import io.druid.server.security.ResourceType;
import org.joda.time.DateTime;

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
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 */
@Path("/druid/v2/")
public class QueryResource
{
  protected static final EmittingLogger log = new EmittingLogger(QueryResource.class);
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  protected static final String APPLICATION_SMILE = "application/smile";

  protected static final int RESPONSE_CTX_HEADER_LEN_LIMIT = 7 * 1024;

  protected final QueryToolChestWarehouse warehouse;
  protected final ServerConfig config;
  protected final ObjectMapper jsonMapper;
  protected final ObjectMapper smileMapper;
  protected final QuerySegmentWalker texasRanger;
  protected final ServiceEmitter emitter;
  protected final RequestLogger requestLogger;
  protected final QueryManager queryManager;
  protected final AuthConfig authConfig;

  @Inject
  public QueryResource(
      QueryToolChestWarehouse warehouse,
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryManager queryManager,
      AuthConfig authConfig
  )
  {
    this.warehouse = warehouse;
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.texasRanger = texasRanger;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryManager = queryManager;
    this.authConfig = authConfig;
  }

  @DELETE
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServer(@PathParam("id") String queryId, @Context final HttpServletRequest req)
  {
    if (log.isDebugEnabled()) {
      log.debug("Received cancel request for query [%s]", queryId);
    }
    if (authConfig.isEnabled()) {
      // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
      final AuthorizationInfo authorizationInfo = (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
      Preconditions.checkNotNull(
          authorizationInfo,
          "Security is enabled but no authorization info found in the request"
      );
      Set<String> datasources = queryManager.getQueryDatasources(queryId);
      if (datasources == null) {
        log.warn("QueryId [%s] not registered with QueryManager, cannot cancel", queryId);
      } else {
        for (String dataSource : datasources) {
          Access authResult = authorizationInfo.isAuthorized(
              new Resource(dataSource, ResourceType.DATASOURCE),
              Action.WRITE
          );
          if (!authResult.isAllowed()) {
            return Response.status(Response.Status.FORBIDDEN).header("Access-Check-Result", authResult).build();
          }
        }
      }
    }
    queryManager.cancelQuery(queryId);
    return Response.status(Response.Status.ACCEPTED).build();
  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response doPost(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req // used to get request content-type, remote address and AuthorizationInfo
  ) throws IOException
  {
    final long start = System.currentTimeMillis();
    Query query = null;
    QueryToolChest toolChest = null;
    String queryId = null;

    final ResponseContext context = createContext(req.getContentType(), pretty != null);

    final String currThreadName = Thread.currentThread().getName();
    try {
      query = context.getObjectMapper().readValue(in, Query.class);
      queryId = query.getId();
      if (queryId == null) {
        queryId = UUID.randomUUID().toString();
        query = query.withId(queryId);
      }
      if (query.getContextValue(QueryContextKeys.TIMEOUT) == null) {
        query = query.withOverriddenContext(
            ImmutableMap.of(
                QueryContextKeys.TIMEOUT,
                config.getMaxIdleTime().toStandardDuration().getMillis()
            )
        );
      }
      toolChest = warehouse.getToolChest(query);

      Thread.currentThread()
            .setName(String.format("%s[%s_%s_%s]", currThreadName, query.getType(), query.getDataSource().getNames(), queryId));
      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", query);
      }

      if (authConfig.isEnabled()) {
        // This is an experimental feature, see - https://github.com/druid-io/druid/pull/2424
        AuthorizationInfo authorizationInfo = (AuthorizationInfo) req.getAttribute(AuthConfig.DRUID_AUTH_TOKEN);
        if (authorizationInfo != null) {
          for (String dataSource : query.getDataSource().getNames()) {
            Access authResult = authorizationInfo.isAuthorized(
                new Resource(dataSource, ResourceType.DATASOURCE),
                Action.READ
            );
            if (!authResult.isAllowed()) {
              return Response.status(Response.Status.FORBIDDEN).header("Access-Check-Result", authResult).build();
            }
          }
        } else {
          throw new ISE("WTF?! Security is enabled but no authorization info found in the request");
        }
      }

      final Map<String, Object> responseContext = new MapMaker().makeMap();
      final Sequence res = query.run(texasRanger, responseContext);
      final Sequence results;
      if (res == null) {
        results = Sequences.empty();
      } else {
        results = res;
      }

      final Yielder yielder = results.toYielder(
          null,
          new YieldingAccumulator()
          {
            @Override
            public Object accumulate(Object accumulated, Object in)
            {
              yield();
              return in;
            }
          }
      );

      try {
        final Query theQuery = query;
        final QueryToolChest theToolChest = toolChest;
        final ObjectWriter jsonWriter = context.newOutputWriter();
        Response.ResponseBuilder builder = Response
            .ok(
                new StreamingOutput()
                {
                  @Override
                  public void write(OutputStream outputStream) throws IOException, WebApplicationException
                  {
                    // json serializer will always close the yielder
                    CountingOutputStream os = new CountingOutputStream(outputStream);
                    jsonWriter.writeValue(os, yielder);

                    os.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
                    os.close();

                    final long queryTime = System.currentTimeMillis() - start;
                    emitter.emit(
                        DruidMetrics.makeQueryTimeMetric(theToolChest, jsonMapper, theQuery, req.getRemoteAddr())
                                    .setDimension("success", "true")
                                    .build("query/time", queryTime)
                    );
                    emitter.emit(
                        DruidMetrics.makeQueryTimeMetric(theToolChest, jsonMapper, theQuery, req.getRemoteAddr())
                                    .build("query/bytes", os.getCount())
                    );

                    requestLogger.log(
                        new RequestLogLine(
                            new DateTime(start),
                            req.getRemoteAddr(),
                            theQuery,
                            new QueryStats(
                                ImmutableMap.<String, Object>of(
                                    "query/time", queryTime,
                                    "query/bytes", os.getCount(),
                                    "success", true
                                )
                            )
                        )
                    );
                  }
                },
                context.getContentType()
            )
            .header("X-Druid-Query-Id", queryId);

        //Limit the response-context header, see https://github.com/druid-io/druid/issues/2331
        //Note that Response.ResponseBuilder.header(String key,Object value).build() calls value.toString()
        //and encodes the string using ASCII, so 1 char is = 1 byte
        String responseCtxString = jsonMapper.writeValueAsString(responseContext);
        if (responseCtxString.length() > RESPONSE_CTX_HEADER_LEN_LIMIT) {
          log.warn("Response Context truncated for id [%s] . Full context is [%s].", queryId, responseCtxString);
          responseCtxString = responseCtxString.substring(0, RESPONSE_CTX_HEADER_LEN_LIMIT);
        }

        return builder
            .header("X-Druid-Response-Context", responseCtxString)
            .build();
      }
      catch (Exception e) {
        // make sure to close yielder if anything happened before starting to serialize the response.
        yielder.close();
        throw Throwables.propagate(e);
      }
      finally {
        // do not close yielder here, since we do not want to close the yielder prior to
        // StreamingOutput having iterated over all the results
      }
    }
    catch (QueryInterruptedException e) {
      try {
        log.warn(e, "Exception while processing queryId [%s]", queryId);
        final long queryTime = System.currentTimeMillis() - start;
        emitter.emit(
            DruidMetrics.makeQueryTimeMetric(toolChest, jsonMapper, query, req.getRemoteAddr())
                        .setDimension("success", "false")
                        .build("query/time", queryTime)
        );
        requestLogger.log(
            new RequestLogLine(
                new DateTime(start),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "query/time",
                        queryTime,
                        "success",
                        false,
                        "interrupted",
                        true,
                        "reason",
                        e.toString()
                    )
                )
            )
        );
      }
      catch (Exception e2) {
        log.error(e2, "Unable to log query [%s]!", query);
      }
      return context.gotError(e);
    }
    catch (Exception e) {
      // Input stream has already been consumed by the json object mapper if query == null
      final String queryString =
          query == null
          ? "unparsable query"
          : query.toString();

      log.warn(e, "Exception occurred on request [%s]", queryString);

      try {
        final long queryTime = System.currentTimeMillis() - start;
        emitter.emit(
            DruidMetrics.makeQueryTimeMetric(toolChest, jsonMapper, query, req.getRemoteAddr())
                        .setDimension("success", "false")
                        .build("query/time", queryTime)
        );
        requestLogger.log(
            new RequestLogLine(
                new DateTime(start),
                req.getRemoteAddr(),
                query,
                new QueryStats(ImmutableMap.<String, Object>of(
                    "query/time",
                    queryTime,
                    "success",
                    false,
                    "exception",
                    e.toString()
                ))
            )
        );
      }
      catch (Exception e2) {
        log.error(e2, "Unable to log query [%s]!", queryString);
      }

      log.makeAlert(e, "Exception handling request")
         .addData("exception", e.toString())
         .addData("query", queryString)
         .addData("peer", req.getRemoteAddr())
         .emit();

      return context.gotError(e);
    }
    finally {
      Thread.currentThread().setName(currThreadName);
    }
  }

  protected ResponseContext createContext(String requestType, boolean pretty)
  {
    boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType) ||
                      APPLICATION_SMILE.equals(requestType);
    String contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;
    return new ResponseContext(contentType, isSmile ? smileMapper : jsonMapper, pretty);
  }

  protected static class ResponseContext
  {
    private final String contentType;
    private final ObjectMapper inputMapper;
    private final boolean isPretty;

    ResponseContext(String contentType, ObjectMapper inputMapper, boolean isPretty)
    {
      this.contentType = contentType;
      this.inputMapper = inputMapper;
      this.isPretty = isPretty;
    }

    String getContentType()
    {
      return contentType;
    }

    public ObjectMapper getObjectMapper()
    {
      return inputMapper;
    }

    ObjectWriter newOutputWriter()
    {
      return isPretty ? inputMapper.writerWithDefaultPrettyPrinter() : inputMapper.writer();
    }

    Response ok(Object object) throws IOException
    {
      return Response.ok(newOutputWriter().writeValueAsString(object), contentType).build();
    }

    Response gotError(Exception e) throws IOException
    {
      return Response.serverError()
                     .type(contentType)
                     .entity(newOutputWriter().writeValueAsBytes(QueryInterruptedException.wrapIfNeeded(e)))
                     .build();
    }
  }
}
