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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.MapMaker;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QueryContextKeys;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QuerySegmentWalker;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.RequestLogger;
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
import java.util.UUID;

/**
 */
@Path("/druid/v2/")
public class QueryResource
{
  private static final EmittingLogger log = new EmittingLogger(QueryResource.class);
  @Deprecated // use SmileMediaTypes.APPLICATION_JACKSON_SMILE
  private static final String APPLICATION_SMILE = "application/smile";

  private static final int RESPONSE_CTX_HEADER_LEN_LIMIT = 7*1024;

  private final ServerConfig config;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final QuerySegmentWalker texasRanger;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final QueryManager queryManager;

  @Inject
  public QueryResource(
      ServerConfig config,
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper,
      QuerySegmentWalker texasRanger,
      ServiceEmitter emitter,
      RequestLogger requestLogger,
      QueryManager queryManager
  )
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.texasRanger = texasRanger;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryManager = queryManager;
  }

  @DELETE
  @Path("{id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServer(@PathParam("id") String queryId)
  {
    queryManager.cancelQuery(queryId);
    return Response.status(Response.Status.ACCEPTED).build();

  }

  @POST
  @Produces({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE})
  @Consumes({MediaType.APPLICATION_JSON, SmileMediaTypes.APPLICATION_JACKSON_SMILE, APPLICATION_SMILE})
  public Response doPost(
      InputStream in,
      @QueryParam("pretty") String pretty,
      @Context final HttpServletRequest req // used only to get request content-type and remote address
  ) throws IOException
  {
    final long start = System.currentTimeMillis();
    Query query = null;
    String queryId = null;

    final String reqContentType = req.getContentType();
    final boolean isSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(reqContentType)
                            || APPLICATION_SMILE.equals(reqContentType);
    final String contentType = isSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON;

    ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    final ObjectWriter jsonWriter = pretty != null
                                    ? objectMapper.writerWithDefaultPrettyPrinter()
                                    : objectMapper.writer();

    try {
      query = objectMapper.readValue(in, Query.class);
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

      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", query);
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
                        DruidMetrics.makeQueryTimeMetric(jsonMapper, theQuery, req.getRemoteAddr())
                                    .setDimension("success", "true")
                                    .build("query/time", queryTime)
                    );
                    emitter.emit(
                        DruidMetrics.makeQueryTimeMetric(jsonMapper, theQuery, req.getRemoteAddr())
                                    .build("query/bytes", os.getCount())
                    );

                    requestLogger.log(
                        new RequestLogLine(
                            new DateTime(),
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
                contentType
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
        log.info("%s [%s]", e.getMessage(), queryId);
        final long queryTime = System.currentTimeMillis() - start;
        emitter.emit(
            DruidMetrics.makeQueryTimeMetric(jsonMapper, query, req.getRemoteAddr())
                        .setDimension("success", "false")
                        .build("query/time", queryTime)
        );
        requestLogger.log(
            new RequestLogLine(
                new DateTime(),
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
      return Response.serverError().type(contentType).entity(
          jsonWriter.writeValueAsBytes(
              ImmutableMap.of(
                  "error", e.getMessage() == null ? "null exception" : e.getMessage()
              )
          )
      ).build();
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
            DruidMetrics.makeQueryTimeMetric(jsonMapper, query, req.getRemoteAddr())
                        .setDimension("success", "false")
                        .build("query/time", queryTime)
        );
        requestLogger.log(
            new RequestLogLine(
                new DateTime(),
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

      return Response.serverError().type(contentType).entity(
          jsonWriter.writeValueAsBytes(
              ImmutableMap.of(
                  "error", e.getMessage() == null ? "null exception" : e.getMessage()
              )
          )
      ).build();
    }
  }
}
