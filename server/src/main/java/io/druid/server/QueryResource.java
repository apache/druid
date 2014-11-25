/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import io.druid.query.QueryMetricUtil;
import io.druid.query.QuerySegmentWalker;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.RequestLogger;
import org.joda.time.DateTime;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.UUID;

/**
 */
@Path("/druid/v2/")
public class QueryResource
{
  private static final EmittingLogger log = new EmittingLogger(QueryResource.class);
  public static final String APPLICATION_SMILE = "application/smile";
  public static final String APPLICATION_JSON = "application/json";

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
    this.jsonMapper = jsonMapper.copy();
    this.jsonMapper.getFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    this.smileMapper = smileMapper.copy();
    this.smileMapper.getFactory().configure(JsonGenerator.Feature.AUTO_CLOSE_TARGET, false);

    this.texasRanger = texasRanger;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.queryManager = queryManager;
  }

  @DELETE
  @Path("{id}")
  @Produces("application/json")
  public Response getServer(@PathParam("id") String queryId)
  {
    queryManager.cancelQuery(queryId);
    return Response.status(Response.Status.ACCEPTED).build();

  }

  @POST
  public Response doPost(
      @Context HttpServletRequest req,
      @Context final HttpServletResponse resp
  ) throws ServletException, IOException
  {
    final long start = System.currentTimeMillis();
    Query query = null;
    byte[] requestQuery = null;
    String queryId = null;

    final boolean isSmile = APPLICATION_SMILE.equals(req.getContentType());
    final String contentType = isSmile ? APPLICATION_SMILE : APPLICATION_JSON;

    ObjectMapper objectMapper = isSmile ? smileMapper : jsonMapper;
    final ObjectWriter jsonWriter = req.getParameter("pretty") == null
                                    ? objectMapper.writer()
                                    : objectMapper.writerWithDefaultPrettyPrinter();

    try {
      requestQuery = ByteStreams.toByteArray(req.getInputStream());
      query = objectMapper.readValue(requestQuery, Query.class);
      queryId = query.getId();
      if (queryId == null) {
        queryId = UUID.randomUUID().toString();
        query = query.withId(queryId);
      }
      if (query.getContextValue("timeout") == null) {
        query = query.withOverriddenContext(
            ImmutableMap.of(
                "timeout",
                config.getMaxIdleTime().toStandardDuration().getMillis()
            )
        );
      }

      if (log.isDebugEnabled()) {
        log.debug("Got query [%s]", query);
      }

      Sequence res = query.run(texasRanger);
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
        long requestTime = System.currentTimeMillis() - start;
        emitter.emit(
            QueryMetricUtil.makeRequestTimeMetric(jsonMapper, query, req.getRemoteAddr())
                           .build("request/time", requestTime)
        );

        requestLogger.log(
            new RequestLogLine(
                new DateTime(),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.<String, Object>of(
                        "request/time", requestTime,
                        "success", true
                    )
                )
            )
        );

        return Response
            .ok(
                new StreamingOutput()
                {
                  @Override
                  public void write(OutputStream outputStream) throws IOException, WebApplicationException
                  {
                    // json serializer will always close the yielder
                    jsonWriter.writeValue(outputStream, yielder);
                    outputStream.close();
                  }
                },
                contentType
        )
            .header("X-Druid-Query-Id", queryId)
            .build();
      }
      catch (Exception e) {
        // make sure to close yieder if anything happened before starting to serialize the response.
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
        requestLogger.log(
            new RequestLogLine(
                new DateTime(),
                req.getRemoteAddr(),
                query,
                new QueryStats(
                    ImmutableMap.<String, Object>of(
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
      final String queryString =
          query == null
          ? (isSmile ? "smile_unknown" : new String(requestQuery, Charsets.UTF_8))
          : query.toString();

      log.warn(e, "Exception occurred on request [%s]", queryString);

      try {
        requestLogger.log(
            new RequestLogLine(
                new DateTime(),
                req.getRemoteAddr(),
                query,
                new QueryStats(ImmutableMap.<String, Object>of("success", false, "exception", e.toString()))
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
