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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CountingOutputStream;
import com.google.inject.Inject;
import com.metamx.common.guava.Yielder;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.query.DruidMetrics;
import io.druid.query.Query;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.log.RequestLogger;
import io.druid.server.security.AuthConfig;
import org.joda.time.DateTime;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Path;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 */
@Path("/druid/v3/")
public class QueryResourceV3 extends QueryResource
{
  private final ServiceEmitter emitter;
  private final ObjectMapper jsonMapper;
  private final RequestLogger requestLogger;

  @Inject
  public QueryResourceV3(
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
    super(warehouse, config, jsonMapper, smileMapper, texasRanger, emitter, requestLogger, queryManager, authConfig);
    this.emitter = emitter;
    this.jsonMapper = jsonMapper;
    this.requestLogger = requestLogger;
  }

  @Override
  protected Response buildQueryResponse(
      final HttpServletRequest req,
      final Query query,
      final QueryToolChest toolChest,
      final ObjectWriter jsonWriter,
      final String contentType,
      final long startTime,
      final Yielder yielder,
      final Map<String, Object> responseContext
  )
      throws IOException
  {
    try {
      return Response
          .ok(
              new StreamingOutput()
              {
                @Override
                public void write(OutputStream outputStream) throws IOException, WebApplicationException
                {
                  CountingOutputStream os = new CountingOutputStream(outputStream);

                  //jsonSerializer would close the yielder.
                  try {
                    serializeResponseToOS(
                        jsonWriter,
                        os,
                        ImmutableMap.of(
                            "result", yielder,
                            "context", responseContext
                        )
                    );
                    os.flush(); // Some types of OutputStream suppress flush errors in the .close() method.
                  } finally {
                    os.close();
                  }

                  final long queryTime = System.currentTimeMillis() - startTime;
                  emitter.emit(
                      DruidMetrics.makeQueryTimeMetric(toolChest, jsonMapper, query, req.getRemoteAddr())
                                  .setDimension("success", "true")
                                  .build("query/time", queryTime)
                  );
                  emitter.emit(
                      DruidMetrics.makeQueryTimeMetric(toolChest, jsonMapper, query, req.getRemoteAddr())
                                  .build("query/bytes", os.getCount())
                  );

                  requestLogger.log(
                      new RequestLogLine(
                          new DateTime(startTime),
                          req.getRemoteAddr(),
                          query,
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
          .header("X-Druid-Query-Id", query.getId())
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

  @VisibleForTesting
  static void serializeResponseToOS(ObjectWriter jsonWriter, OutputStream os, Map<String, Object> responseObj)
      throws IOException
  {
    jsonWriter.writeValue(
        os,
        responseObj
    );
  }
}
