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

package io.druid.server.http;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.guice.annotations.Global;
import io.druid.segment.loading.StorageLocation;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.coordinator.LoadQueuePeon;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.timeline.DataSegment;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 */
@Path("/druid/coordinator/v1")
@ResourceFilters(StateResourceFilter.class)
public class CoordinatorResource
{
  private static final StatusResponseHandler RESPONSE_HANDLER = new StatusResponseHandler(Charsets.UTF_8);

  private final DruidCoordinator coordinator;
  private final ObjectMapper jsonMapper;
  private final InventoryView serverInventoryView;
  private final HttpClient client;

  @Inject
  public CoordinatorResource(
      @Global HttpClient client,
      ObjectMapper jsonMapper,
      DruidCoordinator coordinator,
      InventoryView serverInventoryView
  )
  {
    this.client = client;
    this.jsonMapper = jsonMapper;
    this.coordinator = coordinator;
    this.serverInventoryView = serverInventoryView;
  }

  @GET
  @Path("/leader")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLeader()
  {
    return Response.ok(coordinator.getCurrentLeader()).build();
  }

  @GET
  @Path("/loadstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadStatus(
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    if (simple != null) {
      return Response.ok(coordinator.getSegmentAvailability()).build();
    }

    if (full != null) {
      return Response.ok(coordinator.getReplicationStatus()).build();
    }
    return Response.ok(coordinator.getLoadStatus()).build();
  }

  @GET
  @Path("/loadqueue")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getLoadQueue(
      @QueryParam("simple") String simple,
      @QueryParam("full") String full
  )
  {
    if (simple != null) {
      return Response.ok(
          Maps.transformValues(
              coordinator.getLoadManagementPeons(),
              new Function<LoadQueuePeon, Object>()
              {
                @Override
                public Object apply(LoadQueuePeon input)
                {
                  long loadSize = 0;
                  for (DataSegment dataSegment : input.getSegmentsToLoad()) {
                    loadSize += dataSegment.getSize();
                  }

                  long dropSize = 0;
                  for (DataSegment dataSegment : input.getSegmentsToDrop()) {
                    dropSize += dataSegment.getSize();
                  }

                  return new ImmutableMap.Builder<>()
                      .put("segmentsToLoad", input.getSegmentsToLoad().size())
                      .put("segmentsToDrop", input.getSegmentsToDrop().size())
                      .put("segmentsToLoadSize", loadSize)
                      .put("segmentsToDropSize", dropSize)
                      .build();
                }
              }
          )
      ).build();
    }

    if (full != null) {
      return Response.ok(coordinator.getLoadManagementPeons()).build();
    }

    return Response.ok(
        Maps.transformValues(
            coordinator.getLoadManagementPeons(),
            new Function<LoadQueuePeon, Object>()
            {
              @Override
              public Object apply(LoadQueuePeon input)
              {
                return new ImmutableMap.Builder<>()
                    .put(
                        "segmentsToLoad",
                        Collections2.transform(
                            input.getSegmentsToLoad(),
                            new Function<DataSegment, Object>()
                            {
                              @Override
                              public String apply(DataSegment segment)
                              {
                                return segment.getIdentifier();
                              }
                            }
                        )
                    )
                    .put(
                        "segmentsToDrop", Collections2.transform(
                        input.getSegmentsToDrop(),
                        new Function<DataSegment, Object>()
                        {
                          @Override
                          public String apply(DataSegment segment)
                          {
                            return segment.getIdentifier();
                          }
                        }
                    )
                    )
                    .build();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/historicalstatus")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getHistoricalStorageStatus(
      @QueryParam("simple") String simple
  )
  {

    if (simple != null) {
      List<String> historicalNodes = Lists.newArrayList(
          Iterables.transform(
              serverInventoryView.getInventory(),
              new Function<DruidServer, String>()
              {
                @Override
                public String apply(DruidServer server)
                {
                  return server.getHost();
                }
              }
          )
      );

      return Response.ok(historicalNodes).build();
    }

    Map<String, Object> statusMap = Maps.newHashMap();
    for (DruidServer server: serverInventoryView.getInventory()) {
      try {
        StatusResponseHolder response = client.go(
            new Request(
                HttpMethod.GET,
                historicalUrl(server)
            ),
            RESPONSE_HANDLER
        ).get();
        if (!response.getStatus().equals(HttpResponseStatus.OK)) {
          statusMap.put(server.getHost(), ImmutableMap.of());
        }
        statusMap.put(
            server.getHost(),
            jsonMapper.readValue(
                response.getContent(),
                new TypeReference<List<Map<String, String>>>() {}
            )
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }

    return Response.ok(statusMap).build();
  }

  private URL historicalUrl(DruidServer server)
  {
    try {
      return new URL(String.format("http://%s/druid/historical/v1/localStorageStatus",server.getHost()));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
