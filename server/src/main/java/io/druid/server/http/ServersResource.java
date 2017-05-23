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

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.server.http.security.StateResourceFilter;
import io.druid.timeline.DataSegment;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 */
@Path("/druid/coordinator/v1/servers")
@ResourceFilters(StateResourceFilter.class)
public class ServersResource
{
  private static Map<String, Object> makeSimpleServer(DruidServer input)
  {
    return new ImmutableMap.Builder<String, Object>()
        .put("host", input.getHost())
        .put("tier", input.getTier())
        .put("type", input.getType().toString())
        .put("priority", input.getPriority())
        .put("currSize", input.getCurrSize())
        .put("maxSize", input.getMaxSize())
        .build();
  }

  private static Map<String, Object> makeFullServer(DruidServer input)
  {
    return new ImmutableMap.Builder<String, Object>()
        .put("host", input.getHost())
        .put("maxSize", input.getMaxSize())
        .put("type", input.getType().toString())
        .put("tier", input.getTier())
        .put("priority", input.getPriority())
        .put("segments", input.getSegments())
        .put("currSize", input.getCurrSize())
        .build();
  }

  private final InventoryView serverInventoryView;

  @Inject
  public ServersResource(
      InventoryView serverInventoryView
  )
  {
    this.serverInventoryView = serverInventoryView;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getClusterServers(
      @QueryParam("full") String full,
      @QueryParam("simple") String simple
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);

    if (full != null) {
      return builder.entity(
          Lists.newArrayList(
              Iterables.transform(
                  serverInventoryView.getInventory(),
                  new Function<DruidServer, Map<String, Object>>()
                  {
                    @Override
                    public Map<String, Object> apply(DruidServer input)
                    {
                      return makeFullServer(input);
                    }
                  }
              )
          )
      ).build();
    } else if (simple != null) {
      return builder.entity(
          Lists.newArrayList(
              Iterables.transform(
                  serverInventoryView.getInventory(),
                  new Function<DruidServer, Map<String, Object>>()
                  {
                    @Override
                    public Map<String, Object> apply(DruidServer input)
                    {
                      return makeSimpleServer(input);
                    }
                  }
              )
          )
      ).build();
    }

    return builder.entity(
        Lists.newArrayList(
            Iterables.transform(
                serverInventoryView.getInventory(),
                new Function<DruidServer, String>()
                {
                  @Override
                  public String apply(DruidServer druidServer)
                  {
                    return druidServer.getHost();
                  }
                }
            )
        )
    ).build();
  }

  @GET
  @Path("/{serverName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServer(
      @PathParam("serverName") String serverName,
      @QueryParam("simple") String simple
  )
  {
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);

    if (simple != null) {
      return builder.entity(makeSimpleServer(server)).build();
    }

    return builder.entity(makeFullServer(server))
                  .build();
  }

  @GET
  @Path("/{serverName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServerSegments(
      @PathParam("serverName") String serverName,
      @QueryParam("full") String full
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    if (full != null) {
      return builder.entity(server.getSegments().values()).build();
    }

    return builder.entity(
        Collections2.transform(
            server.getSegments().values(),
            new Function<DataSegment, String>()
            {
              @Override
              public String apply(DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/{serverName}/segments/{segmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServerSegment(
      @PathParam("serverName") String serverName,
      @PathParam("segmentId") String segmentId
  )
  {
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    DataSegment segment = server.getSegment(segmentId);
    if (segment == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    return Response.status(Response.Status.OK).entity(segment).build();
  }
}
