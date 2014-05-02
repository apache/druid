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

package io.druid.server.http;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.client.DruidServer;
import io.druid.client.InventoryView;
import io.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Response;
import java.util.Map;

/**
 */
@Path("/druid/coordinator/v1/servers")
public class ServersResource
{
  private static Map<String, Object> makeSimpleServer(DruidServer input)
  {
    return new ImmutableMap.Builder<String, Object>()
        .put("host", input.getHost())
        .put("tier", input.getTier())
        .put("type", input.getType())
        .put("priority", input.getPriority())
        .put("currSize", input.getCurrSize())
        .put("maxSize", input.getMaxSize())
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
  @Produces("application/json")
  public Response getClusterServers(
      @QueryParam("full") String full,
      @QueryParam("simple") String simple
  )
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);

    if (full != null) {
      return builder.entity(Lists.newArrayList(serverInventoryView.getInventory())).build();
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
  @Produces("application/json")
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

    return builder.entity(server)
                  .build();
  }

  @GET
  @Path("/{serverName}/segments")
  @Produces("application/json")
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
              public String apply(@Nullable DataSegment segment)
              {
                return segment.getIdentifier();
              }
            }
        )
    ).build();
  }

  @GET
  @Path("/{serverName}/segments/{segmentId}")
  @Produces("application/json")
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
