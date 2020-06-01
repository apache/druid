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

package org.apache.druid.server.http;

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.InventoryView;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

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

  private static Map<String, Object> makeFullServer(DruidServer server)
  {
    return new ImmutableMap.Builder<String, Object>()
        .put("host", server.getHost())
        .put("maxSize", server.getMaxSize())
        .put("type", server.getType().toString())
        .put("tier", server.getTier())
        .put("priority", server.getPriority())
        .put("segments", createLazySegmentsMap(server))
        .put("currSize", server.getCurrSize())
        .build();
  }

  /**
   * Emitting this map with "full" options in this resource is excessive because segment ids are repeated in the keys
   * *and* values, but is done so for compatibility with the existing HTTP JSON API.
   *
   * Returns a lazy map suitable for serialization (i. e. entrySet iteration) only, relying on the fact that the
   * segments returned from {@link DruidServer#iterateAllSegments()} are unique. This is not a part of the {@link
   * DruidServer} API to not let abuse this map (like trying to get() from it).
   */
  private static Map<SegmentId, DataSegment> createLazySegmentsMap(DruidServer server)
  {
    return new AbstractMap<SegmentId, DataSegment>()
    {
      @Override
      public Set<Entry<SegmentId, DataSegment>> entrySet()
      {
        return new AbstractSet<Entry<SegmentId, DataSegment>>()
        {
          @Override
          public Iterator<Entry<SegmentId, DataSegment>> iterator()
          {
            return Iterators.transform(
                server.iterateAllSegments().iterator(),
                segment -> new AbstractMap.SimpleImmutableEntry<>(segment.getId(), segment)
            );
          }

          @Override
          public int size()
          {
            return server.getTotalSegments();
          }
        };
      }
    };
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
  public Response getClusterServers(@QueryParam("full") String full, @QueryParam("simple") String simple)
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);

    if (full != null) {
      return builder
          .entity(Collections2.transform(serverInventoryView.getInventory(), ServersResource::makeFullServer))
          .build();
    } else if (simple != null) {
      return builder
          .entity(Collections2.transform(serverInventoryView.getInventory(), ServersResource::makeSimpleServer))
          .build();
    }

    return builder
        .entity(Collections2.transform(serverInventoryView.getInventory(), DruidServer::getHost))
        .build();
  }

  @GET
  @Path("/{serverName}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServer(@PathParam("serverName") String serverName, @QueryParam("simple") String simple)
  {
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    Response.ResponseBuilder builder = Response.status(Response.Status.OK);

    if (simple != null) {
      return builder.entity(makeSimpleServer(server)).build();
    }

    return builder.entity(makeFullServer(server)).build();
  }

  @GET
  @Path("/{serverName}/segments")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServerSegments(@PathParam("serverName") String serverName, @QueryParam("full") String full)
  {
    Response.ResponseBuilder builder = Response.status(Response.Status.OK);
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    if (full != null) {
      return builder.entity(server.iterateAllSegments()).build();
    }

    return builder
        .entity(Iterables.transform(server.iterateAllSegments(), DataSegment::getId))
        .build();
  }

  @GET
  @Path("/{serverName}/segments/{segmentId}")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getServerSegment(@PathParam("serverName") String serverName, @PathParam("segmentId") String segmentId)
  {
    DruidServer server = serverInventoryView.getInventoryValue(serverName);
    if (server == null) {
      return Response.status(Response.Status.NOT_FOUND).build();
    }

    for (SegmentId possibleSegmentId : SegmentId.iterateAllPossibleParsings(segmentId)) {
      DataSegment segment = server.getSegment(possibleSegmentId);
      if (segment != null) {
        return Response.status(Response.Status.OK).entity(segment).build();
      }
    }

    return Response.status(Response.Status.NOT_FOUND).build();
  }
}
