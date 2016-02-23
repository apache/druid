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

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
import io.druid.client.DruidServerDiscovery;
import io.druid.server.coordination.DruidServerMetadata;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;

/**
 */
@Path("/druid/coordinator/v1/nodes")
public class ClusterInfoResource
{

  private static final String HISTORICAL = "historical";
  private static final String COORDINATOR = "coordinator";
  private static final String OVERLORD = "overlord";
  private static final String BROKER = "broker";
  private static final String ROUTER = "router";
  private static final String REALTIME = "realtime";

  private final DruidServerDiscovery discovery;

  @Inject
  public ClusterInfoResource(DruidServerDiscovery discovery)
  {
    this.discovery = discovery;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getClusterInfo()
  {
    final Map<String, List<DruidServerMetadata>> clusterInfo = Maps.newHashMap();
    try {
      clusterInfo.put(COORDINATOR, discovery.getServersForType(COORDINATOR));
      clusterInfo.put(OVERLORD, discovery.getServersForType(OVERLORD));
      clusterInfo.put(HISTORICAL, discovery.getServersForType(HISTORICAL));
      clusterInfo.put(BROKER, discovery.getServersForType(BROKER));
      clusterInfo.put(ROUTER, discovery.getServersForType(ROUTER));
      clusterInfo.put(REALTIME, discovery.getServersForType(REALTIME));
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
    return Response.ok(clusterInfo).build();
  }

  @GET
  @Path("/historical")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getHistoricalInfo()
  {
    final Map<String, List<DruidServerMetadata>> clusterInfo = Maps.newHashMap();
    try {
      clusterInfo.put(HISTORICAL, discovery.getServersForType(HISTORICAL));
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
    return Response.ok(clusterInfo).build();
  }

  @GET
  @Path("/overlord")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getOverlordInfo()
  {
    final Map<String, List<DruidServerMetadata>> clusterInfo = Maps.newHashMap();
    try {
      clusterInfo.put(OVERLORD, discovery.getServersForType(OVERLORD));
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
    return Response.ok(clusterInfo).build();
  }

  @GET
  @Path("/broker")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getBrokerInfo()
  {
    final Map<String, List<DruidServerMetadata>> clusterInfo = Maps.newHashMap();
    try {
      clusterInfo.put(BROKER, discovery.getServersForType(BROKER));
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
    return Response.ok(clusterInfo).build();
  }

  @GET
  @Path("/coordinator")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCoordinatorInfo()
  {
    final Map<String, List<DruidServerMetadata>> clusterInfo = Maps.newHashMap();
    try {
      clusterInfo.put(COORDINATOR, discovery.getServersForType(COORDINATOR));
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
    return Response.ok(clusterInfo).build();
  }

  @GET
  @Path("/router")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRouterInfo()
  {
    final Map<String, List<DruidServerMetadata>> clusterInfo = Maps.newHashMap();
    try {
      clusterInfo.put(ROUTER, discovery.getServersForType(ROUTER));
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
    return Response.ok(clusterInfo).build();
  }

  @GET
  @Path("/realtime")
  @Produces(MediaType.APPLICATION_JSON)
  public Response getRealtimeInfo()
  {
    final Map<String, List<DruidServerMetadata>> clusterInfo = Maps.newHashMap();
    try {
      clusterInfo.put(REALTIME, discovery.getServersForType(REALTIME));
    }
    catch (Exception e) {
      return Response.status(Response.Status.BAD_REQUEST)
                     .entity(ImmutableMap.<String, Object>of("error", e.getMessage()))
                     .build();
    }
    return Response.ok(clusterInfo).build();
  }


}
