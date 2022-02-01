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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.discovery.NodeRoles;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 */
@Path("/druid/coordinator/v1/cluster")
@LazySingleton
@ResourceFilters(StateResourceFilter.class)
public class ClusterResource
{
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final Set<NodeRole> nodeRoles;

  @Inject
  public ClusterResource(
      final DruidNodeDiscoveryProvider discoveryProvider,
      final @Global Set<NodeRole> allNodeRoles)
  {
    this.druidNodeDiscoveryProvider = discoveryProvider;
    this.nodeRoles = allNodeRoles;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getClusterServers(@QueryParam("full") boolean full)
  {
    // Add Druid-defined roles in a defined order.
    List<NodeRole> definedOrder = Arrays.asList(
        NodeRole.COORDINATOR,
        NodeRole.OVERLORD,
        NodeRole.BROKER,
        NodeRole.HISTORICAL,
        NodeRole.MIDDLE_MANAGER,
        NodeRole.INDEXER,
        NodeRole.ROUTER
        );

    // Omit role groups for optional services.
    Set<NodeRole> omitIfEmpty = new HashSet<>(Arrays.asList(
        NodeRole.MIDDLE_MANAGER,
        NodeRole.INDEXER,
        NodeRole.ROUTER
        ));

    ImmutableMap.Builder<NodeRole, Object> entityBuilder = new ImmutableMap.Builder<>();
    for (NodeRole role : definedOrder) {
      Collection<Object> services = NodeRoles.getNodes(druidNodeDiscoveryProvider, role, full);
      if (!omitIfEmpty.contains(role) || !services.isEmpty()) {
        entityBuilder.put(role, services);
      }
    }

    // Add any extension node roles, but only if service instances exist.
    Set<NodeRole> stockRoles = new HashSet<>(definedOrder);
    for (NodeRole role : nodeRoles) {
      if (stockRoles.contains(role)) {
        continue;
      }
      Collection<Object> services = NodeRoles.getNodes(druidNodeDiscoveryProvider, role, full);
      if (!services.isEmpty()) {
        entityBuilder.put(role, services);
      }
    }

    return Response.ok().entity(entityBuilder.build()).build();
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/{nodeRole}")
  public Response getClusterServers(@PathParam("nodeRole") NodeRole nodeRole, @QueryParam("full") boolean full)
  {
    if (nodeRole == null) {
      return Response.serverError()
                     .status(Response.Status.BAD_REQUEST)
                     .entity(
                         ImmutableMap.of(
                             "error",
                             "Invalid nodeRole of null. Valid node roles are " +
                                 Arrays.toString(NodeRole.values())))
                     .build();
    }
    return Response
        .ok()
        .entity(
            NodeRoles.getNodes(druidNodeDiscoveryProvider, nodeRole, full))
        .build();
  }
}
