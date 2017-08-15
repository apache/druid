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

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.LazySingleton;
import io.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.Collection;

/**
 */
@Path("/druid/coordinator/v1/cluster")
@LazySingleton
@ResourceFilters(StateResourceFilter.class)
public class ClusterResource
{
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;

  @Inject
  public ClusterResource(DruidNodeDiscoveryProvider discoveryProvider)
  {
    this.druidNodeDiscoveryProvider = discoveryProvider;
  }

  @GET
  @Produces(MediaType.APPLICATION_JSON)
  public Response getClusterServers()
  {
    ImmutableMap.Builder<String, Object> entityBuilder = new ImmutableMap.Builder<>();

    entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_COORDINATOR,
                      druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_COORDINATOR)
                                                .getAllNodes()
    );
    entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD,
                      druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD)
                                                .getAllNodes()
    );
    entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_BROKER,
                      druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_BROKER)
                                                .getAllNodes()
    );
    entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_HISTORICAL,
                      druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_HISTORICAL)
                                                .getAllNodes()
    );

    Collection<DiscoveryDruidNode> mmNodes = druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_MM)
                                                                       .getAllNodes();
    if (!mmNodes.isEmpty()) {
      entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_MM, mmNodes);
    }

    Collection<DiscoveryDruidNode> routerNodes = druidNodeDiscoveryProvider.getForNodeType(DruidNodeDiscoveryProvider.NODE_TYPE_ROUTER)
                                                                    .getAllNodes();
    if (!routerNodes.isEmpty()) {
      entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_ROUTER, routerNodes);
    }

    return Response.status(Response.Status.OK).entity(entityBuilder.build()).build();
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/{nodeType}")
  public Response getClusterServers(
      @PathParam("nodeType") String nodeType
  )
  {
    if (nodeType == null || !DruidNodeDiscoveryProvider.ALL_NODE_TYPES.contains(nodeType)) {
      return Response.serverError()
                     .status(Response.Status.BAD_REQUEST)
                     .entity(String.format(
                         "Invalid nodeType [%s]. Valid node types are %s .",
                         nodeType,
                         DruidNodeDiscoveryProvider.ALL_NODE_TYPES
                     ))
                     .build();
    } else {
      return Response.status(Response.Status.OK).entity(
          druidNodeDiscoveryProvider.getForNodeType(nodeType).getAllNodes()
      ).build();
    }
  }
}
