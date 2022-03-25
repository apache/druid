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
import com.google.common.collect.Maps;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import org.apache.druid.client.selector.Server;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.discovery.NodeRoles;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.server.http.security.StateResourceFilter;
import org.apache.druid.server.router.TieredBrokerHostSelector;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
@Path("/druid/router/v1")
public class RouterResource
{
  private final TieredBrokerHostSelector tieredBrokerHostSelector;
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final Set<NodeRole> nodeRoles;

  @Inject
  public RouterResource(
      final TieredBrokerHostSelector tieredBrokerHostSelector,
      final DruidNodeDiscoveryProvider discoveryProvider,
      final @Global Set<NodeRole> allNodeRoles)
  {
    this.tieredBrokerHostSelector = tieredBrokerHostSelector;
    this.druidNodeDiscoveryProvider = discoveryProvider;
    this.nodeRoles = allNodeRoles;
  }

  @GET
  @Path("/brokers")
  @ResourceFilters(StateResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Map<String, List<String>> getBrokers()
  {
    Map<String, List<Server>> brokerSelectorMap = tieredBrokerHostSelector.getAllBrokers();

    Map<String, List<String>> brokersMap = Maps.newHashMapWithExpectedSize(brokerSelectorMap.size());

    for (Map.Entry<String, List<Server>> e : brokerSelectorMap.entrySet()) {
      brokersMap.put(e.getKey(), e.getValue().stream().map(s -> s.getHost()).collect(Collectors.toList()));
    }

    return brokersMap;
  }

  /**
   * Returns a map of all services in the cluster, including extension services.
   * Returns the map sorted by name so that output is stable. Visible on the
   * router so clients can get a full cluster view, even if one of the services
   * is down (assuming the router survives.)
   *
   * @see {@code /druid/coordinator/v1/cluster} for a similar API.
   */
  @GET
  @Path("/cluster")
  @ResourceFilters(StateResourceFilter.class)
  @Produces(MediaType.APPLICATION_JSON)
  public Response getCluster(@QueryParam("full") boolean full)
  {
    List<NodeRole> roles = new ArrayList<>(nodeRoles);
    roles.sort((r1, r2) -> r1.getJsonName().compareTo(r2.getJsonName()));
    ImmutableMap.Builder<NodeRole, Object> entityBuilder = new ImmutableMap.Builder<>();
    for (NodeRole role : roles) {
      Collection<Object> services = NodeRoles.getNodes(druidNodeDiscoveryProvider, role, full);
      if (!services.isEmpty()) {
        entityBuilder.put(role, services);
      }
    }
    return Response
        .ok()
        .entity(entityBuilder.build())
        .build();
  }
}
