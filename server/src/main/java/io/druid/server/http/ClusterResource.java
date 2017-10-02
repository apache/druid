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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.sun.jersey.spi.container.ResourceFilters;
import io.druid.discovery.DiscoveryDruidNode;
import io.druid.discovery.DruidNodeDiscoveryProvider;
import io.druid.guice.LazySingleton;
import io.druid.java.util.common.StringUtils;
import io.druid.server.DruidNode;
import io.druid.server.http.security.StateResourceFilter;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
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
  public Response getClusterServers(
      @QueryParam("full") boolean full
  )
  {
    ImmutableMap.Builder<String, Object> entityBuilder = new ImmutableMap.Builder<>();

    entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_COORDINATOR,
                      getNodes(DruidNodeDiscoveryProvider.NODE_TYPE_COORDINATOR, full)
    );
    entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD,
                      getNodes(DruidNodeDiscoveryProvider.NODE_TYPE_OVERLORD, full)
    );
    entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_BROKER,
                      getNodes(DruidNodeDiscoveryProvider.NODE_TYPE_BROKER, full)
    );
    entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_HISTORICAL,
                      getNodes(DruidNodeDiscoveryProvider.NODE_TYPE_HISTORICAL, full)
    );

    Collection<Object> mmNodes = getNodes(DruidNodeDiscoveryProvider.NODE_TYPE_MM, full);
    if (!mmNodes.isEmpty()) {
      entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_MM, mmNodes);
    }

    Collection<Object> routerNodes = getNodes(DruidNodeDiscoveryProvider.NODE_TYPE_ROUTER, full);
    if (!routerNodes.isEmpty()) {
      entityBuilder.put(DruidNodeDiscoveryProvider.NODE_TYPE_ROUTER, routerNodes);
    }

    return Response.status(Response.Status.OK).entity(entityBuilder.build()).build();
  }

  @GET
  @Produces({MediaType.APPLICATION_JSON})
  @Path("/{nodeType}")
  public Response getClusterServers(
      @PathParam("nodeType") String nodeType,
      @QueryParam("full") boolean full
  )
  {
    if (nodeType == null || !DruidNodeDiscoveryProvider.ALL_NODE_TYPES.contains(nodeType)) {
      return Response.serverError()
                     .status(Response.Status.BAD_REQUEST)
                     .entity(StringUtils.format(
                         "Invalid nodeType [%s]. Valid node types are %s .",
                         nodeType,
                         DruidNodeDiscoveryProvider.ALL_NODE_TYPES
                     ))
                     .build();
    } else {
      return Response.status(Response.Status.OK).entity(
          getNodes(nodeType, full)
      ).build();
    }
  }

  private Collection<Object> getNodes(String nodeType, boolean full)
  {
    Collection<DiscoveryDruidNode> discoveryDruidNodes = druidNodeDiscoveryProvider.getForNodeType(nodeType)
                                                                                   .getAllNodes();
    if (full) {
      return (Collection) discoveryDruidNodes;
    } else {
      return Collections2.transform(
          discoveryDruidNodes,
          (discoveryDruidNode) -> Node.from(discoveryDruidNode.getDruidNode())
      );
    }
  }

  @JsonInclude(JsonInclude.Include.NON_NULL)
  private static class Node
  {
    private final String host;
    private final String service;
    private final Integer plaintextPort;
    private final Integer tlsPort;

    @JsonCreator
    public Node(String host, String service, Integer plaintextPort, Integer tlsPort)
    {
      this.host = host;
      this.service = service;
      this.plaintextPort = plaintextPort;
      this.tlsPort = tlsPort;
    }

    @JsonProperty
    public String getHost()
    {
      return host;
    }

    @JsonProperty
    public String getService()
    {
      return service;
    }

    @JsonProperty
    public Integer getPlaintextPort()
    {
      return plaintextPort;
    }

    @JsonProperty
    public Integer getTlsPort()
    {
      return tlsPort;
    }

    public static Node from(DruidNode druidNode)
    {
      return new Node(
          druidNode.getHost(),
          druidNode.getServiceName(),
          druidNode.getPlaintextPort() > 0 ? druidNode.getPlaintextPort() : null,
          druidNode.getTlsPort() > 0 ? druidNode.getTlsPort() : null
      );
    }
  }
}
