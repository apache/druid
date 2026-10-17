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

package org.apache.druid.server.system.handler;

import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.DirectDruidClient;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.apache.druid.server.system.table.SystemTableRoutingMode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class SystemTableNodeLocatorTest
{
  /** {@link SystemTableRoutingMode#ALL_NODES} selects every discovered node for the configured role. */
  @Test
  public void testAllNodesRouting()
  {
    final DiscoveryDruidNode first = node(8081);
    final DiscoveryDruidNode second = node(8082);
    final SystemTableNodeLocator locator = new SystemTableNodeLocator(
        discoveryProvider(List.of(first, second)),
        Mockito.mock(CoordinatorClient.class),
        Mockito.mock(OverlordClient.class)
    );
    final SystemTableDescriptor descriptor = descriptor(SystemTableRoutingMode.ALL_NODES);

    final List<SystemTableNode> nodes = locator.locate(descriptor, query());

    Assertions.assertEquals(2, nodes.size());
  }

  /** {@link SystemTableRoutingMode#LEADER_ONLY} excludes healthy and unavailable Overlord standbys. */
  @Test
  public void testLeaderOnlyRouting()
  {
    final DiscoveryDruidNode standby = node(8081);
    final DiscoveryDruidNode leader = node(8082);
    final OverlordClient overlordClient = Mockito.mock(OverlordClient.class);
    Mockito.when(overlordClient.findCurrentLeader())
           .thenReturn(Futures.immediateFuture(URI.create("http://localhost:8082")));
    final SystemTableNodeLocator locator = new SystemTableNodeLocator(
        discoveryProvider(List.of(standby, leader)),
        Mockito.mock(CoordinatorClient.class),
        overlordClient
    );

    final List<SystemTableNode> nodes = locator.locate(
        descriptor(SystemTableRoutingMode.LEADER_ONLY),
        query()
    );

    Assertions.assertEquals(1, nodes.size());
    Assertions.assertEquals(leader, nodes.get(0).getDiscoveryNode());
  }

  /** Leader routing is role-driven and therefore also supports a Coordinator locator without special-case logic. */
  @Test
  public void testLeaderOnlyRoutingUsesLocatorRegisteredForRole()
  {
    final DiscoveryDruidNode standby = node(NodeRole.COORDINATOR, 8081);
    final DiscoveryDruidNode leader = node(NodeRole.COORDINATOR, 8082);
    final CoordinatorClient coordinatorClient = Mockito.mock(CoordinatorClient.class);
    Mockito.when(coordinatorClient.findCurrentLeader())
           .thenReturn(Futures.immediateFuture(URI.create("http://localhost:8082")));
    final SystemTableNodeLocator locator = new SystemTableNodeLocator(
        discoveryProvider(NodeRole.COORDINATOR, List.of(standby, leader)),
        coordinatorClient,
        Mockito.mock(OverlordClient.class)
    );

    final List<SystemTableNode> nodes = locator.locate(
        descriptor(NodeRole.COORDINATOR, SystemTableRoutingMode.LEADER_ONLY),
        query()
    );

    Assertions.assertEquals(1, nodes.size());
    Assertions.assertEquals(leader, nodes.get(0).getDiscoveryNode());
  }

  /** Leader resolution happens before discovery so a newly elected leader is read from a fresh node snapshot. */
  @Test
  public void testLeaderOnlyRoutingDiscoversNodesAfterResolvingLeader()
  {
    final DiscoveryDruidNode leader = node(8082);
    final AtomicReference<List<DiscoveryDruidNode>> discoveredNodes = new AtomicReference<>(Collections.emptyList());
    final DruidNodeDiscovery discovery = Mockito.mock(DruidNodeDiscovery.class);
    Mockito.when(discovery.getAllNodes()).thenAnswer(ignored -> discoveredNodes.get());
    final DruidNodeDiscoveryProvider provider = Mockito.mock(DruidNodeDiscoveryProvider.class);
    Mockito.when(provider.getForNodeRole(NodeRole.OVERLORD)).thenReturn(discovery);
    final OverlordClient overlordClient = Mockito.mock(OverlordClient.class);
    Mockito.when(overlordClient.findCurrentLeader()).thenAnswer(ignored -> {
      discoveredNodes.set(List.of(leader));
      return Futures.immediateFuture(URI.create("http://localhost:8082"));
    });
    final SystemTableNodeLocator locator = new SystemTableNodeLocator(
        provider,
        Mockito.mock(CoordinatorClient.class),
        overlordClient
    );

    final List<SystemTableNode> nodes = locator.locate(
        descriptor(SystemTableRoutingMode.LEADER_ONLY),
        query()
    );

    Assertions.assertEquals(List.of(leader), nodes.stream().map(SystemTableNode::getDiscoveryNode).toList());
  }

  private static SystemTableDescriptor descriptor(final SystemTableRoutingMode routingMode)
  {
    return descriptor(NodeRole.OVERLORD, routingMode);
  }

  private static SystemTableDescriptor descriptor(
      final NodeRole nodeRole,
      final SystemTableRoutingMode routingMode
  )
  {
    final SystemTableDescriptor descriptor = Mockito.mock(SystemTableDescriptor.class);
    Mockito.when(descriptor.getNodeRoles()).thenReturn(Set.of(nodeRole));
    Mockito.when(descriptor.getRoutingMode()).thenReturn(routingMode);
    return descriptor;
  }

  private static DruidNodeDiscoveryProvider discoveryProvider(final List<DiscoveryDruidNode> nodes)
  {
    return discoveryProvider(NodeRole.OVERLORD, nodes);
  }

  private static DruidNodeDiscoveryProvider discoveryProvider(
      final NodeRole nodeRole,
      final List<DiscoveryDruidNode> nodes
  )
  {
    final DruidNodeDiscovery discovery = Mockito.mock(DruidNodeDiscovery.class);
    Mockito.when(discovery.getAllNodes()).thenReturn(nodes);
    final DruidNodeDiscoveryProvider provider = Mockito.mock(DruidNodeDiscoveryProvider.class);
    Mockito.when(provider.getForNodeRole(nodeRole)).thenReturn(discovery);
    return provider;
  }

  private static DiscoveryDruidNode node(final int port)
  {
    return node(NodeRole.OVERLORD, port);
  }

  private static DiscoveryDruidNode node(final NodeRole nodeRole, final int port)
  {
    return new DiscoveryDruidNode(
        new DruidNode(nodeRole.getJsonName(), "localhost", false, port, null, true, false),
        nodeRole,
        Collections.emptyMap()
    );
  }

  private static Query<?> query()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource("test")
                 .eternityInterval()
                 .context(Map.of(DirectDruidClient.QUERY_FAIL_TIME, Long.MAX_VALUE))
                 .build();
  }
}
