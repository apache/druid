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

package org.apache.druid.server.system.query;

import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.system.table.SystemTableDescriptor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class SystemTableNodeLocatorTest
{
  /**
   * Every discovered node with one of the table's node roles serves the table.
   */
  @Test
  public void testLocatesEveryNodeOfTheTableRoles()
  {
    final DiscoveryDruidNode first = node(8081);
    final DiscoveryDruidNode second = node(8082);
    final SystemTableNodeLocator locator = new SystemTableNodeLocator(discoveryProvider(List.of(first, second)));
    final SystemTableDescriptor descriptor = Mockito.mock(SystemTableDescriptor.class);
    Mockito.when(descriptor.getNodeRoles()).thenReturn(Set.of(NodeRole.OVERLORD));

    final List<SystemTableNode> nodes = locator.locate(descriptor);

    Assertions.assertEquals(List.of(first, second), nodes.stream().map(SystemTableNode::getDiscoveryNode).toList());
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

}
