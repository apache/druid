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

package org.apache.druid.curator.discovery;

import com.google.common.collect.Iterators;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LocalDruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.discovery.NodeRoles;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.Node;
import org.junit.Test;

import java.util.Collection;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Verifies that the NodeRoles class allows injecting a set of roles
 * which includes custom roles defined by an extension.
 */
public class NodeRolesTest
{
  @Test
  public void testNodeRoles()
  {
    Set<NodeRole> knownRules = NodeRoles.knownRoles();
    assertEquals(NodeRole.values().length, knownRules.size());

    NodeRole customRole = new NodeRole("custom");
    Injector injector = Guice.createInjector(
        binder -> {
          NodeRoles.bindKnownRoles(binder);
          NodeRoles.bindRole(binder, customRole);
        });
    Set<NodeRole> roles = injector.getInstance(
        Key.get(new TypeLiteral<Set<NodeRole>>(){}, Global.class));
    assertEquals(NodeRole.values().length + 1, roles.size());
    assertTrue(roles.contains(customRole));
  }

  @Test
  public void testDiscovery()
  {
    // Provider with one node
    LocalDruidNodeDiscoveryProvider provider = new LocalDruidNodeDiscoveryProvider();
    DruidNode druidNode = new DruidNode("broker", "localhost", false, 1000, -1, true, false);
    DiscoveryDruidNode dn = new DiscoveryDruidNode(druidNode, NodeRole.BROKER, null);
    provider.announce(dn);

    Collection<DiscoveryDruidNode> discoveryNodes = NodeRoles.getDiscoveryNodesForRole(provider, NodeRole.BROKER);
    assertEquals(1, discoveryNodes.size());
    assertEquals(dn, Iterators.getOnlyElement(discoveryNodes.iterator()));
    assertEquals(discoveryNodes, NodeRoles.getNodes(provider, NodeRole.BROKER, true));

    discoveryNodes = NodeRoles.getDiscoveryNodesForRole(provider, NodeRole.OVERLORD);
    assertTrue(discoveryNodes.isEmpty());

    Collection<Node> nodes = NodeRoles.getNodesForRole(provider, NodeRole.BROKER);
    assertEquals(1, nodes.size());
    Node node = Iterators.getOnlyElement(nodes.iterator());
    assertEquals(node, new Node(druidNode));
    assertEquals(node, Iterators.getOnlyElement(
        NodeRoles.getNodes(provider, NodeRole.BROKER, false).iterator()));

    nodes = NodeRoles.getNodesForRole(provider, NodeRole.OVERLORD);
    assertTrue(nodes.isEmpty());
  }
}
