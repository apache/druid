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

import com.google.common.collect.Iterators;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.LocalDruidNodeDiscoveryProvider;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.discovery.NodeRoles;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.Node;
import org.junit.Test;

import javax.ws.rs.core.Response;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ClusterResourceTest
{
  private void assertOk(Response resp)
  {
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
  }

  @SuppressWarnings("unchecked")
  private Map<Object, Collection<Object>> mapResult(Response resp)
  {
    assertOk(resp);
    return (Map<Object, Collection<Object>>) resp.getEntity();
  }

  @Test
  public void testNoNodes()
  {
    LocalDruidNodeDiscoveryProvider provider = new LocalDruidNodeDiscoveryProvider();
    ClusterResource resource = new ClusterResource(provider, NodeRoles.knownRoles());

    int emptyCount = 4;
    {
      Response resp = resource.getClusterServers(true);
      Map<Object, Collection<Object>> results = mapResult(resp);
      assertEquals(emptyCount, results.size());
      assertTrue(results.containsKey(NodeRole.COORDINATOR));
      assertTrue(results.containsKey(NodeRole.OVERLORD));
      assertTrue(results.containsKey(NodeRole.BROKER));
      assertTrue(results.containsKey(NodeRole.HISTORICAL));
    }

    {
      Response resp = resource.getClusterServers(false);
      Map<Object, Collection<Object>> results = mapResult(resp);
      assertEquals(emptyCount, results.size());
    }

    {
      Response resp = resource.getClusterServers(NodeRole.BROKER, true);
      assertOk(resp);
      @SuppressWarnings("unchecked")
      Collection<Object> results = (Collection<Object>) resp.getEntity();
      assertTrue(results.isEmpty());
    }

    {
      Response resp = resource.getClusterServers(NodeRole.BROKER, false);
      assertOk(resp);
      @SuppressWarnings("unchecked")
      Collection<Object> results = (Collection<Object>) resp.getEntity();
      assertTrue(results.isEmpty());
    }
  }

  @Test
  public void testNullNodeRole()
  {
    LocalDruidNodeDiscoveryProvider provider = new LocalDruidNodeDiscoveryProvider();
    ClusterResource resource = new ClusterResource(provider, NodeRoles.knownRoles());

    Response resp = resource.getClusterServers(null, true);
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
    @SuppressWarnings("unchecked")
    Map<String, String> results = (Map<String, String>) resp.getEntity();
    assertNotNull(results.get("error"));
  }

  private void announce(LocalDruidNodeDiscoveryProvider provider, NodeRole role, int count)
  {
    String service = StringUtils.toLowerCase(role.getJsonName());
    for (int i = 1; i < count + 1; i++) {
      DruidNode druidNode = new DruidNode(service, service + i, false, 1000, -1, true, false);
      DiscoveryDruidNode dn = new DiscoveryDruidNode(druidNode, role, null);
      provider.announce(dn);
    }
  }

  @Test
  public void testAllTypes()
  {
    LocalDruidNodeDiscoveryProvider provider = new LocalDruidNodeDiscoveryProvider();
    announce(provider, NodeRole.BROKER, 2);
    announce(provider, NodeRole.ROUTER, 3);
    announce(provider, NodeRole.COORDINATOR, 4);
    announce(provider, NodeRole.OVERLORD, 5);
    announce(provider, NodeRole.HISTORICAL, 6);
    announce(provider, NodeRole.MIDDLE_MANAGER, 7);
    announce(provider, NodeRole.INDEXER, 8);
    ClusterResource resource = new ClusterResource(provider, NodeRoles.knownRoles());

    {
      Response resp = resource.getClusterServers(true);
      Map<Object, Collection<Object>> results = mapResult(resp);
      assertEquals(7, results.size());
      assertEquals(2, results.get(NodeRole.BROKER).size());
      assertEquals(3, results.get(NodeRole.ROUTER).size());
      assertEquals(4, results.get(NodeRole.COORDINATOR).size());
      assertEquals(5, results.get(NodeRole.OVERLORD).size());
      assertEquals(6, results.get(NodeRole.HISTORICAL).size());
      assertEquals(7, results.get(NodeRole.MIDDLE_MANAGER).size());
      assertEquals(8, results.get(NodeRole.INDEXER).size());
    }

    {
      Response resp = resource.getClusterServers(NodeRole.BROKER, false);
      assertOk(resp);
      @SuppressWarnings("unchecked")
      Collection<Object> results = (Collection<Object>) resp.getEntity();
      assertEquals(2, results.size());
    }
  }

  @Test
  public void testExtensionService()
  {
    LocalDruidNodeDiscoveryProvider provider = new LocalDruidNodeDiscoveryProvider();
    announce(provider, NodeRole.BROKER, 1);
    announce(provider, NodeRole.COORDINATOR, 1);
    announce(provider, NodeRole.OVERLORD, 1);
    NodeRole customRole = new NodeRole("custom");
    announce(provider, customRole, 1);
    Set<NodeRole> roles = new HashSet<>(NodeRoles.knownRoles());
    roles.add(customRole);

    ClusterResource resource = new ClusterResource(provider, roles);

    {
      Response resp = resource.getClusterServers(true);
      Map<Object, Collection<Object>> results = mapResult(resp);
      // 4 standard + custom
      assertEquals(5, results.size());
      assertEquals(1, results.get(NodeRole.BROKER).size());
      assertEquals(1, results.get(customRole).size());
    }

    {
      Response resp = resource.getClusterServers(customRole, false);
      assertOk(resp);
      @SuppressWarnings("unchecked")
      Collection<Object> results = (Collection<Object>) resp.getEntity();
      assertEquals(1, results.size());
    }

    {
      // Unknown role is the same as a known role with no instances
      NodeRole bogus = new NodeRole("gogus");
      Response resp = resource.getClusterServers(bogus, false);
      assertOk(resp);
      @SuppressWarnings("unchecked")
      Collection<Object> results = (Collection<Object>) resp.getEntity();
      assertTrue(results.isEmpty());
    }
  }

  @Test
  public void testFormat()
  {
    LocalDruidNodeDiscoveryProvider provider = new LocalDruidNodeDiscoveryProvider();
    announce(provider, NodeRole.BROKER, 1);
    ClusterResource resource = new ClusterResource(provider, NodeRoles.knownRoles());

    {
      Response resp = resource.getClusterServers(true);
      Map<Object, Collection<Object>> results = mapResult(resp);
      assertTrue(Iterators.getOnlyElement(results.get(NodeRole.BROKER).iterator()) instanceof DiscoveryDruidNode);
    }

    {
      Response resp = resource.getClusterServers(false);
      Map<Object, Collection<Object>> results = mapResult(resp);
      assertTrue(Iterators.getOnlyElement(results.get(NodeRole.BROKER).iterator()) instanceof Node);
    }

    {
      Response resp = resource.getClusterServers(NodeRole.BROKER, true);
      assertOk(resp);
      @SuppressWarnings("unchecked")
      Collection<Object> results = (Collection<Object>) resp.getEntity();
      assertTrue(Iterators.getOnlyElement(results.iterator()) instanceof DiscoveryDruidNode);
    }

    {
      Response resp = resource.getClusterServers(NodeRole.BROKER, false);
      assertOk(resp);
      @SuppressWarnings("unchecked")
      Collection<Object> results = (Collection<Object>) resp.getEntity();
      assertTrue(Iterators.getOnlyElement(results.iterator()) instanceof Node);
    }
  }
}
