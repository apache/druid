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
import static org.junit.Assert.assertTrue;

public class RouterResourceTest
{
  private void announce(LocalDruidNodeDiscoveryProvider provider, NodeRole role, int count)
  {
    String service = StringUtils.toLowerCase(role.getJsonName());
    for (int i = 1; i < count + 1; i++) {
      DruidNode druidNode = new DruidNode(service, service + i, false, 1000, -1, true, false);
      DiscoveryDruidNode dn = new DiscoveryDruidNode(druidNode, role, null);
      provider.announce(dn);
    }
  }

  /**
   * Verify that the /cluster endpoint works, and includes extension roles.
   */
  @Test
  public void testCluster()
  {
    LocalDruidNodeDiscoveryProvider provider = new LocalDruidNodeDiscoveryProvider();
    announce(provider, NodeRole.BROKER, 1);
    announce(provider, NodeRole.COORDINATOR, 1);
    announce(provider, NodeRole.OVERLORD, 1);
    NodeRole customRole = new NodeRole("custom");
    announce(provider, customRole, 1);
    Set<NodeRole> roles = new HashSet<>(NodeRoles.knownRoles());
    roles.add(customRole);
    RouterResource resource = new RouterResource(null, provider, roles);

    {
      Response resp = resource.getCluster(true);
      assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
      @SuppressWarnings("unchecked")
      Map<Object, Collection<Object>> map = (Map<Object, Collection<Object>>) resp.getEntity();
      assertEquals(4, map.size());
      assertTrue(Iterators.getOnlyElement(map.get(customRole).iterator()) instanceof DiscoveryDruidNode);
      assertEquals(1, map.get(NodeRole.BROKER).size());
      assertEquals(1, map.get(NodeRole.COORDINATOR).size());
      assertEquals(1, map.get(NodeRole.OVERLORD).size());
    }

    {
      Response resp = resource.getCluster(false);
      assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
      @SuppressWarnings("unchecked")
      Map<Object, Collection<Object>> map = (Map<Object, Collection<Object>>) resp.getEntity();
      assertEquals(4, map.size());
      assertTrue(Iterators.getOnlyElement(map.get(customRole).iterator()) instanceof Node);
      assertEquals(1, map.get(NodeRole.BROKER).size());
      assertEquals(1, map.get(NodeRole.COORDINATOR).size());
      assertEquals(1, map.get(NodeRole.OVERLORD).size());
    }
  }
}
