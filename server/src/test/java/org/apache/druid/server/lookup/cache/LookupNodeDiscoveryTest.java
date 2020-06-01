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

package org.apache.druid.server.lookup.cache;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.discovery.DiscoveryDruidNode;
import org.apache.druid.discovery.DruidNodeDiscovery;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.http.HostAndPortWithScheme;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 */
public class LookupNodeDiscoveryTest
{
  private DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private DruidNodeDiscovery druidNodeDiscovery;
  private LookupNodeDiscovery lookupNodeDiscovery;

  @Before
  public void setup()
  {
    druidNodeDiscoveryProvider = EasyMock.createStrictMock(DruidNodeDiscoveryProvider.class);

    druidNodeDiscovery = EasyMock.createStrictMock(DruidNodeDiscovery.class);

    EasyMock.expect(druidNodeDiscoveryProvider.getForService(LookupNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);

    DiscoveryDruidNode node1 = new DiscoveryDruidNode(
        new DruidNode("s1", "h1", false, 8080, null, true, false),
        NodeRole.HISTORICAL,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier1"))
    );

    DiscoveryDruidNode node2 = new DiscoveryDruidNode(
        new DruidNode("s2", "h2", false, 8080, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier1"))
    );

    DiscoveryDruidNode node3 = new DiscoveryDruidNode(
        new DruidNode("s3", "h3", false, 8080, null, true, false),
        NodeRole.PEON,
        ImmutableMap.of(
            LookupNodeService.DISCOVERY_SERVICE_KEY, new LookupNodeService("tier2"))
    );

    EasyMock.expect(druidNodeDiscovery.getAllNodes())
            .andReturn(ImmutableSet.of(node1, node2, node3))
            .anyTimes();

    EasyMock.replay(druidNodeDiscoveryProvider, druidNodeDiscovery);

    lookupNodeDiscovery = new LookupNodeDiscovery(druidNodeDiscoveryProvider);
  }

  @Test
  public void testGetNodesInTier()
  {
    Assert.assertEquals(
        ImmutableList.of(
            HostAndPortWithScheme.fromParts("http", "h1", 8080),
            HostAndPortWithScheme.fromParts("http", "h2", 8080)
        ),
        ImmutableList.copyOf(lookupNodeDiscovery.getNodesInTier("tier1"))
    );

    Assert.assertEquals(
        ImmutableList.of(
            HostAndPortWithScheme.fromParts("http", "h3", 8080)
        ),
        ImmutableList.copyOf(lookupNodeDiscovery.getNodesInTier("tier2"))
    );

    Assert.assertEquals(
        ImmutableList.of(),
        ImmutableList.copyOf(lookupNodeDiscovery.getNodesInTier("tier3"))
    );

    EasyMock.verify(druidNodeDiscoveryProvider, druidNodeDiscovery);
  }

  @Test
  public void testGetAllTiers()
  {
    Assert.assertEquals(
        ImmutableSet.of("tier1", "tier2"),
        lookupNodeDiscovery.getAllTiers()
    );

    EasyMock.verify(druidNodeDiscoveryProvider, druidNodeDiscovery);
  }
}
