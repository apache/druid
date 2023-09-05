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

package org.apache.druid.server.coordinator.loading;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.DruidCluster;
import org.apache.druid.server.coordinator.ServerHolder;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

public class RoundRobinServerSelectorTest
{
  private static final String TIER = "normal";

  private final DataSegment segment = new DataSegment(
      "wiki",
      Intervals.of("2022-01-01/2022-01-02"),
      "1",
      Collections.emptyMap(),
      Collections.emptyList(),
      Collections.emptyList(),
      new NumberedShardSpec(1, 10),
      IndexIO.CURRENT_VERSION_ID,
      100
  );

  @Test
  public void testSingleIterator()
  {
    final ServerHolder serverXL = createHistorical("serverXL", 1000);
    final ServerHolder serverL = createHistorical("serverL", 900);
    final ServerHolder serverM = createHistorical("serverM", 800);

    // This server is too small to house the segment
    final ServerHolder serverXS = createHistorical("serverXS", 10);

    DruidCluster cluster = DruidCluster
        .builder()
        .addTier(TIER, serverXL, serverM, serverXS, serverL)
        .build();
    final RoundRobinServerSelector selector = new RoundRobinServerSelector(cluster);

    // Verify that only eligible servers are returned in order of available size
    Iterator<ServerHolder> pickedServers = selector.getServersInTierToLoadSegment(TIER, segment);

    Assert.assertTrue(pickedServers.hasNext());
    Assert.assertEquals(serverXL, pickedServers.next());
    Assert.assertEquals(serverL, pickedServers.next());
    Assert.assertEquals(serverM, pickedServers.next());

    Assert.assertFalse(pickedServers.hasNext());
  }

  @Test
  public void testNextIteratorContinuesFromSamePosition()
  {
    final ServerHolder serverXL = createHistorical("serverXL", 1000);
    final ServerHolder serverL = createHistorical("serverL", 900);
    final ServerHolder serverM = createHistorical("serverM", 800);

    // This server is too small to house the segment
    final ServerHolder serverXS = createHistorical("serverXS", 10);

    DruidCluster cluster = DruidCluster
        .builder()
        .addTier(TIER, serverXL, serverM, serverXS, serverL)
        .build();
    final RoundRobinServerSelector selector = new RoundRobinServerSelector(cluster);

    // Verify that only eligible servers are returned in order of available size
    Iterator<ServerHolder> pickedServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertTrue(pickedServers.hasNext());
    Assert.assertEquals(serverXL, pickedServers.next());

    // Second iterator starts from previous position but resets allowed number of iterations
    pickedServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertTrue(pickedServers.hasNext());

    Assert.assertEquals(serverL, pickedServers.next());
    Assert.assertEquals(serverM, pickedServers.next());
    Assert.assertEquals(serverXL, pickedServers.next());

    Assert.assertFalse(pickedServers.hasNext());
  }

  @Test
  public void testNoServersInTier()
  {
    DruidCluster cluster = DruidCluster.builder().addTier(TIER).build();
    final RoundRobinServerSelector selector = new RoundRobinServerSelector(cluster);

    Iterator<ServerHolder> eligibleServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertFalse(eligibleServers.hasNext());
  }

  @Test
  public void testNoEligibleServerInTier()
  {
    DruidCluster cluster = DruidCluster.builder().addTier(
        TIER,
        createHistorical("server1", 40),
        createHistorical("server2", 30),
        createHistorical("server3", 10),
        createHistorical("server4", 20)
    ).build();
    final RoundRobinServerSelector selector = new RoundRobinServerSelector(cluster);

    // Verify that only eligible servers are returned in order of available size
    Iterator<ServerHolder> eligibleServers = selector.getServersInTierToLoadSegment(TIER, segment);
    Assert.assertFalse(eligibleServers.hasNext());
  }

  private ServerHolder createHistorical(String name, long size)
  {
    return new ServerHolder(
        new DruidServer(name, name, null, size, ServerType.HISTORICAL, TIER, 1)
            .toImmutableDruidServer(),
        new TestLoadQueuePeon()
    );
  }
}
