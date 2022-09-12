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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.client.DruidServer;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.server.coordinator.CostBalancerStrategyFactory;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Coordinator simulation test to verify behaviour of segment balancing.
 */
public class SegmentBalancingTest extends CoordinatorSimulationBaseTest
{
  @Test
  public void testBalancingWithInventoryViewUpdates()
  {
    testBalancingWhenInventoryIsSynced(true);

    // Fix https://github.com/apache/druid/issues/12881 to enable this test case
    // testBalancingWhenInventoryIsSynced(false);
  }

  private void testBalancingWhenInventoryIsSynced(boolean syncInventory)
  {
    final String datasource = "wikitest";

    // Setup servers and segments
    final List<DruidServer> historicals = createHistoricalTier(Tier.T1, 2, 10_000);
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(datasource)
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .andPartitionsPerInterval(10)
                          .eachOfSizeMb(500);

    // Put all the segments on historicalA
    final DruidServer historicalA = historicals.get(0);
    final DruidServer historicalB = historicals.get(1);
    segments.forEach(historicalA::addDataSegment);

    // Build and run the simulation
    final CoordinatorSimulation sim =
        CoordinatorSimulationImpl.builder()
                                 .balancer(new CostBalancerStrategyFactory())
                                 .segments(segments)
                                 .servers(historicals)
                                 .rulesForDatasource(datasource, Rules.T1_X1)
                                 .build();

    startSimulation(sim);
    runCycle();

    // Verify that segments have been chosen for balancing
    verifyLatestMetricValue("segment/moved/count", 5L);

    loadQueuedSegments();
    if (syncInventory) {
      syncInventoryView();
    }

    // Verify that segments have now been balanced out
    final DruidServer historicalViewA = getInventoryView(historicalA.getName());
    final DruidServer historicalViewB = getInventoryView(historicalB.getName());
    Assert.assertEquals(5, historicalViewA.getTotalSegments());
    Assert.assertEquals(5, historicalViewB.getTotalSegments());
    verifyDatasourceIsFullyLoaded(datasource);
  }

  @Test
  public void testBalancingSkipsOvershadowedSegments()
  {

  }

  @Test
  public void testBalancingOfFullyReplicatedSegment()
  {

  }

}
