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
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;

/**
 * Coordinator simulation test to verify behaviour of segment balancing.
 */
public class SegmentBalancingTest extends CoordinatorSimulationBaseTest
{
  // TODO: randomness and flakiness can creep in if the maxSegmentsToMove < totalLoadedSegments
  @Override
  public void setUp()
  {

  }

  @Test
  public void testBalancingWithUpdatedInventory()
  {
    testBalancingWhenInventoryIsSynced(true);
  }

  @Test
  @Ignore("Fix #12881 to enable this test. "
          + "Current impl requires updated inventory for correct callback behaviour.")
  public void testBalancingWithStaleInventory()
  {
    testBalancingWhenInventoryIsSynced(false);
  }

  private void testBalancingWhenInventoryIsSynced(boolean syncInventory)
  {
    // historicals = 2(T1), segments = 10(1 day)
    final DruidServer historicalT11 = createHistorical(1, Tier.T1, 10_000);
    final DruidServer historicalT12 = createHistorical(2, Tier.T1, 10_000);
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .andPartitionsPerInterval(10)
                          .eachOfSizeMb(500);


    // strategy = cost, replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .segments(segments)
                             .servers(historicalT11, historicalT12)
                             .rules(DS.WIKI, Load.on(Tier.T1, 1).forever())
                             .balancer(new CostBalancerStrategyFactory())
                             .build();

    // Put all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that segments have been chosen for balancing
    verifyLatestMetricValue("segment/moved/count", 5L);

    loadQueuedSegments();
    if (syncInventory) {
      syncInventoryView();
    }

    // Verify that segments have now been balanced out
    final DruidServer historicalViewT11 = getInventoryView(historicalT11.getName());
    final DruidServer historicalViewT12 = getInventoryView(historicalT12.getName());
    Assert.assertEquals(5, historicalViewT11.getTotalSegments());
    Assert.assertEquals(5, historicalViewT12.getTotalSegments());
    verifyDatasourceIsFullyLoaded(DS.WIKI);
  }

  @Test
  public void testBalancingSkipsUnusedSegments()
  {
    // have a bunch of segments
    // mark them all as unused
    // verify that nothing is chosen for balancing
  }

  @Test
  public void testBalancingSkipsOvershadowedSegments()
  {
    // have a bunch of segments
    // overshadow all but one
    // verify that none of the overshadowed ones are chosen for balancing
  }

  @Test
  public void testBalancingOfFullyReplicatedSegment()
  {
    // a couple of segments
    // all of them are fully loaded
    // something will be chosen for balancing
    // verify that the segments on the move don't count towards over-replication
    // this will require 2 coordinator runs
  }

}
