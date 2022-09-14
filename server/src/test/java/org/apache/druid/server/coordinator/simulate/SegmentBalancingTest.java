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
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
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
  private DruidServer historicalT11;
  private DruidServer historicalT12;

  private final String datasource = DS.WIKI;
  private final List<DataSegment> segments = Segments.WIKI_10X1D;

  @Override
  public void setUp()
  {
    // Setup historicals for 2 tiers, size 10 GB each
    historicalT11 = createHistorical(1, Tier.T1, 10_000);
    historicalT12 = createHistorical(2, Tier.T1, 10_000);
  }

  @Test
  public void testBalancingWithSyncedInventory()
  {
    testBalancingWithInventorySynced(true);
  }

  @Test
  @Ignore("Fix #12881 to enable this test. "
          + "Current impl requires updated inventory for correct callback behaviour.")
  public void testBalancingWithUnsyncedInventory()
  {
    testBalancingWithInventorySynced(false);
  }

  private void testBalancingWithInventorySynced(boolean autoSyncInventory)
  {
    // maxSegmentsToMove = 10, unlimited load queue, replicationThrottleLimit = 10
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(10, 0, 10);

    // historicals = 2(T1), replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .segments(segments)
                             .servers(historicalT11, historicalT12)
                             .rules(datasource, Load.on(Tier.T1, 1).forever())
                             .dynamicConfig(dynamicConfig)
                             .autoSyncInventory(autoSyncInventory)
                             .build();

    // Put all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    if (!autoSyncInventory) {
      syncInventoryView();
    }

    runCoordinatorCycle();

    // Verify that segments have been chosen for balancing
    verifyValue(Metric.MOVED_COUNT, 5L);

    loadQueuedSegments();

    // Verify that segments have now been balanced out
    Assert.assertEquals(5, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
    verifyDatasourceIsFullyLoaded(datasource);
  }

  @Test
  public void testBalancingOfFullyReplicatedSegment()
  {
    // maxSegmentsToMove = 10, unlimited load queue, replicationThrottleLimit = 10
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(10, 0, 10);

    // historicals = 2(in T1), replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .segments(segments)
                             .servers(historicalT11, historicalT12)
                             .dynamicConfig(dynamicConfig)
                             .rules(datasource, Load.on(Tier.T1, 1).forever())
                             .build();

    // Put all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that there are segments in the load queue for balancing
    verifyValue(Metric.MOVED_COUNT, 5L);
    verifyValue(
        Metric.LOAD_QUEUE_COUNT,
        filter(DruidMetrics.SERVER, historicalT12.getName()),
        5
    );

    runCoordinatorCycle();

    // Verify that the segments in the load queue are not considered as over-replicated
    verifyValue("segment/dropped/count", 0L);
    verifyValue(
        Metric.LOAD_QUEUE_COUNT,
        filter(DruidMetrics.SERVER, historicalT12.getName()),
        5
    );

    // Finish and verify balancing
    loadQueuedSegments();
    Assert.assertEquals(5, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
    verifyDatasourceIsFullyLoaded(datasource);
  }
}
