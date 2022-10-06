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
import org.junit.Test;

import java.util.List;

/**
 * Coordinator simulation test to verify behaviour of segment loading.
 */
public class SegmentLoadingTest extends CoordinatorSimulationBaseTest
{
  private DruidServer historicalT11;
  private DruidServer historicalT12;
  private DruidServer historicalT21;
  private DruidServer historicalT22;

  private final String datasource = DS.WIKI;
  private final List<DataSegment> segments = Segments.WIKI_10X1D;

  @Override
  public void setUp()
  {
    // Setup historicals for 2 tiers, size 10 GB each
    historicalT11 = createHistorical(1, Tier.T1, 10_000);
    historicalT12 = createHistorical(2, Tier.T1, 10_000);

    historicalT21 = createHistorical(1, Tier.T2, 10_000);
    historicalT22 = createHistorical(2, Tier.T2, 10_000);
  }

  @Test
  public void testSecondReplicaOnAnyTierIsThrottled()
  {
    // Disable balancing, infinite load queue size, replicateThrottleLimit = 2
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 2);

    // historicals = 2(in T1)
    // replicas = 2(on T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .withDynamicConfig(dynamicConfig)
                             .build();

    // Put the first replica of all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that that replicationThrottleLimit is honored
    verifyValue(Metric.ASSIGNED_COUNT, 2L);

    loadQueuedSegments();
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(2, historicalT12.getTotalSegments());
  }

  @Test
  public void testLoadingDoesNotOverassignHistorical()
  {
    // historicals = 1(in T1), size 1 GB
    final DruidServer historicalT11 = createHistorical(1, Tier.T1, 1000);

    // disable balancing, unlimited load queue, replicationThrottleLimit = 10
    CoordinatorDynamicConfig dynamicConfig = createDynamicConfig(0, 0, 10);

    // segments = 10*1day, size 500 MB
    // strategy = cost, replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11)
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withImmediateSegmentLoading(false)
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that the number of segments assigned is within the historical capacity
    verifyValue(Metric.ASSIGNED_COUNT, 2L);
    loadQueuedSegments();
    Assert.assertEquals(2, historicalT11.getTotalSegments());
  }

  @Test
  public void testDropHappensAfterTargetReplicationOnEveryTier()
  {
    // maxNonPrimaryReplicants = 33 ensures that all target replicas (total 4)
    // are assigned for some segments in the first run itself (pigeon-hole)
    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withMaxSegmentsToMove(0)
                                .withReplicationThrottleLimit(10)
                                .withMaxNonPrimaryReplicantsToLoad(33)
                                .build();

    // historicals = 1(in T1) + 2(in T2) + 2(in T3)
    // segments = 10 * 1day, replicas = 2(T2) + 2(T3)
    final DruidServer historicalT31 = createHistorical(1, Tier.T3, 10_000);
    final DruidServer historicalT32 = createHistorical(2, Tier.T3, 10_000);
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withDynamicConfig(dynamicConfig)
                             .withRules(datasource, Load.on(Tier.T2, 2).andOn(Tier.T3, 2).forever())
                             .withServers(
                                 historicalT11,
                                 historicalT21,
                                 historicalT22,
                                 historicalT31,
                                 historicalT32
                             )
                             .build();

    // At the start, T1 has all the segments
    segments.forEach(historicalT11::addDataSegment);

    // Run 1: Nothing is dropped from T1 but things are assigned to T2 and T3
    startSimulation(sim);
    runCoordinatorCycle();

    verifyNoEvent(Metric.DROPPED_COUNT);
    int totalAssignedInRun1
        = getValue(Metric.ASSIGNED_COUNT, filter(DruidMetrics.TIER, Tier.T2)).intValue()
          + getValue(Metric.ASSIGNED_COUNT, filter(DruidMetrics.TIER, Tier.T3)).intValue();
    Assert.assertTrue(totalAssignedInRun1 > 0 && totalAssignedInRun1 < 40);

    // Run 2: Segments still queued, nothing is dropped from T1
    runCoordinatorCycle();
    loadQueuedSegments();

    verifyNoEvent(Metric.DROPPED_COUNT);
    int totalLoadedAfterRun2
        = historicalT21.getTotalSegments() + historicalT22.getTotalSegments()
          + historicalT31.getTotalSegments() + historicalT32.getTotalSegments();
    Assert.assertEquals(totalAssignedInRun1, totalLoadedAfterRun2);

    // Run 3: Some segments have been loaded
    // segments fully replicated on T2 and T3 will now be dropped from T1
    runCoordinatorCycle();
    loadQueuedSegments();

    int totalDroppedInRun3
        = getValue(Metric.DROPPED_COUNT, filter(DruidMetrics.TIER, Tier.T1)).intValue();
    Assert.assertTrue(totalDroppedInRun3 > 0 && totalDroppedInRun3 < 10);
    int totalLoadedAfterRun3
        = historicalT21.getTotalSegments() + historicalT22.getTotalSegments()
          + historicalT31.getTotalSegments() + historicalT32.getTotalSegments();
    Assert.assertEquals(40, totalLoadedAfterRun3);

    // Run 4: All segments are fully replicated on T2 and T3
    runCoordinatorCycle();
    loadQueuedSegments();

    int totalDroppedInRun4
        = getValue(Metric.DROPPED_COUNT, filter(DruidMetrics.TIER, Tier.T1)).intValue();

    Assert.assertEquals(10, totalDroppedInRun3 + totalDroppedInRun4);
    Assert.assertEquals(0, historicalT11.getTotalSegments());
    verifyDatasourceIsFullyLoaded(datasource);
  }

}
