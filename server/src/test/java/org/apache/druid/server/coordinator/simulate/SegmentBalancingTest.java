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
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Coordinator simulation test to verify behaviour of segment balancing.
 */
public class SegmentBalancingTest extends CoordinatorSimulationBaseTest
{
  private DruidServer historicalT11;
  private DruidServer historicalT12;

  private final String datasource = TestDataSource.WIKI;
  private final List<DataSegment> segments = Segments.WIKI_10X1D;

  @Override
  public void setUp()
  {
    // Setup historicals for 2 tiers, size 10 GB each
    historicalT11 = createHistorical(1, Tier.T1, 10_000);
    historicalT12 = createHistorical(2, Tier.T1, 10_000);
  }

  @Test
  public void testBalancingDoesNotOverReplicate()
  {
    testBalancingWithAutoSyncInventory(true);
  }

  @Test
  public void testBalancingWithStaleViewDoesNotOverReplicate()
  {
    testBalancingWithAutoSyncInventory(false);
  }

  private void testBalancingWithAutoSyncInventory(boolean autoSyncInventory)
  {
    // historicals = 2(T1), replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withAutoInventorySync(autoSyncInventory)
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
  public void testBalancingDoesNotUnderReplicateSegment()
  {
    // historicals = 2(T1), replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .build();

    // Put all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    // Run cycle and verify that segments have been chosen for balancing
    startSimulation(sim);
    runCoordinatorCycle();
    verifyValue(Metric.MOVED_COUNT, 5L);

    // Load segments, skip callbacks and verify that some segments are now loaded on histT12
    loadQueuedSegmentsSkipCallbacks();
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());

    // Run another coordinator cycle
    runCoordinatorCycle();
    loadQueuedSegmentsSkipCallbacks();

    // Verify that segments have not been dropped from either server since
    // MOVE_FROM operation is still not complete
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
    verifyNotEmitted(Metric.DROPPED_COUNT);
    verifyNotEmitted(Metric.MOVED_COUNT);

    // Finish the move operations
    loadQueuedSegments();
    Assert.assertEquals(5, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
  }

  @Test
  public void testDropDoesNotHappenWhenLoadFails()
  {
    // historicals = 2(T1), replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .build();

    // Put all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();
    verifyValue(Metric.MOVED_COUNT, 5L);

    // Remove histT12 from cluster so that the move fails
    removeServer(historicalT12);
    loadQueuedSegments();

    // Verify that no segment has been loaded or dropped
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(0, historicalT12.getTotalSegments());
    verifyDatasourceIsFullyLoaded(datasource);
  }

  @Test
  public void testBalancingOfFullyReplicatedSegment()
  {
    // historicals = 2(in T1), replicas = 1(T1)
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .build();

    // Put all the segments on histT11
    segments.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    // Verify that there are segments in the load queue for balancing
    verifyValue(Metric.MOVED_COUNT, 5L);
    verifyValue(Metric.LOAD_QUEUE_COUNT, filterByServer(historicalT12), 5L);

    runCoordinatorCycle();

    // Verify that the segments in the load queue are not considered as over-replicated
    verifyNotEmitted(Metric.DROPPED_COUNT);
    verifyValue(Metric.LOAD_QUEUE_COUNT, filterByServer(historicalT12), 5L);

    // Finish and verify balancing
    loadQueuedSegments();
    Assert.assertEquals(5, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
    verifyDatasourceIsFullyLoaded(datasource);
  }

  @Test
  public void testBalancingMovesSegmentsInLoadQueue()
  {
    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(segments)
                             .withServers(historicalT11)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    // Run 1: All segments are assigned to the first historical
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 10L);
    verifyValue(Metric.LOAD_QUEUE_COUNT, filterByServer(historicalT11), 10L);

    // Run 2: Add new historical, some segments in the queue will be moved
    addServer(historicalT12);
    runCoordinatorCycle();
    verifyNotEmitted(Metric.ASSIGNED_COUNT);
    verifyValue(Metric.CANCELLED_ACTIONS, 5L);
    verifyValue(Metric.MOVED_COUNT, 5L);

    verifyValue(Metric.LOAD_QUEUE_COUNT, filterByServer(historicalT11), 5L);
    verifyValue(Metric.LOAD_QUEUE_COUNT, filterByServer(historicalT12), 5L);

    // Complete loading the segments
    loadQueuedSegments();
    Assert.assertEquals(5, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
  }

  @Test
  public void testBalancingDoesNotMoveLoadedSegmentsWhenTierIsBusy()
  {
    // maxSegmentsToMove = 3, unlimited load queue
    final CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withSmartSegmentLoading(false)
                                .withMaxSegmentsToMove(3)
                                .withMaxSegmentsInNodeLoadingQueue(0)
                                .build();

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withDynamicConfig(dynamicConfig)
                             .withSegments(segments)
                             .withServers(historicalT11)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    // Pre-load some of the segments on histT11
    segments.subList(2, segments.size()).forEach(historicalT11::addDataSegment);

    // Run 1: The remaining segments are assigned to histT11
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 2L);

    // Run 2: Add histT12, some loading segments and some loaded segments are moved to it
    addServer(historicalT12);
    runCoordinatorCycle();
    verifyValue(Metric.MOVED_COUNT, 3L);
    verifyValue(Metric.CANCELLED_ACTIONS, 2L);
    verifyValue(Metric.LOAD_QUEUE_COUNT, filterByServer(historicalT12), 3L);

    // Run 3: No more segments are moved as tier is already busy moving
    runCoordinatorCycle();
    verifyNotEmitted(Metric.MOVED_COUNT);

    // Run 4: Load pending segments, more are moved
    loadQueuedSegments();
    runCoordinatorCycle();
    Assert.assertTrue(getValue(Metric.MOVED_COUNT, null).intValue() > 0);
  }

  @Test(timeout = 60000L)
  public void testMaxSegmentsAreMovedWhenClusterIsSkewed()
  {
    // Add 10 historicals of size 1 TB each
    final long size1TB = 1_000_000;
    List<DruidServer> historicals
        = IntStream.range(0, 10)
                   .mapToObj(i -> createHistorical(i + 1, Tier.T1, size1TB))
                   .collect(Collectors.toList());

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.KOALA_100X100D)
                             .withServers(historicals)
                             .withRules(TestDataSource.KOALA, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    // Run 1: All segments are assigned to the 10 historicals
    runCoordinatorCycle();
    verifyValue(Metric.ASSIGNED_COUNT, 10_000L);
    verifyNotEmitted(Metric.MOVED_COUNT);
    verifyValue(Metric.MOVE_SKIPPED, 100L);

    // Run 2: Add 10 more historicals, some segments are moved to them
    for (int i = 11; i <= 20; ++i) {
      addServer(createHistorical(i, Tier.T1, size1TB));
    }

    runCoordinatorCycle();
    verifyValue(Metric.MOVED_COUNT, 500L);
    verifyNotEmitted(Metric.MOVE_SKIPPED);
  }

}
