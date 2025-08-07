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
import org.apache.druid.server.coordinator.stats.Stats;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class HistoricalCloningTest extends CoordinatorSimulationBaseTest
{
  private static final long SIZE_1TB = 1_000_000;

  private DruidServer historicalT11;
  private DruidServer historicalT12;
  private DruidServer historicalT13;

  private final String datasource = TestDataSource.WIKI;

  @Override
  public void setUp()
  {
    // Setup historicals for 1 tier, size 1 TB each
    historicalT11 = createHistorical(1, Tier.T1, SIZE_1TB);
    historicalT12 = createHistorical(2, Tier.T1, SIZE_1TB);
    historicalT13 = createHistorical(3, Tier.T1, SIZE_1TB);
  }

  @Test
  public void testSimpleCloning()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.WIKI_10X1D)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withDynamicConfig(
                                 CoordinatorDynamicConfig.builder()
                                                         .withCloneServers(Map.of(historicalT12.getHost(), historicalT11.getHost()))
                                                         .withSmartSegmentLoading(true)
                                                         .build()
                             )
                             .withImmediateSegmentLoading(true)
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();

    verifyValue(Metric.ASSIGNED_COUNT, 10L);
    verifyValue(
        Stats.Segments.ASSIGNED_TO_CLONE.getMetricName(),
        Map.of("server", historicalT12.getName()),
        10L
    );
    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT11.getName(), "description", "LOAD: NORMAL"),
        10L
    );
    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT12.getName(), "description", "LOAD: NORMAL"),
        10L
    );

    Assert.assertEquals(Segments.WIKI_10X1D.size(), historicalT11.getTotalSegments());
    Assert.assertEquals(Segments.WIKI_10X1D.size(), historicalT12.getTotalSegments());
    Segments.WIKI_10X1D.forEach(segment -> {
      Assert.assertEquals(segment, historicalT11.getSegment(segment.getId()));
      Assert.assertEquals(segment, historicalT12.getSegment(segment.getId()));
    });
  }

  @Test
  public void testAddingNewHistorical()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.WIKI_10X1D)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withDynamicConfig(
                                 CoordinatorDynamicConfig.builder()
                                                         .withCloneServers(Map.of(historicalT12.getHost(), historicalT11.getHost()))
                                                         .withSmartSegmentLoading(true)
                                                         .build()
                             )
                             .withImmediateSegmentLoading(true)
                             .build();

    // Run 1: Current state is a historical and clone already in sync.
    Segments.WIKI_10X1D.forEach(segment -> {
      historicalT11.addDataSegment(segment);
      historicalT12.addDataSegment(segment);
    });

    startSimulation(sim);

    runCoordinatorCycle();

    // Confirm number of segments.
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());

    // Add a new historical.
    final DruidServer newHistorical = createHistorical(3, Tier.T1, 10_000);
    addServer(newHistorical);

    // Run 2: Let the coordinator balance segments.
    runCoordinatorCycle();

    // Check that segments have been distributed to the new historical and have also been dropped by the clone
    Assert.assertEquals(5, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
    Assert.assertEquals(5, newHistorical.getTotalSegments());
    verifyValue(
        Stats.Segments.DROPPED_FROM_CLONE.getMetricName(),
        Map.of("server", historicalT12.getName()),
        5L
    );
  }

  @Test
  public void testCloningServerDisappearsAndRelaunched()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.WIKI_10X1D)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .withDynamicConfig(
                                 CoordinatorDynamicConfig.builder()
                                                         .withCloneServers(Map.of(historicalT12.getHost(), historicalT11.getHost()))
                                                         .withSmartSegmentLoading(true)
                                                         .build()
                             )
                             .withImmediateSegmentLoading(true)
                             .build();

    startSimulation(sim);

    // Run 1: All segments are loaded.
    runCoordinatorCycle();
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());

    // Target server disappears, loses loaded segments.
    removeServer(historicalT12);
    Segments.WIKI_10X1D.forEach(segment -> historicalT12.removeDataSegment(segment.getId()));

    // Run 2: No change in source historical.
    runCoordinatorCycle();

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(0, historicalT12.getTotalSegments());

    // Server readded
    addServer(historicalT12);

    // Run 3: Segments recloned.
    runCoordinatorCycle();

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());
    verifyValue(
        Stats.Segments.ASSIGNED_TO_CLONE.getMetricName(),
        Map.of("server", historicalT12.getName()),
        10L
    );
    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT12.getName(), "description", "LOAD: NORMAL"),
        10L
    );

    Assert.assertEquals(Segments.WIKI_10X1D.size(), historicalT11.getTotalSegments());
    Assert.assertEquals(Segments.WIKI_10X1D.size(), historicalT12.getTotalSegments());
    Segments.WIKI_10X1D.forEach(segment -> {
      Assert.assertEquals(segment, historicalT11.getSegment(segment.getId()));
      Assert.assertEquals(segment, historicalT12.getSegment(segment.getId()));
    });
  }

  @Test
  public void testClonedServerDoesNotFollowReplicationLimit()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.WIKI_10X100D)
                             .withServers(historicalT11)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withDynamicConfig(
                                 CoordinatorDynamicConfig.builder()
                                                         .withCloneServers(Map.of(historicalT12.getHost(), historicalT11.getHost()))
                                                         .withSmartSegmentLoading(true)
                                                         .withReplicationThrottleLimit(2)
                                                         .build()
                             )
                             .withImmediateSegmentLoading(true)
                             .build();

    Segments.WIKI_10X100D.forEach(segment -> historicalT11.addDataSegment(segment));
    startSimulation(sim);

    // Run 1: All segments are loaded on the source historical
    runCoordinatorCycle();
    Assert.assertEquals(1000, historicalT11.getTotalSegments());
    Assert.assertEquals(0, historicalT12.getTotalSegments());

    // Clone server now added.
    addServer(historicalT12);

    // Run 2: Assigns all segments to the cloned historical
    runCoordinatorCycle();

    Assert.assertEquals(1000, historicalT11.getTotalSegments());
    Assert.assertEquals(1000, historicalT12.getTotalSegments());

    verifyValue(
        Stats.Segments.ASSIGNED_TO_CLONE.getMetricName(),
        Map.of("server", historicalT12.getName()),
        1000L
    );

    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT12.getName(), "description", "LOAD: NORMAL"),
        1000L
    );
  }

  @Test
  public void testCloningHistoricalWithReplicationLimit()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.WIKI_10X1D)
                             .withServers(historicalT11, historicalT12, historicalT13)
                             .withRules(datasource, Load.on(Tier.T1, 2).forever())
                             .withImmediateSegmentLoading(true)
                             .withDynamicConfig(
                                 CoordinatorDynamicConfig.builder()
                                                         .withCloneServers(Map.of(historicalT12.getHost(), historicalT11.getHost()))
                                                         .withSmartSegmentLoading(false)
                                                         .withReplicationThrottleLimit(2)
                                                         .withMaxSegmentsToMove(0)
                                                         .build()
                             )
                             .withImmediateSegmentLoading(true)
                             .build();
    Segments.WIKI_10X1D.forEach(historicalT13::addDataSegment);
    startSimulation(sim);

    // Check that only replication count segments are loaded each run and that the cloning server copies it.
    while (historicalT11.getTotalSegments() < Segments.WIKI_10X1D.size()) {
      runCoordinatorCycle();

      // Check that all segments are cloned.
      Assert.assertEquals(historicalT11.getTotalSegments(), historicalT12.getTotalSegments());

      // Check that the replication throttling is respected.
      verifyValue(Metric.ASSIGNED_COUNT, 2L);
      verifyValue(
          Stats.Segments.ASSIGNED_TO_CLONE.getMetricName(),
          Map.of("server", historicalT12.getName()),
          2L
      );
    }

    Assert.assertEquals(Segments.WIKI_10X1D.size(), historicalT11.getTotalSegments());
    Assert.assertEquals(Segments.WIKI_10X1D.size(), historicalT12.getTotalSegments());
    Assert.assertEquals(Segments.WIKI_10X1D.size(), historicalT13.getTotalSegments());
    Segments.WIKI_10X1D.forEach(segment -> {
      Assert.assertEquals(segment, historicalT11.getSegment(segment.getId()));
      Assert.assertEquals(segment, historicalT12.getSegment(segment.getId()));
      Assert.assertEquals(segment, historicalT13.getSegment(segment.getId()));
    });
  }

  @Test
  public void test_loadsAreCancelledOnClone_ifSegmentsAreRemovedFromSource()
  {
    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withSegments(Segments.WIKI_10X1D)
                             .withServers(historicalT11, historicalT12)
                             .withRules(datasource, Load.on(Tier.T1, 1).forever())
                             .withDynamicConfig(
                                 CoordinatorDynamicConfig.builder()
                                                         .withCloneServers(Map.of(historicalT12.getHost(), historicalT11.getHost()))
                                                         .withSmartSegmentLoading(true)
                                                         .build()
                             )
                             .build();


    // Load all segments on histT11
    Segments.WIKI_10X1D.forEach(historicalT11::addDataSegment);

    startSimulation(sim);
    runCoordinatorCycle();

    verifyValue(
        Stats.Segments.ASSIGNED_TO_CLONE.getMetricName(),
        Map.of("server", historicalT12.getName()),
        10L
    );

    // Remove some segments from histT11
    deleteSegments(Segments.WIKI_10X1D.subList(0, 5));

    // Verify that the loads are cancelled from the clone
    runCoordinatorCycle();
    verifyValue(
        Metric.CANCELLED_ACTIONS,
        Map.of("server", historicalT12.getName()),
        5L
    );

    loadQueuedSegments();
    Assert.assertEquals(5, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
  }
}
