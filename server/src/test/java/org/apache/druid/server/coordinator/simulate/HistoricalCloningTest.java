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
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
    // Setup historicals for 2 tiers, size 10 GB each
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
                                 withCloneServers(
                                     Map.of(
                                         historicalT11.getHost(), historicalT12.getHost()
                                     )
                                 ).build()
                             )
                             .build();

    startSimulation(sim);
    runCoordinatorCycle();
    loadQueuedSegments();

    verifyValue(Metric.ASSIGNED_COUNT, 10L);
    verifyValue(
        Stats.CoordinatorRun.CLONE_LOAD.getMetricName(),
        Map.of("server", historicalT12.getName()),
        10L
    );

    runCoordinatorCycle();
    loadQueuedSegments();

    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT11.getName(), "description", "LOAD: NORMAL"),
        10L
    );
    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT12.getName(), "description", "LOAD: TURBO"),
        10L
    );

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());
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
                                 withCloneServers(
                                     Map.of(
                                         historicalT11.getHost(), historicalT12.getHost()
                                     )
                                 ).build()
                             )
                             .build();

    // Run 1: Current state is a historical and clone already in sync.
    Segments.WIKI_10X1D.forEach(segment -> {
      historicalT11.addDataSegment(segment);
      historicalT12.addDataSegment(segment);
    });

    startSimulation(sim);

    runCoordinatorCycle();
    loadQueuedSegments();

    // Confirm number of segments.
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());

    // Add a new historical.
    final DruidServer newHistorical = createHistorical(3, Tier.T1, 10_000);
    addServer(newHistorical);

    // Run 2: Let the coordinator balance segments.
    runCoordinatorCycle();
    loadQueuedSegments();

    // Check that segments have been distributed to the new historical and have also been dropped by the clone
    Assert.assertEquals(5, historicalT11.getTotalSegments());
    Assert.assertEquals(5, historicalT12.getTotalSegments());
    Assert.assertEquals(5, newHistorical.getTotalSegments());
    verifyValue(
        Stats.CoordinatorRun.CLONE_DROP.getMetricName(),
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
                                 withCloneServers(
                                     Map.of(
                                         historicalT11.getHost(), historicalT12.getHost()
                                     )
                                 ).build()
                             )
                             .build();

    startSimulation(sim);

    // Run 1: All segments are loaded.
    runCoordinatorCycle();
    loadQueuedSegments();
    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());

    // Target server disappears, loses loaded segments.
    removeServer(historicalT12);
    Segments.WIKI_10X1D.forEach(segment -> historicalT12.removeDataSegment(segment.getId()));

    // Run 2: No change in source historical.
    runCoordinatorCycle();
    loadQueuedSegments();

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(0, historicalT12.getTotalSegments());

    // Server readded
    addServer(historicalT12);

    // Run 3: Segments recloned.
    runCoordinatorCycle();
    loadQueuedSegments();

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());
    verifyValue(
        Stats.CoordinatorRun.CLONE_LOAD.getMetricName(),
        Map.of("server", historicalT12.getName()),
        10L
    );

    runCoordinatorCycle();
    loadQueuedSegments();

    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT12.getName(), "description", "LOAD: TURBO"),
        10L
    );

    Assert.assertEquals(10, historicalT11.getTotalSegments());
    Assert.assertEquals(10, historicalT12.getTotalSegments());
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
                                 withCloneServers(
                                     Map.of(
                                         historicalT11.getHost(), historicalT12.getHost()
                                     )
                                 ).withReplicationThrottleLimit(10).build()
                             )
                             .build();

    startSimulation(sim);
    Segments.WIKI_10X100D.forEach(
        segment -> historicalT11.addDataSegment(segment)
    );

    // Run 1: All segments are loaded on the source historical
    runCoordinatorCycle();
    loadQueuedSegments();
    Assert.assertEquals(1000, historicalT11.getTotalSegments());
    Assert.assertEquals(0, historicalT12.getTotalSegments());

    // Clone server now added.
    addServer(historicalT12);

    // Run 2: Assigns all segments to the cloned historical
    runCoordinatorCycle();
    loadQueuedSegments();

    Assert.assertEquals(1000, historicalT11.getTotalSegments());
    Assert.assertEquals(1000, historicalT12.getTotalSegments());

    verifyValue(
        Stats.CoordinatorRun.CLONE_LOAD.getMetricName(),
        Map.of("server", historicalT12.getName()),
        1000L
    );

    runCoordinatorCycle();
    loadQueuedSegments();

    verifyValue(
        Metric.SUCCESS_ACTIONS,
        Map.of("server", historicalT12.getName(), "description", "LOAD: TURBO"),
        1000L
    );
  }

  /**
   * Creates a dynamic config with unlimited load queue, balancing disabled and
   * the given {@code replicationThrottleLimit}.
   */
  private CoordinatorDynamicConfig.Builder withCloneServers(Map<String, String> cloneServers)
  {
    final Set<String> unmanagedServers = new HashSet<>(cloneServers.values());

    return CoordinatorDynamicConfig.builder()
                                   .withSmartSegmentLoading(true)
                                   .withCloneServers(cloneServers)
                                   .withUnmanagedNodes(unmanagedServers)
                                   .withTurboLoadingNodes(unmanagedServers);
  }
}
