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
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.balancer.BalancerStrategy;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

@RunWith(Parameterized.class)
public class BalancingStrategiesTest extends CoordinatorSimulationBaseTest
{
  private static final long SIZE_1TB = 1_000_000;

  private final String strategy;
  private final List<DataSegment> segments = Segments.WIKI_10X100D;

  @Parameterized.Parameters(name = "{0}")
  public static String[] getTestParameters()
  {
    return new String[]{
        BalancerStrategy.COST,
        BalancerStrategy.CACHING_COST,
        BalancerStrategy.UNIFORM_INTERVAL
    };
  }

  public BalancingStrategiesTest(String strategy)
  {
    this.strategy = strategy;
  }

  @Override
  public void setUp()
  {

  }

  @Test
  public void testFreshClusterGetsBalanced()
  {
    final List<DruidServer> historicals = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      historicals.add(createHistorical(i, Tier.T1, SIZE_1TB));
    }

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withDynamicConfig(createDynamicConfig(1000, 0, 100))
                             .withBalancer(strategy)
                             .withRules(DS.WIKI, Load.on(Tier.T1, 1).forever())
                             .withServers(historicals)
                             .withSegments(segments)
                             .build();
    startSimulation(sim);

    // Run 1: all segments are assigned and loaded
    runCoordinatorCycle();
    loadQueuedSegments();
    verifyValue(Metric.ASSIGNED_COUNT, 1000L);
    verifyNotEmitted(Metric.MOVED_COUNT);

    for (DruidServer historical : historicals) {
      Assert.assertEquals(200, historical.getTotalSegments());
    }

    // Run 2: nothing is assigned, nothing is moved as servers are already balanced
    runCoordinatorCycle();
    loadQueuedSegments();
    verifyNotEmitted(Metric.ASSIGNED_COUNT);
    verifyNotEmitted(Metric.MOVED_COUNT);
  }

  @Test
  public void testClusterGetsBalancedWhenServerIsAdded()
  {
    final List<DruidServer> historicals = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      historicals.add(createHistorical(i, Tier.T1, SIZE_1TB));
    }

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withDynamicConfig(createDynamicConfig(1000, 0, 100))
                             .withBalancer(strategy)
                             .withRules(DS.WIKI, Load.on(Tier.T1, 1).forever())
                             .withServers(historicals)
                             .withSegments(segments)
                             .build();
    startSimulation(sim);

    // Run 1: all segments are assigned and loaded
    runCoordinatorCycle();
    loadQueuedSegments();
    verifyValue(Metric.ASSIGNED_COUNT, 1000L);
    verifyNotEmitted(Metric.MOVED_COUNT);

    // Verify that each server is equally loaded
    for (DruidServer historical : historicals) {
      Assert.assertEquals(250, historical.getTotalSegments());
    }

    // Add another historical
    final DruidServer newHistorical = createHistorical(4, Tier.T1, SIZE_1TB);
    addServer(newHistorical);
    historicals.add(newHistorical);

    // Run the coordinator for a few cycles
    for (int i = 0; i < 10; ++i) {
      runCoordinatorCycle();
      loadQueuedSegments();
    }

    // Verify that the segments have been balanced
    for (DruidServer historical : historicals) {
      long loadedSegments = historical.getTotalSegments();
      Assert.assertTrue(loadedSegments >= 199 && loadedSegments <= 201);
    }
  }

  @Test
  public void testClusterGetsBalancedWhenServerIsRemoved()
  {
    final List<DruidServer> historicals = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      historicals.add(createHistorical(i, Tier.T1, SIZE_1TB));
    }

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withDynamicConfig(createDynamicConfig(1000, 0, 100))
                             .withBalancer(strategy)
                             .withRules(DS.WIKI, Load.on(Tier.T1, 1).forever())
                             .withServers(historicals)
                             .withSegments(segments)
                             .build();
    startSimulation(sim);

    // Run 1: all segments are assigned and loaded
    runCoordinatorCycle();
    loadQueuedSegments();
    verifyValue(Metric.ASSIGNED_COUNT, 1000L);
    verifyNotEmitted(Metric.MOVED_COUNT);

    // Verify that each server is equally loaded
    for (DruidServer historical : historicals) {
      Assert.assertEquals(200, historical.getTotalSegments());
    }

    // Remove a historical
    final DruidServer removedHistorical = historicals.remove(4);
    removeServer(removedHistorical);

    // Run 2: the missing segments are assigned to the other servers
    runCoordinatorCycle();
    loadQueuedSegments();
    int assignedCount = getValue(Metric.ASSIGNED_COUNT, null).intValue();
    Assert.assertTrue(assignedCount >= 200);

    for (DruidServer historical : historicals) {
      Assert.assertEquals(250, historical.getTotalSegments());
    }
  }

  @Test
  public void testClusterGetsBalancedWithOneSegmentPerInterval()
  {
    Assume.assumeTrue("uniformInterval".equals(strategy));

    final List<DruidServer> historicals = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      historicals.add(createHistorical(i, Tier.T1, SIZE_1TB));
    }

    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(100, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .withNumPartitions(1)
                          .eachOfSizeInMb(500);

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withDynamicConfig(createDynamicConfig(1000, 0, 100))
                             .withBalancer(strategy)
                             .withRules(DS.WIKI, Load.on(Tier.T1, 1).forever())
                             .withServers(historicals)
                             .withSegments(segments)
                             .build();
    startSimulation(sim);

    // All segments are loaded on histT11
    final DruidServer historicalT11 = historicals.get(0);
    segments.forEach(historicalT11::addDataSegment);

    // Run 1: Some segments are moved to historicalT12
    runCoordinatorCycle();
    verifyNotEmitted(Metric.ASSIGNED_COUNT);
    verifyValue(Metric.MOVED_COUNT, 80L);

    loadQueuedSegments();
    for (DruidServer historical : historicals) {
      Assert.assertEquals(20, historical.getTotalSegments());
    }

    // Run 2: nothing is assigned, nothing is moved as servers are already balanced
    runCoordinatorCycle();
    verifyNotEmitted(Metric.ASSIGNED_COUNT);
    verifyNotEmitted(Metric.MOVED_COUNT);
  }

  @Test
  public void testClusterWithMultipleDatasourcesGetsBalanced()
  {
    Assume.assumeTrue("uniformInterval".equals(strategy));

    final List<DruidServer> historicals = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      historicals.add(createHistorical(i, Tier.T1, SIZE_1TB));
    }

    final Random random = new Random(1414);

    final List<DataSegment> segments = new ArrayList<>();
    final List<Granularity> granularities = Arrays.asList(
        Granularities.HOUR,
        Granularities.EIGHT_HOUR,
        Granularities.DAY,
        Granularities.MONTH,
        Granularities.YEAR,
        Granularities.ALL
    );

    // Create multiple datasources with diferent intervals, granularities and sizes
    for (int i = 0; i < 10; ++i) {
      final String datasource = "ds_" + i;

      Granularity granularity = granularities.get(random.nextInt(granularities.size()));
      int year = 2010 + random.nextInt(14);
      List<DataSegment> datasourceSegments =
          CreateDataSegments.ofDatasource(datasource)
                            .forIntervals(10, granularity)
                            .startingAt(year + "-01-01")
                            .withNumPartitions(random.nextInt(500))
                            .eachOfSizeInMb(random.nextInt(1000));

      segments.addAll(datasourceSegments);
    }

    int totalSegments = segments.size();

    // Use the strategy itself to do assignments, not round robin
    CoordinatorDynamicConfig dynamicConfig =
        CoordinatorDynamicConfig.builder()
                                .withMaxSegmentsToMove(totalSegments)
                                .withMaxSegmentsInNodeLoadingQueue(0)
                                .withReplicationThrottleLimit(totalSegments)
                                .withUseRoundRobinSegmentAssignment(false)
                                .withEmitBalancingStats(true)
                                .build();

    CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withDynamicConfig(dynamicConfig)
                             .withBalancer(strategy)
                             .withServers(historicals)
                             .withSegments(segments)
                             .withRules("_default", Load.on(Tier.T1, 2).forever())
                             .build();
    startSimulation(sim);

    // Run 1: All segments are assigned, some are moved
    runCoordinatorCycle();
    loadQueuedSegments();

    for (DruidServer historical : historicals) {
      double usedPercentage = (100.0f * historical.getCurrSize() / historical.getMaxSize());
      Assert.assertTrue(usedPercentage >= 76.0 && usedPercentage <= 77.0);
    }

    // Verify that no movements happen in later runs
    for (int i = 0; i < 3; ++i) {
      runCoordinatorCycle();
      verifyNotEmitted(Metric.ASSIGNED_COUNT);
      verifyNotEmitted(Metric.MOVED_COUNT);
    }
  }

}
