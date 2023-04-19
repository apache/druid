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
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

@RunWith(Parameterized.class)
public class BalancingStrategiesTest extends CoordinatorSimulationBaseTest
{
  private static final long SIZE_1TB = 1_000_000;

  private final String strategy;
  private final List<DataSegment> segments = Segments.WIKI_10X100D;

  @Parameterized.Parameters(name = "{0}")
  public static String[] getTestParameters()
  {
    return new String[]{"cost", "cachingCost", "uniformInterval"};
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

}
