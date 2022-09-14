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
import org.apache.druid.java.util.emitter.core.Event;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Base test for coordinator simulations.
 * <p>
 * Each test must call {@link #startSimulation(CoordinatorSimulation)} to start
 * the simulation. {@link CoordinatorSimulation#stop()} should not be called as
 * the simulation is stopped when cleaning up after the test in {@link #tearDown()}.
 * <p>
 * Tests that verify balancing behaviour should set
 * {@link CoordinatorDynamicConfig#useBatchedSegmentSampler()} to true.
 * Otherwise, the segment sampling is random and can produce repeated values
 * leading to flakiness in the tests. The simulation sets this field to true by
 * default.
 */
public abstract class CoordinatorSimulationBaseTest
    implements CoordinatorSimulation.CoordinatorState, CoordinatorSimulation.ClusterState
{
  static final double DOUBLE_DELTA = 10e-9;

  private CoordinatorSimulation sim;

  @Before
  public abstract void setUp();

  @After
  public void tearDown()
  {
    if (sim != null) {
      sim.stop();
      sim = null;
    }
  }

  void startSimulation(CoordinatorSimulation simulation)
  {
    this.sim = simulation;
    simulation.start();
  }

  @Override
  public void runCoordinatorCycle()
  {
    sim.coordinator().runCoordinatorCycle();
  }

  @Override
  public List<Event> getMetricEvents()
  {
    return sim.coordinator().getMetricEvents();
  }

  @Override
  public DruidServer getInventoryView(String serverName)
  {
    return sim.coordinator().getInventoryView(serverName);
  }

  @Override
  public void syncInventoryView()
  {
    sim.coordinator().syncInventoryView();
  }

  @Override
  public void loadQueuedSegments()
  {
    sim.cluster().loadQueuedSegments();
  }

  @Override
  public double getLoadPercentage(String datasource)
  {
    return sim.coordinator().getLoadPercentage(datasource);
  }

  // Verification methods
  void verifyDatasourceIsFullyLoaded(String datasource)
  {
    Assert.assertEquals(100.0, getLoadPercentage(datasource), DOUBLE_DELTA);
  }

  void verifyLatestMetricValue(String metricName, Number expectedValue)
  {
    final List<Number> observedValues = getMetricValues(metricName);
    Number latestValue = observedValues.get(observedValues.size() - 1);
    Assert.assertEquals(expectedValue, latestValue);
  }

  private List<Number> getMetricValues(String metricName)
  {
    final List<Number> metricValues = new ArrayList<>();

    for (Event event : sim.coordinator().getMetricEvents()) {
      final Map<String, Object> map = event.toMap();
      final String eventMetricName = (String) map.get("metric");
      if (eventMetricName != null && eventMetricName.equals(metricName)) {
        metricValues.add((Number) map.get("value"));
      }
    }

    return metricValues;
  }

  // Utility methods
  static List<DataSegment> createWikiSegmentsForIntervals(
      int numIntervals,
      Granularity granularity,
      int partitionsPerInterval
  )
  {
    return CreateDataSegments.ofDatasource(DS.WIKI)
                             .forIntervals(1, Granularities.DAY)
                             .startingAt("2022-01-01")
                             .andPartitionsPerInterval(10)
                             .eachOfSizeMb(500);
  }

  static CoordinatorDynamicConfig createDynamicConfig(
      int maxSegmentsToMove,
      int maxSegmentsInNodeLoadingQueue,
      int replicationThrottleLimit
  )
  {
    return CoordinatorDynamicConfig.builder()
                                   .withMaxSegmentsToMove(maxSegmentsToMove)
                                   .withReplicationThrottleLimit(replicationThrottleLimit)
                                   .withMaxSegmentsInNodeLoadingQueue(maxSegmentsInNodeLoadingQueue)
                                   .withUseBatchedSegmentSampler(true)
                                   .build();
  }

  /**
   * Creates a tier of historicals.
   */
  static List<DruidServer> createHistoricalTier(String tier, int numServers, long serverSizeMb)
  {
    List<DruidServer> servers = new ArrayList<>();
    for (int i = 0; i < numServers; ++i) {
      servers.add(createHistorical(i, tier, serverSizeMb));
    }
    return servers;
  }

  /**
   * Creates a historical.
   */
  static DruidServer createHistorical(int uniqueIdInTier, String tier, long serverSizeMb)
  {
    final String name = tier + "__" + "hist__" + uniqueIdInTier;
    return new DruidServer(name, name, name, serverSizeMb, ServerType.HISTORICAL, tier, 1);
  }

  // Utility and constant holder classes

  static class DS
  {
    static final String WIKI = "wiki";
  }

  static class Tier
  {
    static final String T1 = "tier_t1";
    static final String T2 = "tier_t2";
  }

  /**
   * Builder for retention rules.
   *
   * @see Load
   */
  interface RuleBuilder
  {

  }

  /**
   * Builder for a load rule.
   */
  static class Load implements RuleBuilder
  {
    private final Map<String, Integer> tieredReplicants = new HashMap<>();

    static Load on(String tier, int numReplicas)
    {
      Load load = new Load();
      load.tieredReplicants.put(tier, numReplicas);
      return load;
    }

    Load andOn(String tier, int numReplicas)
    {
      tieredReplicants.put(tier, numReplicas);
      return this;
    }

    Rule forever()
    {
      return new ForeverLoadRule(tieredReplicants);
    }
  }
}
