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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
  public void setDynamicConfig(CoordinatorDynamicConfig dynamicConfig)
  {
    sim.coordinator().setDynamicConfig(dynamicConfig);
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

  void verifyNoMetricEvent(String metricName)
  {
    Assert.assertTrue(getMetricValues(metricName, null).isEmpty());
  }

  void verifyLatestMetricValue(String metricName, Number expectedValue)
  {
    verifyLatestMetricValue(metricName, null, expectedValue);
  }

  void verifyLatestMetricValue(String metricName, Map<String, String> dimensionFilters, Number expectedValue)
  {
    Assert.assertEquals(expectedValue, getLatestMetricValue(metricName, dimensionFilters));
  }

  Number getLatestMetricValue(String metricName)
  {
    return getLatestMetricValue(metricName, null);
  }

  Number getLatestMetricValue(String metricName, Map<String, String> dimensionFilters)
  {
    List<Number> values = getMetricValues(metricName, dimensionFilters);
    Assert.assertEquals(
        "Metric must have been emitted exactly once for the given dimensions.",
        1,
        values.size()
    );
    return values.get(0);
  }

  private List<Number> getMetricValues(String metricName, Map<String, String> dimensionFilters)
  {
    dimensionFilters = dimensionFilters == null ? new HashMap<>() : dimensionFilters;
    final List<Number> metricValues = new ArrayList<>();

    for (Event event : sim.coordinator().getMetricEvents()) {
      final Map<String, Object> eventMap = event.toMap();

      boolean match = metricName.equals(eventMap.get("metric"));
      for (String dimension : dimensionFilters.keySet()) {
        if (!match) {
          break;
        }
        match = dimensionFilters.get(dimension).equals(eventMap.get(dimension));
      }

      if (match) {
        metricValues.add((Number) eventMap.get("value"));
      }
    }

    return metricValues;
  }

  // Utility methods
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
   * Creates a map containing dimension key-values to filter out metric events.
   */
  static Map<String, String> filter(String... dimensionValues)
  {
    if (dimensionValues.length < 2 || dimensionValues.length % 2 == 1) {
      throw new IllegalArgumentException("Dimension key-values must be specified in pairs.");
    }

    final Map<String, String> filters = new HashMap<>();
    for (int i = 0; i < dimensionValues.length; ) {
      filters.put(dimensionValues[i], dimensionValues[i + 1]);
      i += 2;
    }
    return filters;
  }

  /**
   * Creates a historical. The {@code uniqueIdInTier} must be correctly specified
   * as it is used to identify the historical throughout the simulation.
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
    static final String T3 = "tier_t3";
  }

  static class Metric
  {
    static final String ASSIGNED_COUNT = "segment/assigned/count";
    static final String MOVED_COUNT = "segment/moved/count";
    static final String DROPPED_COUNT = "segment/dropped/count";
    static final String LOAD_QUEUE_COUNT = "segment/loadQueue/count";
  }

  static class Segments
  {
    /**
     * Segments of datasource {@link DS#WIKI}, size 500 MB each,
     * spanning 1 day containing 10 partitions.
     */
    static final List<DataSegment> WIKI_10X1D =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .withNumPartitions(10)
                          .eachOfSizeMb(500);
  }

  /**
   * Builder for a load rule.
   */
  static class Load
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
