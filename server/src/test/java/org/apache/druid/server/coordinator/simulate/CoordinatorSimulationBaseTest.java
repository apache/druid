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
import org.apache.druid.java.util.metrics.MetricsVerifier;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.coordinator.CoordinatorDynamicConfig;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.server.coordinator.rules.ForeverLoadRule;
import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.DataSegment;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

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
public abstract class CoordinatorSimulationBaseTest implements
    CoordinatorSimulation.CoordinatorState,
    CoordinatorSimulation.ClusterState,
    MetricsVerifier
{
  static final double DOUBLE_DELTA = 10e-9;

  private CoordinatorSimulation sim;
  private MetricsVerifier metricsVerifier;

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

  /**
   * This must be called to start the simulation and set the correct state.
   */
  void startSimulation(CoordinatorSimulation simulation)
  {
    this.sim = simulation;
    simulation.start();
    this.metricsVerifier = this.sim.coordinator().getMetricsVerifier();
  }

  @Override
  public void runCoordinatorCycle()
  {
    sim.coordinator().runCoordinatorCycle();
  }

  @Override
  public MetricsVerifier getMetricsVerifier()
  {
    return null;
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
  public void removeServer(DruidServer server)
  {
    sim.cluster().removeServer(server);
  }

  @Override
  public void addServer(DruidServer server)
  {
    sim.cluster().addServer(server);
  }

  @Override
  public void addSegments(List<DataSegment> segments)
  {
    sim.cluster().addSegments(segments);
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

  @Override
  public List<Number> getMetricValues(
      String metricName,
      Map<String, Object> dimensionFilters
  )
  {
    return metricsVerifier.getMetricValues(metricName, dimensionFilters);
  }

  // Utility methods

  /**
   * Creates a {@link CoordinatorDynamicConfig} with the specified values of:
   * {@code maxSegmentsToMove, maxSegmentsInNodeLoadingQueue and replicationThrottleLimit}.
   * The created config always has {@code useBatchedSegmentSampler=true} to avoid
   * flakiness in tests.
   *
   * @see CoordinatorSimulationBaseTest
   */
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
  static Map<String, Object> filter(String... dimensionValues)
  {
    if (dimensionValues.length < 2 || dimensionValues.length % 2 == 1) {
      throw new IllegalArgumentException("Dimension key-values must be specified in pairs.");
    }

    final Map<String, Object> filters = new HashMap<>();
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
    static final String KOALA = "koala";
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
    static final String UNMOVED_COUNT = "segment/unmoved/count";
    static final String DROPPED_COUNT = "segment/dropped/count";
    static final String LOAD_QUEUE_COUNT = "segment/loadQueue/count";
  }

  static class Segments
  {
    /**
     * Segments of datasource {@link DS#WIKI}, size 500 MB each,
     * spanning 1 day containing 10 partitions each.
     */
    static final List<DataSegment> WIKI_10X1D =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(1, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .withNumPartitions(10)
                          .eachOfSizeInMb(500);

    /**
     * Segments of datasource {@link DS#WIKI}, size 500 MB each,
     * spanning 100 days containing 10 partitions each.
     */
    static final List<DataSegment> WIKI_10X100D =
        CreateDataSegments.ofDatasource(DS.WIKI)
                          .forIntervals(100, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .withNumPartitions(10)
                          .eachOfSizeInMb(500);

    /**
     * Segments of datasource {@link DS#KOALA}, size 500 MB each,
     * spanning 100 days containing 100 partitions each.
     */
    static final List<DataSegment> KOALA_100X100D =
        CreateDataSegments.ofDatasource(DS.KOALA)
                          .forIntervals(100, Granularities.DAY)
                          .startingAt("2022-01-01")
                          .withNumPartitions(100)
                          .eachOfSizeInMb(500);
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
