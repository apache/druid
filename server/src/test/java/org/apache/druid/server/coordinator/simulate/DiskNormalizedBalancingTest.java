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
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Simulation test to verify that the {@code diskNormalized} balancer strategy
 * equalizes disk utilization across homogeneous historicals.
 */
public class DiskNormalizedBalancingTest extends CoordinatorSimulationBaseTest
{
  private static final String DATASOURCE = "test_ds";
  private static final long SERVER_SIZE_MB = 100_000;

  @Override
  public void setUp()
  {
  }

  /**
   * Creates a skewed initial state: one server has 4x the disk usage of the
   * other. Verifies that the diskNormalized strategy moves segments from the
   * heavy server to the light one to reduce disk skew.
   */
  @Test
  public void testDiskNormalizedStrategyReducesSkew()
  {
    final DruidServer hist1 = createHistorical(1, Tier.T1, SERVER_SIZE_MB);
    final DruidServer hist2 = createHistorical(2, Tier.T1, SERVER_SIZE_MB);

    // "old" large segments (100 MB each) and "new" small segments (10 MB each)
    final List<DataSegment> largeSegments =
        CreateDataSegments.ofDatasource(DATASOURCE)
                          .forIntervals(40, Granularities.DAY)
                          .startingAt("2024-01-01")
                          .withNumPartitions(1)
                          .eachOfSizeInMb(100);
    final List<DataSegment> smallSegments =
        CreateDataSegments.ofDatasource(DATASOURCE)
                          .forIntervals(40, Granularities.DAY)
                          .startingAt("2025-01-01")
                          .withNumPartitions(1)
                          .eachOfSizeInMb(10);

    final List<DataSegment> allSegments = new ArrayList<>();
    allSegments.addAll(largeSegments);
    allSegments.addAll(smallSegments);

    // Create initial skew: hist1 gets all the large segments (4000 MB),
    // hist2 gets all the small segments (400 MB)
    largeSegments.forEach(hist1::addDataSegment);
    smallSegments.forEach(hist2::addDataSegment);

    long hist1SizeBefore = hist1.getCurrSize();
    long hist2SizeBefore = hist2.getCurrSize();
    Assert.assertTrue(
        "hist1 should start with significantly more data",
        hist1SizeBefore > hist2SizeBefore * 3
    );

    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withBalancer("diskNormalized")
                             .withSegments(allSegments)
                             .withServers(hist1, hist2)
                             .withRules(DATASOURCE, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    for (int i = 0; i < 10; ++i) {
      runCoordinatorCycle();
      loadQueuedSegments();
    }

    long hist1SizeAfter = hist1.getCurrSize();
    long hist2SizeAfter = hist2.getCurrSize();
    double skewBefore = (double) hist1SizeBefore / hist2SizeBefore;
    double skewAfter = Math.max(hist1SizeAfter, hist2SizeAfter)
                       / (double) Math.min(hist1SizeAfter, hist2SizeAfter);
    Assert.assertTrue(
        "Disk skew should be reduced (was " + skewBefore + ", now " + skewAfter + ")",
        skewAfter < skewBefore
    );

    verifyDatasourceIsFullyLoaded(DATASOURCE);
  }

  /**
   * Verifies that after disk usage converges, the strategy stops moving
   * segments (no oscillation).
   */
  @Test
  public void testNoOscillationAfterConvergence()
  {
    final List<DruidServer> historicals = new ArrayList<>();
    for (int i = 1; i <= 5; i++) {
      historicals.add(createHistorical(i, Tier.T1, SERVER_SIZE_MB));
    }

    // 100 segments, 50 MB each, evenly distributed
    final List<DataSegment> segments =
        CreateDataSegments.ofDatasource(DATASOURCE)
                          .forIntervals(100, Granularities.DAY)
                          .startingAt("2024-01-01")
                          .withNumPartitions(1)
                          .eachOfSizeInMb(50);

    for (int i = 0; i < segments.size(); i++) {
      historicals.get(i % historicals.size()).addDataSegment(segments.get(i));
    }

    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withBalancer("diskNormalized")
                             .withSegments(segments)
                             .withServers(historicals)
                             .withRules(DATASOURCE, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    // Run a few cycles to let any initial adjustments settle
    for (int i = 0; i < 5; ++i) {
      runCoordinatorCycle();
      loadQueuedSegments();
    }

    // Run one more cycle and verify no segments are moved
    runCoordinatorCycle();
    verifyNotEmitted(Metric.MOVED_COUNT);

    verifyDatasourceIsFullyLoaded(DATASOURCE);
  }

  /**
   * Verifies that new segments are assigned to the less-full server, even when
   * both servers have the same number of existing segments.
   */
  @Test
  public void testNewSegmentsPreferLessFullServer()
  {
    final DruidServer hist1 = createHistorical(1, Tier.T1, SERVER_SIZE_MB);
    final DruidServer hist2 = createHistorical(2, Tier.T1, SERVER_SIZE_MB);

    // hist1: 20 large segments at 200 MB each = 4000 MB
    // hist2: 20 small segments at 20 MB each  = 400 MB
    final List<DataSegment> largeExisting =
        CreateDataSegments.ofDatasource(DATASOURCE)
                          .forIntervals(20, Granularities.DAY)
                          .startingAt("2024-01-01")
                          .withNumPartitions(1)
                          .eachOfSizeInMb(200);
    final List<DataSegment> smallExisting =
        CreateDataSegments.ofDatasource(DATASOURCE)
                          .forIntervals(20, Granularities.DAY)
                          .startingAt("2024-06-01")
                          .withNumPartitions(1)
                          .eachOfSizeInMb(20);

    largeExisting.forEach(hist1::addDataSegment);
    smallExisting.forEach(hist2::addDataSegment);

    // New segments to assign
    final List<DataSegment> newSegments =
        CreateDataSegments.ofDatasource(DATASOURCE)
                          .forIntervals(20, Granularities.DAY)
                          .startingAt("2025-06-01")
                          .withNumPartitions(1)
                          .eachOfSizeInMb(50);

    final List<DataSegment> allSegments = new ArrayList<>();
    allSegments.addAll(largeExisting);
    allSegments.addAll(smallExisting);
    allSegments.addAll(newSegments);

    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withBalancer("diskNormalized")
                             .withSegments(allSegments)
                             .withServers(hist1, hist2)
                             .withRules(DATASOURCE, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    runCoordinatorCycle();
    loadQueuedSegments();

    // hist2 started with less disk usage, so it should receive more of the new segments
    int hist2Segments = hist2.getTotalSegments();
    Assert.assertTrue(
        "Less-full server should receive more new segments, but had " + hist2Segments + " total",
        hist2Segments > 20 + 10
    );

    verifyDatasourceIsFullyLoaded(DATASOURCE);
  }

  /**
   * Models the production temp tier: homogeneous historicals with similarly-sized
   * segments but a bimodal usage distribution (~90% vs ~69%).
   * <p>
   * Real observations:
   * <ul>
   *   <li>71 historicals, all ~8.4 TB maxSize</li>
   *   <li>56 servers at ~90% usage, 15 servers at ~69% usage</li>
   *   <li>Segments overwhelmingly 1-5 GB, avg ~2.7 GB</li>
   * </ul>
   * Scaled model: 10 servers (7 high, 3 low) with 500 MB segments.
   * Verifies the strategy converges toward balanced utilization.
   */
  @Test
  public void testBimodalSkewWithSimilarlySizedSegmentsConverges()
  {
    final int numHighUsage = 7;
    final int numLowUsage = 3;
    final int segmentsPerHighServer = 180;   // 90,000 MB = 90% of 100 GB
    final int segmentsPerLowServer = 138;    // 69,000 MB = 69% of 100 GB

    final List<DruidServer> historicals = new ArrayList<>();
    for (int i = 1; i <= numHighUsage + numLowUsage; i++) {
      historicals.add(createHistorical(i, Tier.T1, SERVER_SIZE_MB));
    }

    // All segments are the same size (500 MB), single datasource
    int totalSegments = numHighUsage * segmentsPerHighServer + numLowUsage * segmentsPerLowServer;
    final List<DataSegment> allSegments =
        CreateDataSegments.ofDatasource(DATASOURCE)
                          .forIntervals(totalSegments, Granularities.HOUR)
                          .startingAt("2024-01-01")
                          .withNumPartitions(1)
                          .eachOfSizeInMb(500);

    // Distribute: first 7 servers get 180 segments each, last 3 get 138 each
    int segIdx = 0;
    for (int i = 0; i < numHighUsage; i++) {
      for (int j = 0; j < segmentsPerHighServer; j++) {
        historicals.get(i).addDataSegment(allSegments.get(segIdx++));
      }
    }
    for (int i = 0; i < numLowUsage; i++) {
      for (int j = 0; j < segmentsPerLowServer; j++) {
        historicals.get(numHighUsage + i).addDataSegment(allSegments.get(segIdx++));
      }
    }

    // Verify initial skew
    long maxBefore = historicals.stream().mapToLong(DruidServer::getCurrSize).max().orElse(0);
    long minBefore = historicals.stream().mapToLong(DruidServer::getCurrSize).min().orElse(0);
    double skewBefore = (double) maxBefore / minBefore;
    Assert.assertTrue(
        "Initial skew should be > 1.2, was " + skewBefore,
        skewBefore > 1.2
    );

    final CoordinatorSimulation sim =
        CoordinatorSimulation.builder()
                             .withBalancer("diskNormalized")
                             .withSegments(allSegments)
                             .withServers(historicals)
                             .withRules(DATASOURCE, Load.on(Tier.T1, 1).forever())
                             .build();

    startSimulation(sim);

    for (int i = 0; i < 20; ++i) {
      runCoordinatorCycle();
      loadQueuedSegments();
    }

    long maxAfter = historicals.stream().mapToLong(DruidServer::getCurrSize).max().orElse(0);
    long minAfter = historicals.stream().mapToLong(DruidServer::getCurrSize).min().orElse(0);
    double skewAfter = (double) maxAfter / minAfter;

    Assert.assertTrue(
        "Skew should be reduced from " + skewBefore + " to below 1.15, but was " + skewAfter,
        skewAfter < 1.15
    );

    verifyDatasourceIsFullyLoaded(DATASOURCE);
  }
}
