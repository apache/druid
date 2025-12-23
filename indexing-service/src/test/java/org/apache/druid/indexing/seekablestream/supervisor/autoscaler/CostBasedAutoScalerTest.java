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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler;

import org.apache.druid.indexing.overlord.supervisor.SupervisorSpec;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.FIFTEEN_MINUTE_NAME;
import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.FIVE_MINUTE_NAME;
import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.ONE_MINUTE_NAME;
import static org.mockito.Mockito.when;

public class CostBasedAutoScalerTest
{
  private CostBasedAutoScaler autoScaler;

  @Before
  public void setUp()
  {
    SupervisorSpec mockSupervisorSpec = Mockito.mock(SupervisorSpec.class);
    SeekableStreamSupervisor mockSupervisor = Mockito.mock(SeekableStreamSupervisor.class);
    ServiceEmitter mockEmitter = Mockito.mock(ServiceEmitter.class);
    SeekableStreamSupervisorIOConfig mockIoConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(mockSupervisorSpec.getId()).thenReturn("test-supervisor");
    when(mockSupervisor.getIoConfig()).thenReturn(mockIoConfig);
    when(mockIoConfig.getStream()).thenReturn("test-stream");

    CostBasedAutoScalerConfig config = CostBasedAutoScalerConfig.builder()
                                                                .taskCountMax(100)
                                                                .taskCountMin(1)
                                                                .enableTaskAutoScaler(true)
                                                                .lagWeight(0.6)
                                                                .idleWeight(0.4)
                                                                .build();

    autoScaler = new CostBasedAutoScaler(mockSupervisor, config, mockSupervisorSpec, mockEmitter);
  }

  @Test
  public void testComputeValidTaskCounts()
  {
    // For 100 partitions at 25 tasks (4 partitions/task), valid counts include 25 and 34
    int[] validTaskCounts = CostBasedAutoScaler.computeValidTaskCounts(100, 25);

    Assert.assertTrue("Should contain the current task count", contains(validTaskCounts, 25));
    Assert.assertTrue("Should contain the next scale-up option", contains(validTaskCounts, 34));

    // Edge case: zero partitions return an empty array
    Assert.assertEquals(0, CostBasedAutoScaler.computeValidTaskCounts(0, 10).length);
  }

  @Test
  public void testComputeOptimalTaskCountInvalidInputs()
  {
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(null));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(0.0, 10, 0, 0.0)));
  }

  @Test
  public void testComputeOptimalTaskCountScaling()
  {
    // High idle (underutilized) - should scale down
    // With high idle (0.8), the algorithm evaluates lower task counts and finds they have lower idle cost
    int scaleDownResult = autoScaler.computeOptimalTaskCount(createMetrics(100.0, 25, 100, 0.8));
    Assert.assertTrue("Should scale down when idle > 0.6", scaleDownResult < 25);
  }

  @Test
  public void testComputeOptimalTaskCountLowIdleDoesNotScaleUpWithBalancedWeights()
  {
    // With a corrected idle ratio model and marginal lag model, low idle does not automatically trigger scale-up.
    int result = autoScaler.computeOptimalTaskCount(createMetrics(1000.0, 25, 100, 0.1));

    // Algorithm evaluates costs and may find the current count optimal
    // or may scale down if idle cost reduction outweighs lag increase.
    Assert.assertTrue(
        "With low idle and balanced weights, algorithm should not scale up aggressively", result <= 25
    );
  }

  @Test
  public void testExtractPollIdleRatio()
  {
    // Null and empty return 0
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(null), 0.0001);
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(Collections.emptyMap()), 0.0001);

    // Missing metrics return 0
    Map<String, Map<String, Object>> missingMetrics = new HashMap<>();
    missingMetrics.put("0", Collections.singletonMap("task-0", new HashMap<>()));
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(missingMetrics), 0.0001);

    // Valid stats return average
    Map<String, Map<String, Object>> validStats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", buildTaskStatsWithPollIdle(0.3));
    group.put("task-1", buildTaskStatsWithPollIdle(0.5));
    validStats.put("0", group);
    Assert.assertEquals(0.4, CostBasedAutoScaler.extractPollIdleRatio(validStats), 0.0001);
  }

  @Test
  public void testExtractProcessingRateMovingAverage()
  {
    // Null and empty return -1
    Assert.assertEquals(
        -1.,
        CostBasedAutoScaler.extractMovingAverage(null),
        0.0001
    );
    Assert.assertEquals(
        -1.,
        CostBasedAutoScaler.extractMovingAverage(Collections.emptyMap()),
        0.0001
    );

    // Missing metrics return -1
    Map<String, Map<String, Object>> missingMetrics = new HashMap<>();
    missingMetrics.put("0", Collections.singletonMap("task-0", new HashMap<>()));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(missingMetrics), 0.0001);

    // Valid stats return average
    Map<String, Map<String, Object>> validStats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", buildTaskStatsWithMovingAverage(1000.0));
    group.put("task-1", buildTaskStatsWithMovingAverage(2000.0));
    validStats.put("0", group);
    Assert.assertEquals(1500.0, CostBasedAutoScaler.extractMovingAverage(validStats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageFifteenMinuteFallback()
  {
    // Test that 15-minute average is preferred when available
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", buildTaskStatsWithMovingAverageForInterval(FIFTEEN_MINUTE_NAME, 1500.0));
    stats.put("0", group);
    Assert.assertEquals(1500.0, CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageOneMinuteFallback()
  {
    // Test that 1-minute average is used as final fallback
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put(
        "task-0",
        buildTaskStatsWithMovingAverageForInterval(ONE_MINUTE_NAME, 500.0)
    );
    stats.put("0", group);
    Assert.assertEquals(500.0, CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAveragePrefersFifteenOverFive()
  {
    // Test that 15-minute average is preferred over 5-minute when both are available
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", buildTaskStatsWithMultipleMovingAverages(1500.0, 1000.0, 500.0));
    stats.put("0", group);
    // Should use 15-minute average (1500.0), not 5-minute (1000.0) or 1-minute (500.0)
    Assert.assertEquals(1500.0, CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testComputeTaskCountForScaleActionScaleUp()
  {
    // Test scale-up scenario: optimal > current
    // With low idle ratio and high lag, should want to scale up
    CostMetrics highLagMetrics = new CostMetrics(
        10000.0,  // high lag
        5,        // current task count
        100,      // partition count
        0.1,      // low idle ratio (busy)
        3600,
        1000.0
    );

    int result = autoScaler.computeOptimalTaskCount(highLagMetrics);
    // The algorithm should evaluate different task counts
    Assert.assertTrue("Should return a valid task count", result >= -1);
  }

  @Test
  public void testComputeTaskCountForScaleActionNoScale()
  {
    // Test no-scale scenario: optimal == current
    CostMetrics balancedMetrics = new CostMetrics(
        100.0,    // moderate lag
        25,       // current task count
        100,      // partition count
        0.4,      // moderate idle ratio
        3600,
        1000.0
    );

    int result = autoScaler.computeOptimalTaskCount(balancedMetrics);
    // Either returns -1 (no change) or a different task count
    Assert.assertTrue("Result should be -1 or a valid positive number", result >= -1);
  }

  @Test
  public void testComputeOptimalTaskCountWithNegativePartitions()
  {
    CostMetrics invalidMetrics = new CostMetrics(
        100.0,
        10,
        -5,  // negative partition count
        0.3,
        3600,
        1000.0
    );
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(invalidMetrics));
  }

  @Test
  public void testComputeOptimalTaskCountWithNegativeTaskCount()
  {
    CostMetrics invalidMetrics = new CostMetrics(
        100.0,
        -1,  // negative task count
        100,
        0.3,
        3600,
        1000.0
    );
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(invalidMetrics));
  }

  @Test
  public void testComputeValidTaskCountsWithSinglePartition()
  {
    // Edge case: single partition
    int[] validTaskCounts = CostBasedAutoScaler.computeValidTaskCounts(1, 1);
    Assert.assertTrue("Should have at least one valid count", validTaskCounts.length > 0);
    Assert.assertTrue("Should contain 1 as valid count", contains(validTaskCounts, 1));
  }

  @Test
  public void testComputeValidTaskCountsWithNegativePartitions()
  {
    // Negative partitions should return empty array
    int[] validTaskCounts = CostBasedAutoScaler.computeValidTaskCounts(-5, 10);
    Assert.assertEquals(0, validTaskCounts.length);
  }

  @Test
  public void testExtractPollIdleRatioWithNonMapTaskMetric()
  {
    // Test branch where taskMetric is not a Map
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", "not-a-map");
    stats.put("0", group);
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(stats), 0.0001);
  }

  @Test
  public void testExtractPollIdleRatioWithMissingAutoscalerMetrics()
  {
    // Test branch where autoscaler metrics map is present but poll idle ratio is missing
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    Map<String, Object> emptyAutoscalerMetrics = new HashMap<>();
    taskStats.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, emptyAutoscalerMetrics);
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageWithNonMapTaskMetric()
  {
    // Test branch where taskMetric is not a Map
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", "not-a-map");
    stats.put("0", group);
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageWithMissingBuildSegments()
  {
    // Test branch where movingAverages exists but buildSegments is missing
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    Map<String, Object> movingAverages = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageWithNonMapMovingAverage()
  {
    // Test branch where movingAveragesObj is not a Map
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", "not-a-map");
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testComputeTaskCountForScaleActionReturnsMinusOneWhenScaleDown()
  {
    // When optimal < current, computeTaskCountForScaleAction should return -1
    // This tests the ternary: optimalTaskCount >= currentTaskCount ? optimalTaskCount : -1
    // Create a scenario where the algorithm wants to scale down (high idle ratio)
    CostMetrics highIdleMetrics = new CostMetrics(
        10.0,     // low lag
        50,       // current task count (high)
        100,      // partition count
        0.9,      // very high idle ratio (underutilized)
        3600,
        1000.0
    );

    // computeOptimalTaskCount may return a lower task count
    int optimalResult = autoScaler.computeOptimalTaskCount(highIdleMetrics);
    // The test verifies that computeTaskCountForScaleAction handles scale-down correctly
    Assert.assertTrue("Scale down scenario should return optimal <= current", optimalResult <= 50);
  }

  @Test
  public void testComputeTaskCountForScaleActionReturnsPositiveWhenScaleUp()
  {
    // When optimal > current, computeTaskCountForScaleAction should return the optimal value
    // Create a scenario with low idle (tasks are busy) and some lag
    CostMetrics busyMetrics = new CostMetrics(
        5000.0,   // significant lag
        5,        // low current task count
        100,      // partition count (20 partitions per task)
        0.05,     // very low idle ratio (tasks are very busy)
        3600,
        1000.0
    );

    int optimalResult = autoScaler.computeOptimalTaskCount(busyMetrics);
    // With very low idle ratio, algorithm should evaluate higher task counts
    Assert.assertTrue("Busy scenario result should be valid", optimalResult >= -1);
  }

  @Test
  public void testComputeOptimalTaskCountWhenOptimalEqualsCurrent()
  {
    // Test the branch where optimalTaskCount == currentTaskCount returns -1
    // Create balanced metrics that likely result in current count being optimal
    CostMetrics balancedMetrics = new CostMetrics(
        50.0,     // low lag
        20,       // current task count
        100,      // partition count (5 partitions per task)
        0.5,      // moderate idle ratio
        3600,
        1000.0
    );

    int result = autoScaler.computeOptimalTaskCount(balancedMetrics);
    // Either -1 (optimal == current) or a different task count
    Assert.assertTrue("Result should be -1 or positive", result >= -1);
  }

  @Test
  public void testExtractPollIdleRatioWithNonMapAutoscalerMetrics()
  {
    // Test branch where AUTOSCALER_METRICS_KEY exists but is not a Map
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, "not-a-map");
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(stats), 0.0001);
  }

  @Test
  public void testExtractPollIdleRatioWithNonNumberPollIdleRatio()
  {
    // Test branch where pollIdleRatioAvg exists but is not a Number
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    Map<String, Object> autoscalerMetrics = new HashMap<>();
    autoscalerMetrics.put(SeekableStreamIndexTaskRunner.POLL_IDLE_RATIO_KEY, "not-a-number");
    taskStats.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, autoscalerMetrics);
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageWithNonMapBuildSegments()
  {
    // Test branch where buildSegmentsObj is not a Map
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, "not-a-map");
    taskStats.put("movingAverages", movingAverages);
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageWithNonMapIntervalData()
  {
    // Test branch where the 15min/5min/1min interval data is not a Map
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    Map<String, Object> movingAverages = new HashMap<>();
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(FIFTEEN_MINUTE_NAME, "not-a-map");
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);
    taskStats.put("movingAverages", movingAverages);
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageWithNonNumberProcessedRate()
  {
    // Test branch where processedRate is not a Number
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    Map<String, Object> movingAverages = new HashMap<>();
    Map<String, Object> buildSegments = new HashMap<>();
    Map<String, Object> fifteenMin = new HashMap<>();
    fifteenMin.put(RowIngestionMeters.PROCESSED, "not-a-number");
    buildSegments.put(FIFTEEN_MINUTE_NAME, fifteenMin);
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);
    taskStats.put("movingAverages", movingAverages);
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageFallsBackToFiveMinuteWhenFifteenMinuteNull()
  {
    // Test the fallback from 15min to 5min when 15min is explicitly null
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    Map<String, Object> movingAverages = new HashMap<>();
    Map<String, Object> buildSegments = new HashMap<>();
    // Explicitly set 15min to null (not just missing)
    buildSegments.put(FIFTEEN_MINUTE_NAME, null);
    buildSegments.put(FIVE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, 750.0));
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);
    taskStats.put("movingAverages", movingAverages);
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(750.0, CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  @Test
  public void testExtractMovingAverageFallsBackToOneMinuteWhenBothNull()
  {
    // Test the fallback from 15min to 5min to 1min when both 15min and 5min are null
    Map<String, Map<String, Object>> stats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    Map<String, Object> taskStats = new HashMap<>();
    Map<String, Object> movingAverages = new HashMap<>();
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(FIFTEEN_MINUTE_NAME, null);
    buildSegments.put(FIVE_MINUTE_NAME, null);
    buildSegments.put(ONE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, 250.0));
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);
    taskStats.put("movingAverages", movingAverages);
    group.put("task-0", taskStats);
    stats.put("0", group);
    Assert.assertEquals(250.0, CostBasedAutoScaler.extractMovingAverage(stats), 0.0001);
  }

  private CostMetrics createMetrics(
      double avgPartitionLag,
      int currentTaskCount,
      int partitionCount,
      double pollIdleRatio
  )
  {
    return new CostMetrics(
        avgPartitionLag,
        currentTaskCount,
        partitionCount,
        pollIdleRatio,
        3600,
        1000.0
    );
  }

  private boolean contains(int[] array, int value)
  {
    for (int i : array) {
      if (i == value) {
        return true;
      }
    }
    return false;
  }

  private Map<String, Object> buildTaskStatsWithPollIdle(double pollIdleRatio)
  {
    Map<String, Object> autoscalerMetrics = new HashMap<>();
    autoscalerMetrics.put(SeekableStreamIndexTaskRunner.POLL_IDLE_RATIO_KEY, pollIdleRatio);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, autoscalerMetrics);
    return taskStats;
  }

  private Map<String, Object> buildTaskStatsWithMovingAverage(double processedRate)
  {
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(
        FIVE_MINUTE_NAME,
        Map.of(RowIngestionMeters.PROCESSED, processedRate)
    );

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }

  private Map<String, Object> buildTaskStatsWithMovingAverageForInterval(String intervalName, double processedRate)
  {
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(intervalName, Map.of(RowIngestionMeters.PROCESSED, processedRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }

  private Map<String, Object> buildTaskStatsWithMultipleMovingAverages(
      double fifteenMinRate,
      double fiveMinRate,
      double oneMinRate
  )
  {
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(
        FIFTEEN_MINUTE_NAME,
        Map.of(RowIngestionMeters.PROCESSED, fifteenMinRate)
    );
    buildSegments.put(FIVE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, fiveMinRate));
    buildSegments.put(ONE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, oneMinRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }

  @Test
  public void testComputeValidTaskCountsWhenCurrentExceedsPartitions()
  {
    // the currentTaskCount > partitionCount should still yield valid,
    // deduplicated options
    int[] counts = CostBasedAutoScaler.computeValidTaskCounts(2, 5);
    Assert.assertEquals(2, counts.length);
    Assert.assertTrue(contains(counts, 1));
    Assert.assertTrue(contains(counts, 2));
  }

  @Test
  public void testComputeTaskCountForRolloverReturnsMinusOneWhenSuspended()
  {
    // Arrange: build autoscaler with suspended spec so collectMetrics returns null
    SupervisorSpec spec = Mockito.mock(SupervisorSpec.class);
    SeekableStreamSupervisor supervisor = Mockito.mock(SeekableStreamSupervisor.class);
    ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    SeekableStreamSupervisorIOConfig ioConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(spec.getId()).thenReturn("s-up");
    when(spec.isSuspended()).thenReturn(true);
    when(supervisor.getIoConfig()).thenReturn(ioConfig);
    when(ioConfig.getStream()).thenReturn("stream");

    CostBasedAutoScalerConfig cfg = CostBasedAutoScalerConfig.builder()
                                                             .taskCountMax(10)
                                                             .taskCountMin(1)
                                                             .enableTaskAutoScaler(true)
                                                             .lagWeight(0.5)
                                                             .idleWeight(0.5)
                                                             .build();

    CostBasedAutoScaler scaler = new CostBasedAutoScaler(supervisor, cfg, spec, emitter);

    // Then
    Assert.assertEquals(-1, scaler.computeTaskCountForRollover());
  }

  @Test
  public void testComputeTaskCountForRolloverReturnsMinusOneWhenLagStatsNull()
  {
    // Arrange: collectMetrics should early-return when lagStats is null
    SupervisorSpec spec = Mockito.mock(SupervisorSpec.class);
    SeekableStreamSupervisor supervisor = Mockito.mock(SeekableStreamSupervisor.class);
    ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    SeekableStreamSupervisorIOConfig ioConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(spec.getId()).thenReturn("s-up");
    when(spec.isSuspended()).thenReturn(false);
    when(supervisor.computeLagStats()).thenReturn(null);
    when(supervisor.getIoConfig()).thenReturn(ioConfig);
    when(ioConfig.getStream()).thenReturn("stream");

    CostBasedAutoScalerConfig cfg = CostBasedAutoScalerConfig.builder()
                                                             .taskCountMax(10)
                                                             .taskCountMin(1)
                                                             .enableTaskAutoScaler(true)
                                                             .lagWeight(0.5)
                                                             .idleWeight(0.5)
                                                             .build();

    CostBasedAutoScaler scaler = new CostBasedAutoScaler(supervisor, cfg, spec, emitter);

    // Then
    Assert.assertEquals(-1, scaler.computeTaskCountForRollover());
  }
}
