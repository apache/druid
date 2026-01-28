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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.FIFTEEN_MINUTE_NAME;
import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.FIVE_MINUTE_NAME;
import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.ONE_MINUTE_NAME;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler.EXTRA_SCALING_LAG_PER_PARTITION_THRESHOLD;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler.computeExtraMaxPartitionsPerTaskIncrease;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler.computeValidTaskCounts;
import static org.mockito.Mockito.when;

@SuppressWarnings("SameParameterValue")
public class CostBasedAutoScalerTest
{
  private CostBasedAutoScaler autoScaler;
  private CostBasedAutoScalerConfig config;

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

    config = CostBasedAutoScalerConfig.builder()
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
    int[] validTaskCounts = computeValidTaskCounts(100, 25, 0L, 100);
    Assert.assertTrue("Should contain the current task count", contains(validTaskCounts, 25));
    Assert.assertTrue("Should contain the next scale-up option", contains(validTaskCounts, 34));

    // Edge cases
    Assert.assertEquals(0, computeValidTaskCounts(0, 10, 0L, 100).length);
    Assert.assertEquals(0, computeValidTaskCounts(-5, 10, 0L, 100).length);

    // Single partition
    int[] singlePartition = computeValidTaskCounts(1, 1, 0L, 100);
    Assert.assertTrue("Single partition should have at least one valid count", singlePartition.length > 0);
    Assert.assertTrue("Single partition should contain 1", contains(singlePartition, 1));

    // Current exceeds partitions - should still yield valid, deduplicated options
    int[] exceedsPartitions = computeValidTaskCounts(2, 5, 0L, 100);
    Assert.assertEquals(2, exceedsPartitions.length);
    Assert.assertTrue(contains(exceedsPartitions, 1));
    Assert.assertTrue(contains(exceedsPartitions, 2));

    // Lag expansion: low lag should not include max, high lag should
    int[] lowLagCounts = computeValidTaskCounts(30, 3, 0L, 30);
    Assert.assertFalse("Low lag should not include max task count", contains(lowLagCounts, 30));
    Assert.assertTrue("Low lag should cap scale up around 4 tasks", contains(lowLagCounts, 4));

    long highAggregateLag = 30L * 500_000L;
    int[] highLagCounts = computeValidTaskCounts(30, 3, highAggregateLag, 30);
    Assert.assertTrue("High lag should allow scaling to max tasks", contains(highLagCounts, 30));

    // Respects taskCountMax
    int[] cappedCounts = computeValidTaskCounts(30, 4, highAggregateLag, 3);
    Assert.assertTrue("Should include taskCountMax when doable", contains(cappedCounts, 3));
    Assert.assertFalse("Should not exceed taskCountMax", contains(cappedCounts, 4));
  }

  @Test
  public void testScalingExamplesTable()
  {
    int partitionCount = 30;
    int taskCountMax = 30;
    double pollIdleRatio = 0.1;
    double avgProcessingRate = 10.0;

    // Create a local autoScaler with taskCountMax matching the test parameters
    SupervisorSpec mockSpec = Mockito.mock(SupervisorSpec.class);
    SeekableStreamSupervisor mockSupervisor = Mockito.mock(SeekableStreamSupervisor.class);
    ServiceEmitter mockEmitter = Mockito.mock(ServiceEmitter.class);
    SeekableStreamSupervisorIOConfig mockIoConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(mockSpec.getId()).thenReturn("test-supervisor");
    when(mockSupervisor.getIoConfig()).thenReturn(mockIoConfig);
    when(mockIoConfig.getStream()).thenReturn("test-stream");

    CostBasedAutoScalerConfig localConfig = CostBasedAutoScalerConfig.builder()
                                                                      .taskCountMax(taskCountMax)
                                                                      .taskCountMin(1)
                                                                      .enableTaskAutoScaler(true)
                                                                      .lagWeight(0.6)
                                                                      .idleWeight(0.4)
                                                                      .build();

    CostBasedAutoScaler localAutoScaler = new CostBasedAutoScaler(
        mockSupervisor,
        localConfig,
        mockSpec,
        mockEmitter
    );

    class Example
    {
      final int currentTasks;
      final long lagPerPartition;
      final int expectedTasks;

      Example(int currentTasks, long lagPerPartition, int expectedTasks)
      {
        this.currentTasks = currentTasks;
        this.lagPerPartition = lagPerPartition;
        this.expectedTasks = expectedTasks;
      }
    }

    Example[] examples = new Example[]{
        new Example(3, 50_000L, 8),
        new Example(3, 300_000L, 15),
        new Example(3, 500_000L, 30),
        new Example(10, 100_000L, 15),
        new Example(10, 300_000L, 30),
        new Example(10, 500_000L, 30),
        new Example(20, 500_000L, 30),
        new Example(25, 500_000L, 30)
    };

    for (Example example : examples) {
      long aggregateLag = example.lagPerPartition * partitionCount;
      int[] validCounts = computeValidTaskCounts(partitionCount, example.currentTasks, aggregateLag, taskCountMax);
      Assert.assertTrue(
          "Should include expected task count for current=" + example.currentTasks + ", lag=" + example.lagPerPartition,
          contains(validCounts, example.expectedTasks)
      );

      CostMetrics metrics = createMetricsWithRate(
          example.lagPerPartition,
          example.currentTasks,
          partitionCount,
          pollIdleRatio,
          avgProcessingRate
      );
      int actualOptimal = localAutoScaler.computeOptimalTaskCount(metrics);
      if (actualOptimal == -1) {
        actualOptimal = example.currentTasks;
      }
      Assert.assertEquals(
          "Optimal task count should match for current=" + example.currentTasks
          + ", lag=" + example.lagPerPartition
          + ", valid=" + Arrays.toString(validCounts),
          example.expectedTasks,
          actualOptimal
      );
    }
  }

  @Test
  public void testComputeExtraPPTIncrease()
  {
    // No extra increase below the threshold
    Assert.assertEquals(0, computeExtraMaxPartitionsPerTaskIncrease(30L * 49_000L, 30, 3, 30));
    Assert.assertEquals(4, computeExtraMaxPartitionsPerTaskIncrease(30L * EXTRA_SCALING_LAG_PER_PARTITION_THRESHOLD, 30, 3, 30));

    // More aggressive increase when the lag is high
    Assert.assertEquals(6, computeExtraMaxPartitionsPerTaskIncrease(30L * 300_000L, 30, 3, 30));
    // Zero when on max task count
    Assert.assertEquals(0, computeExtraMaxPartitionsPerTaskIncrease(30L * 500_000L, 30, 30, 30));
  }

  @Test
  public void testComputeOptimalTaskCount()
  {
    // Invalid inputs return -1
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(null));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(0.0, 10, 0, 0.0)));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(100.0, 10, -5, 0.3)));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(100.0, -1, 100, 0.3)));

    // High idle (underutilized) - should scale down
    int scaleDownResult = autoScaler.computeOptimalTaskCount(createMetrics(100.0, 25, 100, 0.8));
    Assert.assertTrue("Should scale down when idle > 0.6", scaleDownResult < 25);

    // Very high idle with high task count - should scale down
    int highIdleResult = autoScaler.computeOptimalTaskCount(createMetrics(10.0, 50, 100, 0.9));
    Assert.assertTrue("Scale down scenario should return optimal <= current", highIdleResult <= 50);

    // With low idle and balanced weights, the algorithm should not scale up aggressively
    int lowIdleResult = autoScaler.computeOptimalTaskCount(createMetrics(1000.0, 25, 100, 0.1));
    Assert.assertTrue("With low idle and balanced weights, should not scale up aggressively", lowIdleResult <= 25);
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

    // Invalid types: non-map task metric
    Map<String, Map<String, Object>> nonMapTask = new HashMap<>();
    nonMapTask.put("0", Collections.singletonMap("task-0", "not-a-map"));
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(nonMapTask), 0.0001);

    // Invalid types: empty autoscaler metrics
    Map<String, Map<String, Object>> emptyAutoscaler = new HashMap<>();
    Map<String, Object> taskStats1 = new HashMap<>();
    taskStats1.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, new HashMap<>());
    emptyAutoscaler.put("0", Collections.singletonMap("task-0", taskStats1));
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(emptyAutoscaler), 0.0001);

    // Invalid types: non-map autoscaler metrics
    Map<String, Map<String, Object>> nonMapAutoscaler = new HashMap<>();
    Map<String, Object> taskStats2 = new HashMap<>();
    taskStats2.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, "not-a-map");
    nonMapAutoscaler.put("0", Collections.singletonMap("task-0", taskStats2));
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(nonMapAutoscaler), 0.0001);

    // Invalid types: non-number poll idle ratio
    Map<String, Map<String, Object>> nonNumberRatio = new HashMap<>();
    Map<String, Object> taskStats3 = new HashMap<>();
    Map<String, Object> autoscalerMetrics = new HashMap<>();
    autoscalerMetrics.put(SeekableStreamIndexTaskRunner.POLL_IDLE_RATIO_KEY, "not-a-number");
    taskStats3.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, autoscalerMetrics);
    nonNumberRatio.put("0", Collections.singletonMap("task-0", taskStats3));
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(nonNumberRatio), 0.0001);
  }

  @Test
  public void testExtractMovingAverage()
  {
    // Null and empty return -1
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(null), 0.0001);
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(Collections.emptyMap()), 0.0001);

    // Missing metrics return -1
    Map<String, Map<String, Object>> missingMetrics = new HashMap<>();
    missingMetrics.put("0", Collections.singletonMap("task-0", new HashMap<>()));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(missingMetrics), 0.0001);

    // Valid stats return average (using 5-minute)
    Map<String, Map<String, Object>> validStats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", buildTaskStatsWithMovingAverage(1000.0));
    group.put("task-1", buildTaskStatsWithMovingAverage(2000.0));
    validStats.put("0", group);
    Assert.assertEquals(1500.0, CostBasedAutoScaler.extractMovingAverage(validStats), 0.0001);

    // Interval fallback: 15-minute preferred, then 5-minute, then 1-minute
    Map<String, Map<String, Object>> fifteenMin = new HashMap<>();
    fifteenMin.put("0", Collections.singletonMap("task-0", buildStatsWithMovingAverageForInterval(FIFTEEN_MINUTE_NAME, 1500.0)));
    Assert.assertEquals(1500.0, CostBasedAutoScaler.extractMovingAverage(fifteenMin), 0.0001);

    // 1-minute as a final fallback
    Map<String, Map<String, Object>> oneMin = new HashMap<>();
    oneMin.put(
        "0",
        Collections.singletonMap("task-0", buildStatsWithMovingAverageForInterval(ONE_MINUTE_NAME, 500.0))
    );
    Assert.assertEquals(500.0, CostBasedAutoScaler.extractMovingAverage(oneMin), 0.0001);

    // 15-minute preferred over 5-minute when both available
    Map<String, Map<String, Object>> allIntervals = new HashMap<>();
    allIntervals.put("0", Collections.singletonMap("task-0", buildStatsWithMultipleMovingAverages(1500.0, 1000.0, 500.0)));
    Assert.assertEquals(1500.0, CostBasedAutoScaler.extractMovingAverage(allIntervals), 0.0001);

    // Falls back to 5-minute when 15-minute is null
    Map<String, Map<String, Object>> nullFifteen = new HashMap<>();
    nullFifteen.put("0", Collections.singletonMap("task-0", buildStatsWithNullInterval(FIFTEEN_MINUTE_NAME, FIVE_MINUTE_NAME, 750.0)));
    Assert.assertEquals(750.0, CostBasedAutoScaler.extractMovingAverage(nullFifteen), 0.0001);

    // Falls back to 1-minute when both 15 and 5 are null
    Map<String, Map<String, Object>> bothNull = new HashMap<>();
    bothNull.put("0", Collections.singletonMap("task-0", buildStatsWithTwoNullIntervals(250.0)));
    Assert.assertEquals(250.0, CostBasedAutoScaler.extractMovingAverage(bothNull), 0.0001);
  }

  @Test
  public void testExtractMovingAverageInvalidTypes()
  {
    // Non-map task metric
    Map<String, Map<String, Object>> nonMapTask = new HashMap<>();
    nonMapTask.put("0", Collections.singletonMap("task-0", "not-a-map"));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(nonMapTask), 0.0001);

    Map<String, Map<String, Object>> missingBuild = new HashMap<>();
    Map<String, Object> taskStats1 = new HashMap<>();
    taskStats1.put("movingAverages", new HashMap<>());
    missingBuild.put("0", Collections.singletonMap("task-0", taskStats1));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(missingBuild), 0.0001);

    Map<String, Map<String, Object>> nonMapMA = new HashMap<>();
    Map<String, Object> taskStats2 = new HashMap<>();
    taskStats2.put("movingAverages", "not-a-map");
    nonMapMA.put("0", Collections.singletonMap("task-0", taskStats2));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(nonMapMA), 0.0001);
  }

  @Test
  public void testComputeTaskCountForRolloverReturnsScaleDownValue()
  {
    // Tests the happy path: computeTaskCountForRollover returns optimal count for scale-down
    // This is the key difference from computeTaskCountForScaleAction which blocks scale-down
    SupervisorSpec spec = Mockito.mock(SupervisorSpec.class);
    SeekableStreamSupervisor supervisor = Mockito.mock(SeekableStreamSupervisor.class);
    ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    SeekableStreamSupervisorIOConfig ioConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(spec.getId()).thenReturn("test-supervisor");
    when(spec.isSuspended()).thenReturn(false);
    when(supervisor.getIoConfig()).thenReturn(ioConfig);
    when(ioConfig.getStream()).thenReturn("test-stream");
    when(ioConfig.getTaskCount()).thenReturn(10);

    CostBasedAutoScalerConfig cfg = CostBasedAutoScalerConfig.builder()
                                                             .taskCountMax(100)
                                                             .taskCountMin(1)
                                                             .enableTaskAutoScaler(true)
                                                             .lagWeight(0.6)
                                                             .idleWeight(0.4)
                                                             .build();

    CostBasedAutoScaler scaler = Mockito.spy(new CostBasedAutoScaler(supervisor, cfg, spec, emitter));

    // Mock computeOptimalTaskCount to return a scale-down value (5 < current 10)
    Mockito.doReturn(5).when(scaler).computeOptimalTaskCount(Mockito.any());

    // Set lastKnownMetrics by calling with non-null metrics
    CostMetrics metrics = new CostMetrics(100.0, 10, 100, 0.8, 3600, 1000.0);
    scaler.setLastKnownMetrics(metrics);

    int result = scaler.computeTaskCountForRollover();

    // Unlike computeTaskCountForScaleAction which returns -1 for scale-down,
    // computeTaskCountForRollover should return the optimal count (5)
    Assert.assertEquals(
        "computeTaskCountForRollover should return optimal count for scale-down during rollover",
        5,
        result
    );
  }

  @Test
  public void testComputeTaskCountForRolloverAndConfigProperties()
  {
    SupervisorSpec spec = Mockito.mock(SupervisorSpec.class);
    SeekableStreamSupervisor supervisor = Mockito.mock(SeekableStreamSupervisor.class);
    ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    SeekableStreamSupervisorIOConfig ioConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(spec.getId()).thenReturn("s-up");
    when(supervisor.getIoConfig()).thenReturn(ioConfig);
    when(ioConfig.getStream()).thenReturn("stream");

    // Test config defaults for scaleDownBarrier, defaultProcessingRate, scaleDownDuringTaskRolloverOnly
    CostBasedAutoScalerConfig cfgWithDefaults = CostBasedAutoScalerConfig.builder()
                                                                          .taskCountMax(10)
                                                                          .taskCountMin(1)
                                                                          .enableTaskAutoScaler(true)
                                                                          .build();
    Assert.assertEquals(5, cfgWithDefaults.getScaleDownBarrier());
    Assert.assertEquals(1000.0, cfgWithDefaults.getDefaultProcessingRate(), 0.001);
    Assert.assertFalse(cfgWithDefaults.isScaleDownOnTaskRolloverOnly());

    // Test custom config values
    CostBasedAutoScalerConfig cfgWithCustom = CostBasedAutoScalerConfig.builder()
                                                                        .taskCountMax(10)
                                                                        .taskCountMin(1)
                                                                        .enableTaskAutoScaler(true)
                                                                        .scaleDownBarrier(10)
                                                                        .defaultProcessingRate(5000.0)
                                                                        .scaleDownDuringTaskRolloverOnly(true)
                                                                        .build();
    Assert.assertEquals(10, cfgWithCustom.getScaleDownBarrier());
    Assert.assertEquals(5000.0, cfgWithCustom.getDefaultProcessingRate(), 0.001);
    Assert.assertTrue(cfgWithCustom.isScaleDownOnTaskRolloverOnly());

    // computeTaskCountForRollover returns -1 when scaleDownDuringTaskRolloverOnly=false (default)
    when(spec.isSuspended()).thenReturn(false);
    CostBasedAutoScaler scaler = new CostBasedAutoScaler(supervisor, cfgWithDefaults, spec, emitter);
    Assert.assertEquals(-1, scaler.computeTaskCountForRollover());

    // computeTaskCountForRollover returns -1 when lastKnownMetrics is null (even with scaleDownDuringTaskRolloverOnly=true)
    CostBasedAutoScaler scalerWithRolloverOnly = new CostBasedAutoScaler(supervisor, cfgWithCustom, spec, emitter);
    Assert.assertEquals(-1, scalerWithRolloverOnly.computeTaskCountForRollover());
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

  private CostMetrics createMetricsWithRate(
      double avgPartitionLag,
      int currentTaskCount,
      int partitionCount,
      double pollIdleRatio,
      double avgProcessingRate
  )
  {
    return new CostMetrics(
        avgPartitionLag,
        currentTaskCount,
        partitionCount,
        pollIdleRatio,
        3600,
        avgProcessingRate
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

  private Map<String, Object> buildTaskStatsWithMovingAverage(double processRate)
  {
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(FIVE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, processRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }

  private Map<String, Object> buildStatsWithMovingAverageForInterval(String intervalName, double processRate)
  {
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(intervalName, Map.of(RowIngestionMeters.PROCESSED, processRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }

  private Map<String, Object> buildStatsWithMultipleMovingAverages(
      double fifteenMinRate,
      double fiveMinRate,
      double oneMinRate
  )
  {
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(FIFTEEN_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, fifteenMinRate));
    buildSegments.put(FIVE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, fiveMinRate));
    buildSegments.put(ONE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, oneMinRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }

  private Map<String, Object> buildStatsWithNullInterval(
      String nullInterval,
      String validInterval,
      double processedRate
  )
  {
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(nullInterval, null);
    buildSegments.put(validInterval, Map.of(RowIngestionMeters.PROCESSED, processedRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }

  private Map<String, Object> buildStatsWithTwoNullIntervals(double oneMinRate)
  {
    Map<String, Object> buildSegments = new HashMap<>();
    buildSegments.put(FIFTEEN_MINUTE_NAME, null);
    buildSegments.put(FIVE_MINUTE_NAME, null);
    buildSegments.put(ONE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, oneMinRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }
}
