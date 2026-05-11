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
import org.apache.druid.indexing.overlord.supervisor.autoscaler.LagStats;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.FIFTEEN_MINUTE_NAME;
import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.FIVE_MINUTE_NAME;
import static org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters.ONE_MINUTE_NAME;
import static org.apache.druid.indexing.seekablestream.supervisor.autoscaler.CostBasedAutoScaler.computeValidTaskCounts;
import static org.mockito.Mockito.when;

@SuppressWarnings({"SameParameterValue"})
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
    when(mockSupervisorSpec.getDataSources()).thenReturn(List.of("test-datasource"));
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
    final int[] validTaskCounts = computeValidTaskCounts(
        100,
        25,
        1,
        100
    );
    Assert.assertTrue("Expected current task count to be included", contains(validTaskCounts, 25));
    Assert.assertTrue("Expected next scale-up option (34) to be included", contains(validTaskCounts, 34));

    // Single partition
    final int[] singlePartition = computeValidTaskCounts(
        1,
        1,
        1,
        100
    );
    Assert.assertTrue("Single partition should yield at least one valid count", singlePartition.length > 0);
    Assert.assertTrue("Single partition should include task count 1", contains(singlePartition, 1));

    // Current exceeds partitions - should still yield valid, deduplicated options
    final int[] exceedsPartitions = computeValidTaskCounts(
        2,
        5,
        1,
        100
    );
    Assert.assertEquals(2, exceedsPartitions.length);
    Assert.assertTrue(contains(exceedsPartitions, 1));
    Assert.assertTrue(contains(exceedsPartitions, 2));

    // Unbounded candidate generation includes both nearby and maximum task counts.
    final int[] taskCounts = computeValidTaskCounts(30, 3, 1, 30);
    Assert.assertTrue("Valid task counts should include max task count", contains(taskCounts, 30));
    Assert.assertTrue("Valid task counts should include nearby scale-up task count", contains(taskCounts, 4));

    // Respects taskCountMax
    final int[] cappedCounts = computeValidTaskCounts(
        30,
        4,
        1,
        3
    );
    Assert.assertTrue("Should include taskCountMax when within bounds", contains(cappedCounts, 3));
    Assert.assertFalse("Should not exceed taskCountMax", contains(cappedCounts, 4));

    // Respects taskCountMin - filters out values below the minimum
    // With partitionCount=100, currentTaskCount=10, the computed range includes values like 8, 9, 10, 12, 13
    final int[] minCappedCounts = computeValidTaskCounts(100, 10, 10, 100);
    Assert.assertFalse("Should not include values below taskCountMin (8)", contains(minCappedCounts, 8));
    Assert.assertFalse("Should not include values below taskCountMin (9)", contains(minCappedCounts, 9));
    Assert.assertTrue("Should include values at taskCountMin (10)", contains(minCappedCounts, 10));
    Assert.assertTrue("Should include values above taskCountMin (12)", contains(minCappedCounts, 12));

    // Both bounds applied together
    final int[] bothBounds = computeValidTaskCounts(100, 10, 10, 12);
    Assert.assertFalse("Should not include values below taskCountMin (8)", contains(bothBounds, 8));
    Assert.assertFalse("Should not include values below taskCountMin (9)", contains(bothBounds, 9));
    Assert.assertFalse("Should not include values above taskCountMax (13)", contains(bothBounds, 13));
    Assert.assertTrue("Should include values at taskCountMin (10)", contains(bothBounds, 10));
    Assert.assertTrue("Should include values at taskCountMax (12)", contains(bothBounds, 12));
  }

  @Test
  public void testComputeOptimalTaskCount()
  {
    // Invalid inputs return -1
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(null));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(0.0, 10, 0, 0.0)));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(100.0, 10, -5, 0.3)));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(100.0, -1, 100, 0.3)));

    // Negative pollIdleRatio (metric unavailable) should still allow scaling
    final int unavailableIdleResult = autoScaler.computeOptimalTaskCount(createMetrics(100.0, 25, 100, -1.0));
    MatcherAssert.assertThat(
        "Negative pollIdleRatio should not reject scaling",
        unavailableIdleResult,
        Matchers.greaterThanOrEqualTo(1)
    );

    // High idle (underutilized) - should scale down
    final int scaleDownResult = autoScaler.computeOptimalTaskCount(createMetrics(100.0, 25, 100, 0.8));
    Assert.assertTrue("Expected scale-down when idle ratio is high (>0.6)", scaleDownResult < 25);

    // Very high idle with high task count - should scale down
    final int highIdleResult = autoScaler.computeOptimalTaskCount(createMetrics(10.0, 50, 100, 0.9));
    Assert.assertTrue("High idle should not suggest scale-up", highIdleResult <= 50);

    // With idle below ideal (0.1 < 0.25), U-shaped cost penalizes under-provisioning,
    // driving a moderate scale-up toward the ideal operating point.
    final int lowIdleResult = autoScaler.computeOptimalTaskCount(createMetrics(1000.0, 25, 100, 0.1));
    Assert.assertTrue(
        "Low idle below ideal should drive scale-up toward ideal operating point",
        lowIdleResult > 25
    );
  }

  @Test
  public void testComputeOptimalTaskCountLimitsTaskCountJumps()
  {
    final CostBasedAutoScalerConfig boundedScaleUpConfig = CostBasedAutoScalerConfig.builder()
                                                                                   .taskCountMax(100)
                                                                                   .taskCountMin(1)
                                                                                   .enableTaskAutoScaler(true)
                                                                                   .lagWeight(1.0)
                                                                                   .idleWeight(0.0)
                                                                                   .useTaskCountBoundariesOnScaleUp(true)
                                                                                   .build();
    final CostBasedAutoScaler boundedScaleUpScaler = createAutoScaler(boundedScaleUpConfig);

    Assert.assertEquals(
        "Scale-up should only evaluate two task-count candidates above the current count",
        13,
        boundedScaleUpScaler.computeOptimalTaskCount(createMetrics(100_000.0, 10, 100, 0.25))
    );

    final CostBasedAutoScalerConfig unboundedScaleUpConfig = CostBasedAutoScalerConfig.builder()
                                                                                     .taskCountMax(100)
                                                                                     .taskCountMin(1)
                                                                                     .enableTaskAutoScaler(true)
                                                                                     .lagWeight(1.0)
                                                                                     .idleWeight(0.0)
                                                                                     .build();
    final CostBasedAutoScaler unboundedScaleUpScaler = createAutoScaler(unboundedScaleUpConfig);
    Assert.assertEquals(
        "Without scale-up boundaries, lag-only optimization should jump to max task count",
        100,
        unboundedScaleUpScaler.computeOptimalTaskCount(createMetrics(100_000.0, 10, 100, 0.25))
    );

    final CostBasedAutoScalerConfig boundedScaleDownConfig = CostBasedAutoScalerConfig.builder()
                                                                                     .taskCountMax(100)
                                                                                     .taskCountMin(1)
                                                                                     .enableTaskAutoScaler(true)
                                                                                     .lagWeight(0.0)
                                                                                     .idleWeight(1.0)
                                                                                     .useTaskCountBoundariesOnScaleDown(true)
                                                                                     .build();
    final CostBasedAutoScaler boundedScaleDownScaler = createAutoScaler(boundedScaleDownConfig);

    Assert.assertEquals(
        "Scale-down should only evaluate two task-count candidates below the current count",
        34,
        boundedScaleDownScaler.computeOptimalTaskCount(createMetrics(0.0, 100, 100, 0.9))
    );

    final CostBasedAutoScalerConfig unboundedScaleDownConfig = CostBasedAutoScalerConfig.builder()
                                                                                       .taskCountMax(25)
                                                                                       .taskCountMin(1)
                                                                                       .enableTaskAutoScaler(true)
                                                                                       .lagWeight(0.0)
                                                                                       .idleWeight(1.0)
                                                                                       .build();
    final CostBasedAutoScaler unboundedScaleDownScaler = createAutoScaler(unboundedScaleDownConfig);
    Assert.assertEquals(
        "Without scale-down boundaries, idle-only optimization may select a much lower task count",
        1,
        unboundedScaleDownScaler.computeOptimalTaskCount(createMetrics(0.0, 100, 100, 0.9))
    );
  }

  @Test
  public void testExtractPollIdleRatio()
  {
    // Null and empty return -1 (no data)
    Assert.assertEquals(
        "Null stats should yield -1 idle ratio",
        -1.,
        CostBasedAutoScaler.extractPollIdleRatio(null),
        0.0001
    );
    Assert.assertEquals(
        "Empty stats should yield -1 idle ratio",
        -1.,
        CostBasedAutoScaler.extractPollIdleRatio(Collections.emptyMap()),
        0.0001
    );

    // Missing metrics return -1 (no data)
    Map<String, Map<String, Object>> missingMetrics = new HashMap<>();
    missingMetrics.put("0", Collections.singletonMap("task-0", new HashMap<>()));
    Assert.assertEquals(
        "Missing autoscaler metrics should yield -1 idle ratio",
        -1.,
        CostBasedAutoScaler.extractPollIdleRatio(missingMetrics),
        0.0001
    );

    // Valid stats return average
    Map<String, Map<String, Object>> validStats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", buildTaskStatsWithPollIdle(0.3));
    group.put("task-1", buildTaskStatsWithPollIdle(0.5));
    validStats.put("0", group);
    Assert.assertEquals(
        "Average poll idle ratio should be computed across tasks",
        0.4,
        CostBasedAutoScaler.extractPollIdleRatio(validStats),
        0.0001
    );

    // Invalid types: non-map task metric
    Map<String, Map<String, Object>> nonMapTask = new HashMap<>();
    nonMapTask.put("0", Collections.singletonMap("task-0", "not-a-map"));
    Assert.assertEquals(
        "Non-map task stats should be ignored",
        -1.,
        CostBasedAutoScaler.extractPollIdleRatio(nonMapTask),
        0.0001
    );

    // Invalid types: empty autoscaler metrics
    Map<String, Map<String, Object>> emptyAutoscaler = new HashMap<>();
    Map<String, Object> taskStats1 = new HashMap<>();
    taskStats1.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, new HashMap<>());
    emptyAutoscaler.put("0", Collections.singletonMap("task-0", taskStats1));
    Assert.assertEquals(
        "Empty autoscaler metrics should yield -1 idle ratio",
        -1.,
        CostBasedAutoScaler.extractPollIdleRatio(emptyAutoscaler),
        0.0001
    );

    // Invalid types: non-map autoscaler metrics
    Map<String, Map<String, Object>> nonMapAutoscaler = new HashMap<>();
    Map<String, Object> taskStats2 = new HashMap<>();
    taskStats2.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, "not-a-map");
    nonMapAutoscaler.put("0", Collections.singletonMap("task-0", taskStats2));
    Assert.assertEquals(
        "Non-map autoscaler metrics should be ignored",
        -1.,
        CostBasedAutoScaler.extractPollIdleRatio(nonMapAutoscaler),
        0.0001
    );

    // Invalid types: non-number poll idle ratio
    Map<String, Map<String, Object>> nonNumberRatio = new HashMap<>();
    Map<String, Object> taskStats3 = new HashMap<>();
    Map<String, Object> autoscalerMetrics = new HashMap<>();
    autoscalerMetrics.put(SeekableStreamIndexTaskRunner.POLL_IDLE_RATIO_KEY, "not-a-number");
    taskStats3.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, autoscalerMetrics);
    nonNumberRatio.put("0", Collections.singletonMap("task-0", taskStats3));
    Assert.assertEquals(
        "Non-numeric poll idle ratio should be ignored",
        -1.,
        CostBasedAutoScaler.extractPollIdleRatio(nonNumberRatio),
        0.0001
    );
  }

  @Test
  public void testExtractMovingAverage()
  {
    // Null and empty return -1
    Assert.assertEquals(
        "Null stats should yield -1 moving average",
        -1.,
        CostBasedAutoScaler.extractMovingAverage(null),
        0.0001
    );
    Assert.assertEquals(
        "Empty stats should yield -1 moving average",
        -1.,
        CostBasedAutoScaler.extractMovingAverage(Collections.emptyMap()),
        0.0001
    );

    // Missing metrics return -1
    Map<String, Map<String, Object>> missingMetrics = new HashMap<>();
    missingMetrics.put("0", Collections.singletonMap("task-0", new HashMap<>()));
    Assert.assertEquals(
        "Missing moving averages should yield -1",
        -1.,
        CostBasedAutoScaler.extractMovingAverage(missingMetrics),
        0.0001
    );

    // Valid stats return average (using 5-minute)
    Map<String, Map<String, Object>> validStats = new HashMap<>();
    Map<String, Object> group = new HashMap<>();
    group.put("task-0", buildTaskStatsWithMovingAverage(1000.0));
    group.put("task-1", buildTaskStatsWithMovingAverage(2000.0));
    validStats.put("0", group);
    Assert.assertEquals(
        "Average 5-minute processing rate should be computed across tasks",
        1500.0,
        CostBasedAutoScaler.extractMovingAverage(validStats),
        0.0001
    );

    // Interval fallback: 15-minute preferred, then 5-minute, then 1-minute
    Map<String, Map<String, Object>> fifteenMin = new HashMap<>();
    fifteenMin.put(
        "0",
        Collections.singletonMap(
            "task-0",
            buildTaskStatsWithMovingAverageForInterval(FIFTEEN_MINUTE_NAME, 1500.0)
        )
    );
    Assert.assertEquals(
        "15-minute interval should be preferred when available",
        1500.0,
        CostBasedAutoScaler.extractMovingAverage(fifteenMin),
        0.0001
    );

    // 1-minute as a final fallback
    Map<String, Map<String, Object>> oneMin = new HashMap<>();
    oneMin.put(
        "0",
        Collections.singletonMap("task-0", buildTaskStatsWithMovingAverageForInterval(ONE_MINUTE_NAME, 500.0))
    );
    Assert.assertEquals(
        "1-minute interval should be used as a final fallback",
        500.0,
        CostBasedAutoScaler.extractMovingAverage(oneMin),
        0.0001
    );

    // 15-minute preferred over 5-minute when both available
    Map<String, Map<String, Object>> allIntervals = new HashMap<>();
    allIntervals.put(
        "0",
        Collections.singletonMap("task-0", buildTaskStatsWithMultipleMovingAverages(1500.0, 1000.0, 500.0))
    );
    Assert.assertEquals(
        "15-minute interval should win when multiple intervals are present",
        1500.0,
        CostBasedAutoScaler.extractMovingAverage(allIntervals),
        0.0001
    );

    // Falls back to 5-minute when 15-minute is null
    Map<String, Map<String, Object>> nullFifteen = new HashMap<>();
    nullFifteen.put(
        "0",
        Collections.singletonMap(
            "task-0",
            buildTaskStatsWithNullInterval(FIFTEEN_MINUTE_NAME, FIVE_MINUTE_NAME, 750.0)
        )
    );
    Assert.assertEquals(
        "Should fall back to 5-minute when 15-minute is null",
        750.0,
        CostBasedAutoScaler.extractMovingAverage(nullFifteen),
        0.0001
    );

    // Falls back to 1-minute when both 15 and 5 are null
    Map<String, Map<String, Object>> bothNull = new HashMap<>();
    bothNull.put("0", Collections.singletonMap("task-0", buildTaskStatsWithTwoNullIntervals(250.0)));
    Assert.assertEquals(
        "Should fall back to 1-minute when 15 and 5 are null",
        250.0,
        CostBasedAutoScaler.extractMovingAverage(bothNull),
        0.0001
    );
  }

  @Test
  public void testExtractMovingAverageInvalidTypes()
  {
    // Non-map task metric
    Map<String, Map<String, Object>> nonMapTask = new HashMap<>();
    nonMapTask.put("0", Collections.singletonMap("task-0", "not-a-map"));
    Assert.assertEquals(
        "Non-map task stats should be ignored",
        -1.,
        CostBasedAutoScaler.extractMovingAverage(nonMapTask),
        0.0001
    );

    Map<String, Map<String, Object>> missingBuild = new HashMap<>();
    Map<String, Object> taskStats1 = new HashMap<>();
    taskStats1.put("movingAverages", new HashMap<>());
    missingBuild.put("0", Collections.singletonMap("task-0", taskStats1));
    Assert.assertEquals(
        "Missing buildSegments moving average should yield -1",
        -1.,
        CostBasedAutoScaler.extractMovingAverage(missingBuild),
        0.0001
    );

    Map<String, Map<String, Object>> nonMapMA = new HashMap<>();
    Map<String, Object> taskStats2 = new HashMap<>();
    taskStats2.put("movingAverages", "not-a-map");
    nonMapMA.put("0", Collections.singletonMap("task-0", taskStats2));
    Assert.assertEquals(
        "Non-map movingAverages should be ignored",
        -1.,
        CostBasedAutoScaler.extractMovingAverage(nonMapMA),
        0.0001
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
    when(spec.getDataSources()).thenReturn(List.of("test-datasource"));
    when(supervisor.getIoConfig()).thenReturn(ioConfig);
    when(ioConfig.getStream()).thenReturn("stream");

    CostBasedAutoScalerConfig cfgWithDefaults = CostBasedAutoScalerConfig.builder()
                                                                         .taskCountMax(10)
                                                                         .taskCountMin(1)
                                                                         .enableTaskAutoScaler(true)
                                                                         .build();
    Assert.assertEquals(
        CostBasedAutoScalerConfig.DEFAULT_MIN_SCALE_DELAY,
        cfgWithDefaults.getMinScaleDownDelay()
    );
    Assert.assertFalse(cfgWithDefaults.isScaleDownOnTaskRolloverOnly());

    // Test custom config values
    CostBasedAutoScalerConfig cfgWithCustom = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(10)
                                                                       .taskCountMin(1)
                                                                       .enableTaskAutoScaler(true)
                                                                       .minScaleDownDelay(Duration.standardMinutes(10))
                                                                       .scaleDownDuringTaskRolloverOnly(true)
                                                                       .build();
    Assert.assertEquals(Duration.standardMinutes(10), cfgWithCustom.getMinScaleDownDelay());
    Assert.assertTrue(cfgWithCustom.isScaleDownOnTaskRolloverOnly());

    // computeTaskCountForRollover returns -1 when scaleDownDuringTaskRolloverOnly=false (default)
    when(spec.isSuspended()).thenReturn(false);
    CostBasedAutoScaler scaler = new CostBasedAutoScaler(supervisor, cfgWithDefaults, spec, emitter);
    Assert.assertEquals(-1, scaler.computeTaskCountForRollover());

    // computeTaskCountForRollover returns -1 when lastKnownMetrics is null (even with scaleDownDuringTaskRolloverOnly=true)
    CostBasedAutoScaler scalerWithRolloverOnly = new CostBasedAutoScaler(supervisor, cfgWithCustom, spec, emitter);
    Assert.assertEquals(-1, scalerWithRolloverOnly.computeTaskCountForRollover());
  }

  @Test
  public void testScalingActionSkippedWhenMovingAverageRateUnavailable()
  {
    SupervisorSpec spec = Mockito.mock(SupervisorSpec.class);
    SeekableStreamSupervisor supervisor = Mockito.mock(SeekableStreamSupervisor.class);
    ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    SeekableStreamSupervisorIOConfig ioConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(spec.getId()).thenReturn("test-supervisor");
    when(spec.getDataSources()).thenReturn(List.of("test-datasource"));
    when(spec.isSuspended()).thenReturn(false);
    when(supervisor.getIoConfig()).thenReturn(ioConfig);
    when(ioConfig.getStream()).thenReturn("test-stream");
    when(ioConfig.getTaskDuration()).thenReturn(Duration.standardHours(1));
    when(ioConfig.getTaskCount()).thenReturn(1);
    when(supervisor.computeLagStats()).thenReturn(new LagStats(100, 100, 100));
    // No task stats means the moving average rate is unavailable
    when(supervisor.getStats()).thenReturn(Collections.emptyMap());

    CostBasedAutoScalerConfig config = CostBasedAutoScalerConfig.builder()
                                                                .taskCountMax(10)
                                                                .taskCountMin(1)
                                                                .enableTaskAutoScaler(true)
                                                                .build();
    CostBasedAutoScaler scaler = new CostBasedAutoScaler(supervisor, config, spec, emitter);

    Assert.assertEquals(
        "No scaling action should be requested when the moving average rate is unavailable",
        -1,
        scaler.computeTaskCountForScaleAction()
    );
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

  private CostBasedAutoScaler createAutoScaler(CostBasedAutoScalerConfig config)
  {
    final SupervisorSpec mockSupervisorSpec = Mockito.mock(SupervisorSpec.class);
    final SeekableStreamSupervisor mockSupervisor = Mockito.mock(SeekableStreamSupervisor.class);
    final ServiceEmitter mockEmitter = Mockito.mock(ServiceEmitter.class);
    final SeekableStreamSupervisorIOConfig mockIoConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(mockSupervisorSpec.getId()).thenReturn("test-supervisor");
    when(mockSupervisorSpec.getDataSources()).thenReturn(List.of("test-datasource"));
    when(mockSupervisor.getIoConfig()).thenReturn(mockIoConfig);
    when(mockIoConfig.getStream()).thenReturn("test-stream");

    return new CostBasedAutoScaler(mockSupervisor, config, mockSupervisorSpec, mockEmitter);
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
    buildSegments.put(FIVE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, processedRate));

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
    buildSegments.put(FIFTEEN_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, fifteenMinRate));
    buildSegments.put(FIVE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, fiveMinRate));
    buildSegments.put(ONE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, oneMinRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }

  private Map<String, Object> buildTaskStatsWithNullInterval(
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

  private Map<String, Object> buildTaskStatsWithTwoNullIntervals(double oneMinRate)
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
