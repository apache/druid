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

    // Edge cases
    Assert.assertEquals("Zero partitions return empty array", 0, CostBasedAutoScaler.computeValidTaskCounts(0, 10).length);
    Assert.assertEquals("Negative partitions return empty array", 0, CostBasedAutoScaler.computeValidTaskCounts(-5, 10).length);

    // Single partition
    int[] singlePartition = CostBasedAutoScaler.computeValidTaskCounts(1, 1);
    Assert.assertTrue("Single partition should have at least one valid count", singlePartition.length > 0);
    Assert.assertTrue("Single partition should contain 1", contains(singlePartition, 1));

    // Current exceeds partitions - should still yield valid, deduplicated options
    int[] exceedsPartitions = CostBasedAutoScaler.computeValidTaskCounts(2, 5);
    Assert.assertEquals(2, exceedsPartitions.length);
    Assert.assertTrue(contains(exceedsPartitions, 1));
    Assert.assertTrue(contains(exceedsPartitions, 2));
  }

  @Test
  public void testComputeOptimalTaskCountInvalidInputs()
  {
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(null));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(0.0, 10, 0, 0.0)));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(100.0, 10, -5, 0.3)));
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(createMetrics(100.0, -1, 100, 0.3)));
  }

  @Test
  public void testComputeOptimalTaskCountScaling()
  {
    // High idle (underutilized) - should scale down
    int scaleDownResult = autoScaler.computeOptimalTaskCount(createMetrics(100.0, 25, 100, 0.8));
    Assert.assertTrue("Should scale down when idle > 0.6", scaleDownResult < 25);

    // Very high idle with high task count - should scale down
    int highIdleResult = autoScaler.computeOptimalTaskCount(createMetrics(10.0, 50, 100, 0.9));
    Assert.assertTrue("Scale down scenario should return optimal <= current", highIdleResult <= 50);

    // With low idle and balanced weights, algorithm should not scale up aggressively
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
  }

  @Test
  public void testExtractPollIdleRatioInvalidTypes()
  {
    // Non-map task metric
    Map<String, Map<String, Object>> nonMapTask = new HashMap<>();
    nonMapTask.put("0", Collections.singletonMap("task-0", "not-a-map"));
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(nonMapTask), 0.0001);

    // Empty autoscaler metrics
    Map<String, Map<String, Object>> emptyAutoscaler = new HashMap<>();
    Map<String, Object> taskStats1 = new HashMap<>();
    taskStats1.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, new HashMap<>());
    emptyAutoscaler.put("0", Collections.singletonMap("task-0", taskStats1));
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(emptyAutoscaler), 0.0001);

    // Non-map autoscaler metrics
    Map<String, Map<String, Object>> nonMapAutoscaler = new HashMap<>();
    Map<String, Object> taskStats2 = new HashMap<>();
    taskStats2.put(SeekableStreamIndexTaskRunner.AUTOSCALER_METRICS_KEY, "not-a-map");
    nonMapAutoscaler.put("0", Collections.singletonMap("task-0", taskStats2));
    Assert.assertEquals(0., CostBasedAutoScaler.extractPollIdleRatio(nonMapAutoscaler), 0.0001);

    // Non-number poll idle ratio
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
  }

  @Test
  public void testExtractMovingAverageIntervalFallback()
  {
    // 15-minute average is preferred
    Map<String, Map<String, Object>> fifteenMin = new HashMap<>();
    fifteenMin.put("0", Collections.singletonMap("task-0", buildTaskStatsWithMovingAverageForInterval(FIFTEEN_MINUTE_NAME, 1500.0)));
    Assert.assertEquals(1500.0, CostBasedAutoScaler.extractMovingAverage(fifteenMin), 0.0001);

    // 1-minute as final fallback
    Map<String, Map<String, Object>> oneMin = new HashMap<>();
    oneMin.put("0", Collections.singletonMap("task-0", buildTaskStatsWithMovingAverageForInterval(ONE_MINUTE_NAME, 500.0)));
    Assert.assertEquals(500.0, CostBasedAutoScaler.extractMovingAverage(oneMin), 0.0001);

    // 15-minute preferred over 5-minute when both available
    Map<String, Map<String, Object>> allIntervals = new HashMap<>();
    allIntervals.put("0", Collections.singletonMap("task-0", buildTaskStatsWithMultipleMovingAverages(1500.0, 1000.0, 500.0)));
    Assert.assertEquals(1500.0, CostBasedAutoScaler.extractMovingAverage(allIntervals), 0.0001);

    // Falls back to 5-minute when 15-minute is null
    Map<String, Map<String, Object>> nullFifteen = new HashMap<>();
    nullFifteen.put("0", Collections.singletonMap("task-0", buildTaskStatsWithNullInterval(FIFTEEN_MINUTE_NAME, FIVE_MINUTE_NAME, 750.0)));
    Assert.assertEquals(750.0, CostBasedAutoScaler.extractMovingAverage(nullFifteen), 0.0001);

    // Falls back to 1-minute when both 15 and 5 are null
    Map<String, Map<String, Object>> bothNull = new HashMap<>();
    bothNull.put("0", Collections.singletonMap("task-0", buildTaskStatsWithTwoNullIntervals(250.0)));
    Assert.assertEquals(250.0, CostBasedAutoScaler.extractMovingAverage(bothNull), 0.0001);
  }

  @Test
  public void testExtractMovingAverageInvalidTypes()
  {
    // Non-map task metric
    Map<String, Map<String, Object>> nonMapTask = new HashMap<>();
    nonMapTask.put("0", Collections.singletonMap("task-0", "not-a-map"));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(nonMapTask), 0.0001);

    // Missing buildSegments
    Map<String, Map<String, Object>> missingBuild = new HashMap<>();
    Map<String, Object> taskStats1 = new HashMap<>();
    taskStats1.put("movingAverages", new HashMap<>());
    missingBuild.put("0", Collections.singletonMap("task-0", taskStats1));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(missingBuild), 0.0001);

    // Non-map movingAverages
    Map<String, Map<String, Object>> nonMapMA = new HashMap<>();
    Map<String, Object> taskStats2 = new HashMap<>();
    taskStats2.put("movingAverages", "not-a-map");
    nonMapMA.put("0", Collections.singletonMap("task-0", taskStats2));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(nonMapMA), 0.0001);

    // Non-map buildSegments
    Map<String, Map<String, Object>> nonMapBS = new HashMap<>();
    Map<String, Object> taskStats3 = new HashMap<>();
    Map<String, Object> movingAverages3 = new HashMap<>();
    movingAverages3.put(RowIngestionMeters.BUILD_SEGMENTS, "not-a-map");
    taskStats3.put("movingAverages", movingAverages3);
    nonMapBS.put("0", Collections.singletonMap("task-0", taskStats3));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(nonMapBS), 0.0001);

    // Non-map interval data
    Map<String, Map<String, Object>> nonMapInterval = new HashMap<>();
    Map<String, Object> taskStats4 = new HashMap<>();
    Map<String, Object> movingAverages4 = new HashMap<>();
    Map<String, Object> buildSegments4 = new HashMap<>();
    buildSegments4.put(FIFTEEN_MINUTE_NAME, "not-a-map");
    movingAverages4.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments4);
    taskStats4.put("movingAverages", movingAverages4);
    nonMapInterval.put("0", Collections.singletonMap("task-0", taskStats4));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(nonMapInterval), 0.0001);

    // Non-number processed rate
    Map<String, Map<String, Object>> nonNumberRate = new HashMap<>();
    Map<String, Object> taskStats5 = new HashMap<>();
    Map<String, Object> movingAverages5 = new HashMap<>();
    Map<String, Object> buildSegments5 = new HashMap<>();
    Map<String, Object> fifteenMin = new HashMap<>();
    fifteenMin.put(RowIngestionMeters.PROCESSED, "not-a-number");
    buildSegments5.put(FIFTEEN_MINUTE_NAME, fifteenMin);
    movingAverages5.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments5);
    taskStats5.put("movingAverages", movingAverages5);
    nonNumberRate.put("0", Collections.singletonMap("task-0", taskStats5));
    Assert.assertEquals(-1., CostBasedAutoScaler.extractMovingAverage(nonNumberRate), 0.0001);
  }

  @Test
  public void testComputeTaskCountForRolloverReturnsMinusOneWhenSuspended()
  {
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
    Assert.assertEquals(-1, scaler.computeTaskCountForRollover());
  }

  @Test
  public void testComputeTaskCountForRolloverReturnsMinusOneWhenLagStatsNull()
  {
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
    Assert.assertEquals(-1, scaler.computeTaskCountForRollover());
  }

  @Test
  public void testComputeTaskCountForRolloverReturnsMinusOneWhenNoMetrics()
  {
    // Tests the case where lastKnownMetrics is null (no computeTaskCountForScaleAction called)
    SupervisorSpec spec = Mockito.mock(SupervisorSpec.class);
    SeekableStreamSupervisor supervisor = Mockito.mock(SeekableStreamSupervisor.class);
    ServiceEmitter emitter = Mockito.mock(ServiceEmitter.class);
    SeekableStreamSupervisorIOConfig ioConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(spec.getId()).thenReturn("s-up");
    when(spec.isSuspended()).thenReturn(false);
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
    // Should return -1 when lastKnownMetrics is null
    Assert.assertEquals(-1, scaler.computeTaskCountForRollover());
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

  private Map<String, Object> buildTaskStatsWithNullInterval(String nullInterval, String validInterval, double processedRate)
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
