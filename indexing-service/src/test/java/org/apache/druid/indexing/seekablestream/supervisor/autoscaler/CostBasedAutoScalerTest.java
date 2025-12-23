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

import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMeters;
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
    buildSegments.put(DropwizardRowIngestionMeters.FIVE_MINUTE_NAME, Map.of(RowIngestionMeters.PROCESSED, processedRate));

    Map<String, Object> movingAverages = new HashMap<>();
    movingAverages.put(RowIngestionMeters.BUILD_SEGMENTS, buildSegments);

    Map<String, Object> taskStats = new HashMap<>();
    taskStats.put("movingAverages", movingAverages);
    return taskStats;
  }
}
