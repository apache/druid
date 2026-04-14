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
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEventBuilder;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CostBasedAutoScalerMockTest
{
  private static final String SUPERVISOR_ID = "test-supervisor";
  private static final String STREAM_NAME = "test-stream";
  private static final int PARTITION_COUNT = 100;
  private static final long TASK_DURATION_SECONDS = 3600;
  private static final double AVG_PROCESSING_RATE = 1000.0;

  private SupervisorSpec mockSpec;
  private SeekableStreamSupervisor mockSupervisor;
  private ServiceEmitter mockEmitter;
  private SeekableStreamSupervisorIOConfig mockIoConfig;
  private CostBasedAutoScalerConfig config;

  @Before
  public void setUp()
  {
    mockSpec = Mockito.mock(SupervisorSpec.class);
    mockSupervisor = Mockito.mock(SeekableStreamSupervisor.class);
    mockEmitter = Mockito.mock(ServiceEmitter.class);
    mockIoConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(mockSpec.getId()).thenReturn(SUPERVISOR_ID);
    when(mockSpec.getDataSources()).thenReturn(List.of("test-datasource"));
    when(mockSpec.isSuspended()).thenReturn(false);
    when(mockSupervisor.getIoConfig()).thenReturn(mockIoConfig);
    when(mockIoConfig.getStream()).thenReturn(STREAM_NAME);
    when(mockIoConfig.getTaskDuration()).thenReturn(Duration.standardSeconds(TASK_DURATION_SECONDS));

    config = CostBasedAutoScalerConfig.builder()
                                      .taskCountMax(100)
                                      .taskCountMin(1)
                                      .enableTaskAutoScaler(true)
                                      .lagWeight(0.6)
                                      .idleWeight(0.4)
                                      .build();
  }

  @Test
  public void testScaleUpWhenOptimalGreaterThanCurrent()
  {
    // Use config with a long barrier to test cooldown behavior
    CostBasedAutoScalerConfig barrierConfig = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(100)
                                                                       .taskCountMin(1)
                                                                       .enableTaskAutoScaler(true)
                                                                       .minScaleDownDelay(Duration.standardHours(1))
                                                                       .build();

    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(
        mockSupervisor,
        barrierConfig,
        mockSpec,
        mockEmitter
    ));

    int currentTaskCount = 10;
    int scaleUpOptimal = 17;
    // Trigger scale-up, which should set the cooldown timer
    doReturn(scaleUpOptimal).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 5000.0, 0.1);

    Assert.assertEquals(
        "Should return optimal count when it's greater than current (scale-up)",
        scaleUpOptimal,
        autoScaler.computeTaskCountForScaleAction()
    );

    // Verify cooldown blocks immediate subsequent scaling
    doReturn(scaleUpOptimal).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 10.0, 0.9);
    Assert.assertEquals(
        "Scale action should be blocked during the cooldown window",
        -1,
        autoScaler.computeTaskCountForScaleAction()
    );
  }

  @Test
  public void testNoOpWhenOptimalEqualsCurrent()
  {
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 25;
    int optimalCount = 25; // Same as current

    doReturn(optimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 100.0, 0.5);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals("Should return -1 when it equals current (no change needed)", -1, result);
  }

  @Test
  public void testScaleDownBlockedReturnsMinusOne()
  {
    // Use config with a long barrier to test cooldown behavior
    CostBasedAutoScalerConfig barrierConfig = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(100)
                                                                       .taskCountMin(1)
                                                                       .enableTaskAutoScaler(true)
                                                                       .minScaleDownDelay(Duration.standardHours(1))
                                                                       .build();

    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(
        mockSupervisor,
        barrierConfig,
        mockSpec,
        mockEmitter
    ));

    int currentTaskCount = 50;
    int optimalCount = 30; // Lower than current (scale-down scenario)

    doReturn(optimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 10.0, 0.9);

    // First attempt: allowed (no prior scale action)
    Assert.assertEquals(
        "Scale-down should succeed when no prior scale action exists",
        optimalCount,
        autoScaler.computeTaskCountForScaleAction()
    );

    // Second attempt: blocked by cooldown
    Assert.assertEquals(
        "Scale-down should be blocked during the cooldown window",
        -1,
        autoScaler.computeTaskCountForScaleAction()
    );
  }

  @Test
  public void testReturnsMinusOneWhenMetricsCollectionFails()
  {
    // When the supervisor is suspended, collectMetrics returns null which causes
    // computeOptimalTaskCount to return -1
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 10;

    // Mock computeOptimalTaskCount to return -1 (simulating null metrics scenario)
    doReturn(-1).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 100.0, 0.5);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should return -1 when computeOptimalTaskCount returns -1 (e.g., due to invalid metrics)",
        -1,
        result
    );
  }

  @Test
  public void testReturnsMinusOneWhenLagStatsNull()
  {
    // When lag stats are null, computeOptimalTaskCount returns -1
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 10;

    // Mock computeOptimalTaskCount to return -1 (simulating null lag stats scenario)
    doReturn(-1).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 100.0, 0.5);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should return -1 when computeOptimalTaskCount returns -1 (e.g., due to null lag stats)",
        -1,
        result
    );
  }

  @Test
  public void testScaleUpFromMinimumTasks()
  {
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 1;
    int expectedOptimalCount = 5;

    doReturn(expectedOptimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 10000.0, 0.0);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should allow scale-up from the minimum task count",
        expectedOptimalCount,
        result
    );
  }

  @Test
  public void testReturnsTaskCountMinWhenConfiguredTaskCountIsBelowMin()
  {
    CostBasedAutoScalerConfig boundedConfig = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(100)
                                                                       .taskCountMin(50)
                                                                       .enableTaskAutoScaler(true)
                                                                       .build();
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, boundedConfig, mockSpec, mockEmitter));

    final int configuredTaskCount = 1;
    final int taskCountMin = 50;

    // Mock computeOptimalTaskCount to return a value different from the boundary,
    // so the assertion proves the boundary clamping path was taken.
    doReturn(taskCountMin - 1).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, configuredTaskCount, 1000.0, 0.2);

    final int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should scale to taskCountMin when the configured task count is below the minimum boundary",
        taskCountMin,
        result
    );
  }

  @Test
  public void testReturnsTaskCountMaxWhenConfiguredTaskCountIsAboveMax()
  {
    CostBasedAutoScalerConfig boundedConfig = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(50)
                                                                       .taskCountMin(1)
                                                                       .enableTaskAutoScaler(true)
                                                                       .build();
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, boundedConfig, mockSpec, mockEmitter));

    final int configuredTaskCount = 100;
    final int taskCountMax = 50;

    // Mock computeOptimalTaskCount to return a value different from the boundary,
    // so the assertion proves the boundary clamping path was taken.
    doReturn(taskCountMax + 1).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, configuredTaskCount, 10.0, 0.8);

    final int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should scale to taskCountMax when the configured task count is above the maximum boundary",
        taskCountMax,
        result
    );
  }

  @Test
  public void testScaleUpToMaximumTasks()
  {
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 90;
    int expectedOptimalCount = 100; // Maximum allowed

    doReturn(expectedOptimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 50000.0, 0.0);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should allow scale-up to maximum task count",
        expectedOptimalCount,
        result
    );
  }

  @Test
  public void testBoundaryConditionOptimalEqualsCurrentPlusOne()
  {
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 25;
    int expectedOptimalCount = 26; // Just one more than current

    doReturn(expectedOptimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 1000.0, 0.2);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should allow scale-up by exactly one task",
        expectedOptimalCount,
        result
    );
  }

  @Test
  public void testBoundaryConditionOptimalEqualsCurrentMinusOne()
  {
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 25;
    int optimalCount = 24; // Just one less than current

    doReturn(optimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 10.0, 0.8);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should allow scale-down by one task when cooldown has elapsed",
        optimalCount,
        result
    );
  }

  @Test
  public void testScaleDownBlockedWhenScaleDownOnRolloverOnlyEnabled()
  {
    CostBasedAutoScalerConfig rolloverOnlyConfig = CostBasedAutoScalerConfig.builder()
                                                                            .taskCountMax(100)
                                                                            .taskCountMin(1)
                                                                            .enableTaskAutoScaler(true)
                                                                            .scaleDownDuringTaskRolloverOnly(true)
                                                                            .minScaleDownDelay(Duration.ZERO)
                                                                            .build();

    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(
        mockSupervisor,
        rolloverOnlyConfig,
        mockSpec,
        mockEmitter
    ));

    int currentTaskCount = 50;
    int optimalCount = 30; // Lower than current (scale-down scenario)

    doReturn(optimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 10.0, 0.9);

    Assert.assertEquals(
        "Should return -1 when scaleDownDuringTaskRolloverOnly is true",
        -1,
        autoScaler.computeTaskCountForScaleAction()
    );
  }

  @Test
  public void testScaleDownAllowedDuringRolloverWhenScaleDownOnRolloverOnlyEnabled()
  {
    CostBasedAutoScalerConfig rolloverOnlyConfig = CostBasedAutoScalerConfig.builder()
                                                                            .taskCountMax(100)
                                                                            .taskCountMin(1)
                                                                            .enableTaskAutoScaler(true)
                                                                            .scaleDownDuringTaskRolloverOnly(true)
                                                                            .minScaleDownDelay(Duration.ZERO)
                                                                            .build();

    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(
        mockSupervisor,
        rolloverOnlyConfig,
        mockSpec,
        mockEmitter
    ));

    int currentTaskCount = 50;
    int optimalCount = 30;

    // Set up lastKnownMetrics by calling computeTaskCountForScaleAction first without scaling
    doReturn(currentTaskCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 10.0, 0.9);
    autoScaler.computeTaskCountForScaleAction(); // This populates lastKnownMetrics

    doReturn(optimalCount).when(autoScaler).computeOptimalTaskCount(any());
    Assert.assertEquals(
        "Should scale-down during rollover when scaleDownDuringTaskRolloverOnly is true",
        optimalCount,
        autoScaler.computeTaskCountForRollover()
    );
  }

  @Test
  public void testEmitsMaxTaskCountSkipReasonWhenCurrentIsAtMax()
  {
    CostBasedAutoScalerConfig boundedConfig = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(10)
                                                                       .taskCountMin(1)
                                                                       .enableTaskAutoScaler(true)
                                                                       .build();
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, boundedConfig, mockSpec, mockEmitter));

    final int currentTaskCount = 10; // already at max
    doReturn(-1).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 100.0, 0.5);

    Assert.assertEquals(-1, autoScaler.computeTaskCountForScaleAction());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<ServiceEventBuilder<ServiceMetricEvent>> captor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    verify(mockEmitter).emit(captor.capture());
    Assert.assertEquals(
        "Should emit 'Already at max task count' skip reason when current task count is at maximum",
        "Already at max task count",
        ((ServiceMetricEvent.Builder) captor.getValue())
            .getDimension(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION)
    );
  }

  @Test
  public void testEmitsMinTaskCountSkipReasonWhenCurrentIsAtMin()
  {
    CostBasedAutoScalerConfig boundedConfig = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(100)
                                                                       .taskCountMin(10)
                                                                       .enableTaskAutoScaler(true)
                                                                       .build();
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, boundedConfig, mockSpec, mockEmitter));

    final int currentTaskCount = 10; // already at min
    doReturn(-1).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 100.0, 0.5);

    Assert.assertEquals(-1, autoScaler.computeTaskCountForScaleAction());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<ServiceEventBuilder<ServiceMetricEvent>> captor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    verify(mockEmitter).emit(captor.capture());
    Assert.assertEquals(
        "Should emit 'Already at min task count' skip reason when current task count is at minimum",
        "Already at min task count",
        ((ServiceMetricEvent.Builder) captor.getValue())
            .getDimension(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION)
    );
  }

  @Test
  public void testMaxSkipReasonTakesPriorityWhenMinEqualsMax()
  {
    // When min == max, current is simultaneously at both bounds.
    // The comment in the production code states that signaling max has higher priority.
    CostBasedAutoScalerConfig boundedConfig = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(5)
                                                                       .taskCountMin(5)
                                                                       .enableTaskAutoScaler(true)
                                                                       .build();
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, boundedConfig, mockSpec, mockEmitter));

    final int currentTaskCount = 5; // at both min and max
    doReturn(-1).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(autoScaler, currentTaskCount, 100.0, 0.5);

    Assert.assertEquals(-1, autoScaler.computeTaskCountForScaleAction());

    @SuppressWarnings("unchecked")
    ArgumentCaptor<ServiceEventBuilder<ServiceMetricEvent>> captor = ArgumentCaptor.forClass(ServiceEventBuilder.class);
    verify(mockEmitter).emit(captor.capture());
    Assert.assertEquals(
        "Max skip reason should take priority over min skip reason when min equals max",
        "Already at max task count",
        ((ServiceMetricEvent.Builder) captor.getValue())
            .getDimension(SeekableStreamSupervisor.AUTOSCALER_SKIP_REASON_DIMENSION)
    );
  }

  private void setupMocksForMetricsCollection(
      CostBasedAutoScaler autoScaler,
      int taskCount,
      double avgLag,
      double pollIdleRatio
  )
  {
    CostMetrics metrics = new CostMetrics(
        avgLag,
        taskCount,
        PARTITION_COUNT,
        pollIdleRatio,
        TASK_DURATION_SECONDS,
        AVG_PROCESSING_RATE
    );
    doReturn(metrics).when(autoScaler).collectMetrics();

    SeekableStreamSupervisorIOConfig ioConfig = mock(SeekableStreamSupervisorIOConfig.class);
    doReturn(ioConfig).when(mockSupervisor).getIoConfig();
    doReturn(taskCount).when(ioConfig).getTaskCount();
    doReturn(STREAM_NAME).when(ioConfig).getStream();
  }

}
