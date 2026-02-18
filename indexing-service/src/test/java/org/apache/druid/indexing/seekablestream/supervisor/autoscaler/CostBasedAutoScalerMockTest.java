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
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisor;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.joda.time.Duration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
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

  private void setupMocksForMetricsCollection(CostBasedAutoScaler autoScaler, int taskCount, double avgLag, double pollIdleRatio)
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
        TASK_DURATION_SECONDS,
        AVG_PROCESSING_RATE
    );
  }
}
