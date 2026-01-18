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

import java.util.Collections;

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
    // Setup: current = 10, optimal should be higher due to high lag and low idle
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 10;
    int expectedOptimalCount = 17; // Higher than current

    CostMetrics metrics = createMetrics(5000.0, currentTaskCount, PARTITION_COUNT, 0.1);

    doReturn(expectedOptimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(currentTaskCount, 5000.0, 0.1);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should return optimal count when it's greater than current (scale-up)",
        expectedOptimalCount,
        result
    );
  }

  @Test
  public void testNoOpWhenOptimalEqualsCurrent()
  {
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 25;
    int optimalCount = 25; // Same as current

    doReturn(optimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(currentTaskCount, 100.0, 0.5);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should return optimal count when it equals current (no change needed)",
        optimalCount,
        result
    );
  }

  @Test
  public void testScaleDownBlockedReturnsMinusOne()
  {
    // Scale-down is blocked in computeTaskCountForScaleAction
    CostBasedAutoScaler autoScaler = spy(new CostBasedAutoScaler(mockSupervisor, config, mockSpec, mockEmitter));

    int currentTaskCount = 50;
    int optimalCount = 30; // Lower than current (scale-down scenario)

    doReturn(optimalCount).when(autoScaler).computeOptimalTaskCount(any());
    setupMocksForMetricsCollection(currentTaskCount, 10.0, 0.9);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should return -1 when optimal is less than current (scale-down blocked)",
        -1,
        result
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
    setupMocksForMetricsCollection(currentTaskCount, 100.0, 0.5);

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
    setupMocksForMetricsCollection(currentTaskCount, 100.0, 0.5);

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
    setupMocksForMetricsCollection(currentTaskCount, 10000.0, 0.0);

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
    setupMocksForMetricsCollection(currentTaskCount, 50000.0, 0.0);

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
    setupMocksForMetricsCollection(currentTaskCount, 1000.0, 0.2);

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
    setupMocksForMetricsCollection(currentTaskCount, 10.0, 0.8);

    int result = autoScaler.computeTaskCountForScaleAction();

    Assert.assertEquals(
        "Should block scale-down even by one task",
        -1,
        result
    );
  }

  private void setupMocksForMetricsCollection(int taskCount, double avgLag, double pollIdleRatio)
  {
    when(mockSupervisor.computeLagStats()).thenReturn(new LagStats(0, (long) avgLag * 2, (long) avgLag));
    when(mockIoConfig.getTaskCount()).thenReturn(taskCount);
    when(mockSupervisor.getPartitionCount()).thenReturn(PARTITION_COUNT);
    when(mockSupervisor.getStats()).thenReturn(Collections.emptyMap());
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
