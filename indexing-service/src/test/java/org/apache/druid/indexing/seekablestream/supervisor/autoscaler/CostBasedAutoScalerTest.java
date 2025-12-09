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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.when;

public class CostBasedAutoScalerTest
{
  private CostBasedAutoScaler autoScaler;
  private SeekableStreamSupervisor mockSupervisor;
  private ServiceEmitter mockEmitter;
  private CostBasedAutoScalerConfig config;

  @Before
  public void setUp()
  {
    mockSupervisor = Mockito.mock(SeekableStreamSupervisor.class);
    mockEmitter = Mockito.mock(ServiceEmitter.class);

    SeekableStreamSupervisorIOConfig mockIoConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);
    when(mockSupervisor.getIoConfig()).thenReturn(mockIoConfig);
    when(mockIoConfig.getStream()).thenReturn("test-stream");

    config = CostBasedAutoScalerConfig.builder()
                                      .taskCountMax(100)
                                      .taskCountMin(1)
                                      .enableTaskAutoScaler(true)
                                      .taskCountStart(10)
                                      .minTriggerScaleActionFrequencyMillis(600000L)
                                      .metricsCollectionIntervalMillis(30000L)
                                      .scaleActionStartDelayMillis(300000L)
                                      .scaleActionPeriodMillis(60000L)
                                      .lagWeight(0.6)
                                      .idleWeight(0.4)
                                      .build();

    autoScaler = new CostBasedAutoScaler(
        mockSupervisor,
        "test-datasource",
        config,
        Mockito.mock(SupervisorSpec.class),
        mockEmitter
    );
  }

  @Test
  public void testComputeFactorsGradualScaling()
  {
    // Verify gradual scaling: for 100 partitions, going from 25 tasks (4 partitions/task)
    // the next step should be 34 tasks (3 partitions/task)
    List<Integer> factors = autoScaler.computeFactors(100);

    int idx25 = factors.indexOf(25);
    int idx34 = factors.indexOf(34);
    Assert.assertTrue("25 should be in factors", idx25 >= 0);
    Assert.assertTrue("34 should be in factors", idx34 >= 0);
    Assert.assertEquals("34 should be the next factor after 25", idx25 + 1, idx34);
  }

  @Test
  public void testComputeOptimalTaskCountInvalidInputs()
  {
    // Empty metrics list
    int result = autoScaler.computeOptimalTaskCount(new AtomicReference<>(null));
    Assert.assertEquals(-1, result);

    // Zero partitions
    CostMetrics zeroPartitionsMetrics = new CostMetrics(
        System.currentTimeMillis(),
        0.0,  // avgPartitionLag
        10,   // currentTaskCount
        0,    // partitionCount = 0
        0.0   // pollIdleRatio
    );

    result = autoScaler.computeOptimalTaskCount(new AtomicReference<>(zeroPartitionsMetrics));
    Assert.assertEquals(-1, result);
  }

  @Test
  public void testComputeOptimalTaskCountLowIdleScenario_scaleUpGradually()
  {
    // Very high lag scenario - algorithm should recommend scaling up
    CostMetrics oldMetrics = new CostMetrics(
        System.currentTimeMillis(),
        300.0,  // avgPartitionLag - low
        100,         // currentTaskCount
        100,        // partitionCount
        0.001       // pollIdleRatio - low idle (tasks are busy)
    );

    int initialResult = autoScaler.computeOptimalTaskCount(new AtomicReference<>(oldMetrics));

    // Expect -1 since we're scaling down, but config should contain 34 now.
    Assert.assertEquals(-1, initialResult);
    Assert.assertNotNull(config.getTaskCountStart());
    Assert.assertEquals(34, config.getTaskCountStart().intValue());

    CostMetrics newMetrics = new CostMetrics(
        System.currentTimeMillis(),
        400.0,  // avgPartitionLag - moderate lag for gradual scaling
        34,         // currentTaskCount
        100,        // partitionCount
        0.001       // pollIdleRatio - low idle (tasks are busy)
    );

    int result = autoScaler.computeOptimalTaskCount(new AtomicReference<>(newMetrics));
    // With relatively high lag and very low idle, the algorithm should recommend scaling up
    Assert.assertEquals(50, result);
  }

  @Test
  public void testComputeOptimalTaskCountHighLagScenario_scaleUpAggressively()
  {
    // Very high lag scenario - algorithm should recommend scaling up
    CostMetrics metrics = new CostMetrics(
        System.currentTimeMillis(),
        10001.0,  // avgPartitionLag - very high (above HIGH_LAG_THRESHOLD)
        25,         // currentTaskCount
        100,        // partitionCount
        0.01         // pollIdleRatio - low idle (tasks are busy)
    );

    int result = autoScaler.computeOptimalTaskCount(new AtomicReference<>(metrics));
    // With very high lag and low idle, algorithm should recommend scaling up aggressively
    Assert.assertEquals(50, result);
  }
}
