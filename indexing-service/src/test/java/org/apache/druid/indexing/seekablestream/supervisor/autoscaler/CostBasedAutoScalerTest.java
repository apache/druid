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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

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
                                      .metricsCollectionRangeMillis(600000L)
                                      .scaleActionStartDelayMillis(300000L)
                                      .scaleActionPeriodMillis(60000L)
                                      .lagWeight(0.6)
                                      .idleWeight(0.3)
                                      .changeWeight(0.1)
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
  public void testComputeFactorsWithBounds()
  {
    // 100 partitions, bounds [1, 100] - full range
    List<Integer> factors = autoScaler.computeFactors(100, new int[]{1, 100});
    Assert.assertEquals(1, (int) factors.get(0));
    Assert.assertEquals(100, (int) factors.get(factors.size() - 1));
    Assert.assertTrue(factors.contains(25));
    Assert.assertTrue(factors.contains(34));
    Assert.assertTrue(factors.contains(50));

    // 100 partitions, bounds [5, 20] - constrained range
    List<Integer> constrainedFactors = autoScaler.computeFactors(100, new int[]{5, 20});
    for (int f : constrainedFactors) {
      Assert.assertTrue("Factor " + f + " should be >= 5", f >= 5);
      Assert.assertTrue("Factor " + f + " should be <= 20", f <= 20);
    }
    Assert.assertTrue(constrainedFactors.contains(10));
    Assert.assertTrue(constrainedFactors.contains(13));
    Assert.assertTrue(constrainedFactors.contains(17));
    Assert.assertTrue(constrainedFactors.contains(20));

    // Empty result when no factors in range
    List<Integer> emptyFactors = autoScaler.computeFactors(100, new int[]{101, 200});
    Assert.assertTrue(emptyFactors.isEmpty());

    // Edge case: zero partitions
    Assert.assertTrue(autoScaler.computeFactors(0, new int[]{1, 100}).isEmpty());
  }

  @Test
  public void testComputeFactorsGradualScaling()
  {
    // Verify gradual scaling: for 100 partitions, going from 25 tasks (4 partitions/task)
    // the next step should be 34 tasks (3 partitions/task)
    List<Integer> factors = autoScaler.computeFactors(100, new int[]{1, 100});

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
    int result = autoScaler.computeOptimalTaskCount(Collections.emptyList());
    Assert.assertEquals(-1, result);

    // Zero partitions
    CostMetrics zeroPartitionsMetrics = new CostMetrics(
        System.currentTimeMillis(),
        0.0,  // avgPartitionLag
        10,   // currentTaskCount
        0,    // partitionCount = 0
        0.0   // pollIdleRatio
    );

    result = autoScaler.computeOptimalTaskCount(Collections.singletonList(zeroPartitionsMetrics));
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

    int initialResult = autoScaler.computeOptimalTaskCount(Collections.singletonList(oldMetrics));
    Assert.assertEquals(34, initialResult);

    CostMetrics newMetrics = new CostMetrics(
        System.currentTimeMillis(),
        400.0,  // avgPartitionLag - moderate lag for gradual scaling
        34,         // currentTaskCount
        100,        // partitionCount
        0.001       // pollIdleRatio - low idle (tasks are busy)
    );

    int result = autoScaler.computeOptimalTaskCount(Arrays.asList(oldMetrics, newMetrics));
    // With relatively high lag and very low idle, the algorithm should recommend scaling up
    Assert.assertEquals(50, result);
  }

  @Test
  public void testComputeOptimalTaskCountHighLagScenario_scaleUpAggressively()
  {
    // Very high lag scenario - algorithm should recommend scaling up
    CostMetrics metrics = new CostMetrics(
        System.currentTimeMillis(),
        3000000.0,  // avgPartitionLag - very high (above HIGH_LAG_THRESHOLD)
        25,         // currentTaskCount
        100,        // partitionCount
        0.1         // pollIdleRatio - low idle (tasks are busy)
    );

    int result = autoScaler.computeOptimalTaskCount(Collections.singletonList(metrics));
    // With very high lag and low idle, algorithm should recommend scaling up aggressively
    Assert.assertEquals(50, result);
  }
}
