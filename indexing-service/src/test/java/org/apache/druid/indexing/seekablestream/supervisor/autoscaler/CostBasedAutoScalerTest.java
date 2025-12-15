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

import static org.mockito.Mockito.when;

public class CostBasedAutoScalerTest
{
  private CostBasedAutoScaler autoScaler;
  private SupervisorSpec mockSupervisorSpec;
  private SeekableStreamSupervisor mockSupervisor;
  private ServiceEmitter mockEmitter;
  private CostBasedAutoScalerConfig config;

  @Before
  public void setUp()
  {
    mockSupervisorSpec = Mockito.mock(SupervisorSpec.class);
    mockSupervisor = Mockito.mock(SeekableStreamSupervisor.class);
    mockEmitter = Mockito.mock(ServiceEmitter.class);

    SeekableStreamSupervisorIOConfig mockIoConfig = Mockito.mock(SeekableStreamSupervisorIOConfig.class);

    when(mockSupervisorSpec.getId()).thenReturn("test-supervisor");
    when(mockSupervisor.getIoConfig()).thenReturn(mockIoConfig);
    when(mockIoConfig.getStream()).thenReturn("test-stream");

    config = CostBasedAutoScalerConfig.builder()
                                      .taskCountMax(100)
                                      .taskCountMin(1)
                                      .enableTaskAutoScaler(true)
                                      .taskCountStart(10)
                                      .minTriggerScaleActionFrequencyMillis(600000L)
                                      .scaleActionPeriodMillis(60000L)
                                      .lagWeight(0.6)
                                      .idleWeight(0.4)
                                      .build();

    autoScaler = new CostBasedAutoScaler(
        mockSupervisor,
        config,
        mockSupervisorSpec,
        mockEmitter
    );
  }

  @Test
  public void testComputeValidTaskCountsGradualScaling()
  {
    // Verify gradual scaling: for 100 partitions, going from 25 tasks (4 partitions/task)
    // the next step should be 34 tasks (3 partitions/task)
    int[] validTaskCounts = CostBasedAutoScaler.computeValidTaskCounts(100, 25);

    int idx25 = Arrays.binarySearch(validTaskCounts, 25);
    int idx34 = Arrays.binarySearch(validTaskCounts, 34);
    Assert.assertTrue("25 should be in factors", idx25 >= 0);
    Assert.assertTrue("34 should be in factors", idx34 >= 0);
    Assert.assertEquals("34 should be the next factor after 25", idx25 + 1, idx34);
  }

  @Test
  public void testComputeOptimalTaskCountInvalidInputs()
  {
    // Empty metrics list
    int result = autoScaler.computeOptimalTaskCount(null);
    Assert.assertEquals(-1, result);

    // Zero partitions
    CostMetrics zeroPartitionsMetrics = new CostMetrics(
        0.0,
        10,   // currentTaskCount
        0,    // partitionCount = 0
        0.0   // pollIdleRatio
    );

    result = autoScaler.computeOptimalTaskCount(zeroPartitionsMetrics);
    Assert.assertEquals(-1, result);
  }

  @Test
  public void testComputeOptimalTaskCountLowIdleScenario_scaleUpGradually()
  {
    // Low idle scenario - algorithm should recommend scaling up to increase idle toward ideal range
    CostMetrics oldMetrics = new CostMetrics(
        300.0,  // avgPartitionLag - low
        100,         // currentTaskCount
        100,        // partitionCount
        0.001       // pollIdleRatio - very low idle (tasks are overloaded)
    );

    int initialResult = autoScaler.computeOptimalTaskCount(oldMetrics);

    // Very low idle (0.001) is below ideal range, but at 100 tasks (max), there's nowhere to scale up
    // The cost function might recommend staying or scaling down depends on the bounds
    Assert.assertTrue("Result should be -1 or a valid task count",
                      initialResult == -1 || initialResult > 0);

    CostMetrics newMetrics = new CostMetrics(
        400.0,  // avgPartitionLag - moderate lag
        34,         // currentTaskCount
        100,        // partitionCount
        0.05        // pollIdleRatio - low idle (below ideal range 0.2)
    );

    int result = autoScaler.computeOptimalTaskCount(newMetrics);
    // With low idle (below ideal range 0.2), the algorithm should recommend scaling up
    // to increase idle toward the ideal range [0.2, 0.6]
    Assert.assertTrue("Should recommend scaling up when idle < 0.2", result > 34);
  }

  @Test
  public void testComputeOptimalTaskCountHighLagScenario_scaleUpAggressively()
  {
    // Very high lag scenario - algorithm should recommend scaling up
    CostMetrics metrics = new CostMetrics(
        10001.0,  // avgPartitionLag - very high (above HIGH_LAG_THRESHOLD)
        25,         // currentTaskCount
        100,        // partitionCount
        0.01         // pollIdleRatio - low idle (tasks are busy)
    );

    int result = autoScaler.computeOptimalTaskCount(metrics);
    // With very high lag and low idle, the algorithm should recommend scaling up aggressively
    Assert.assertEquals(50, result);
  }

  @Test
  public void testComputeOptimalTaskCountIdleInIdealRange_noScaling()
  {
    // When idle is in the ideal range [0.2, 0.6], no scaling should occur
    // regardless of lag level - optimal utilization has been achieved

    // Test with idle at lower bound of ideal range
    CostMetrics metricsLowIdeal = new CostMetrics(
        5000.0,   // avgPartitionLag - high lag
        25,       // currentTaskCount
        100,      // partitionCount
        0.2       // pollIdleRatio - at lower bound of ideal range [0.2, 0.6]
    );
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(metricsLowIdeal));

    // Test with idle in middle of ideal range
    CostMetrics metricsMidIdeal = new CostMetrics(
        10000.0,  // avgPartitionLag - very high lag
        25,       // currentTaskCount
        100,      // partitionCount
        0.4       // pollIdleRatio - in middle of ideal range
    );
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(metricsMidIdeal));

    // Test with idle at upper bound of ideal range
    CostMetrics metricsHighIdeal = new CostMetrics(
        100.0,    // avgPartitionLag - low lag
        25,       // currentTaskCount
        100,      // partitionCount
        0.6       // pollIdleRatio - at upper bound of ideal range
    );
    Assert.assertEquals(-1, autoScaler.computeOptimalTaskCount(metricsHighIdeal));
  }

  @Test
  public void testComputeOptimalTaskCountIdleOutsideIdealRange_scalingAllowed()
  {
    // When idle is outside the ideal range, scaling should be evaluated

    // Below ideal range (overloaded) - should scale up
    CostMetrics metricsOverloaded = new CostMetrics(
        1000.0,   // avgPartitionLag
        25,       // currentTaskCount
        100,      // partitionCount
        0.1       // pollIdleRatio - below ideal range (overloaded)
    );
    int resultOverloaded = autoScaler.computeOptimalTaskCount(metricsOverloaded);
    Assert.assertTrue("Should recommend scaling up when idle < 0.2", resultOverloaded > 25);

    // Above ideal range (underutilized) with low lag - should scale down (returns -1 but sets config)
    CostMetrics metricsUnderutilized = new CostMetrics(
        100.0,    // avgPartitionLag - low
        25,       // currentTaskCount
        100,      // partitionCount
        0.8       // pollIdleRatio - above ideal range (underutilized)
    );
    int resultUnderutilized = autoScaler.computeOptimalTaskCount(metricsUnderutilized);
    // For scale-down, the method returns -1 but sets the config internally
    Assert.assertEquals(-1, resultUnderutilized);
  }
}
