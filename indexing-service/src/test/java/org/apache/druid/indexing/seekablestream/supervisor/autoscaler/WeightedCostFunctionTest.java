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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class WeightedCostFunctionTest
{
  private WeightedCostFunction costFunction;
  private CostBasedAutoScalerConfig config;

  @Before
  public void setUp()
  {
    costFunction = new WeightedCostFunction();
    config = CostBasedAutoScalerConfig.builder()
                                      .taskCountMax(100)
                                      .taskCountMin(1)
                                      .enableTaskAutoScaler(true)
                                      .lagWeight(0.6)
                                      .idleWeight(0.4)
                                      .build();
  }

  @Test
  public void testComputeCost_invalidInputsReturnInfinity()
  {
    CostMetrics validMetrics = createMetrics(100000.0, 10, 100, 0.3);
    CostMetrics zeroPartitionsMetrics = createMetrics(0.0, 10, 0, 0.3);

    // Null metrics
    Assert.assertEquals(
        "Null metrics should return POSITIVE_INFINITY",
        Double.POSITIVE_INFINITY,
        costFunction.computeCost(null, 10, config),
        0.0
    );

    // Null config
    Assert.assertEquals(
        "Null config should return POSITIVE_INFINITY",
        Double.POSITIVE_INFINITY,
        costFunction.computeCost(validMetrics, 10, null),
        0.0
    );

    // Zero task count
    Assert.assertEquals(
        "Zero task count should return POSITIVE_INFINITY",
        Double.POSITIVE_INFINITY,
        costFunction.computeCost(validMetrics, 0, config),
        0.0
    );

    // Negative task count
    Assert.assertEquals(
        "Negative task count should return POSITIVE_INFINITY",
        Double.POSITIVE_INFINITY,
        costFunction.computeCost(validMetrics, -5, config),
        0.0
    );

    // Zero partitions
    Assert.assertEquals(
        "Zero partitions should return POSITIVE_INFINITY",
        Double.POSITIVE_INFINITY,
        costFunction.computeCost(zeroPartitionsMetrics, 10, config),
        0.0
    );
  }

  @Test
  public void testComputeCost_validInput()
  {
    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.3);

    // Warm up bounds with observed lag
    for (int i = 0; i < 5; i++) {
      costFunction.updateLagBounds(metrics.getAvgPartitionLag());
    }

    double cost = costFunction.computeCost(metrics, 10, config);
    Assert.assertTrue("Cost should be finite", Double.isFinite(cost));
    Assert.assertTrue("Cost should be non-negative", cost >= 0.0);
  }

  @Test
  public void testComputeCost_increasingTasksReducesLag()
  {
    // High lag scenario
    CostMetrics highLagMetrics = createMetrics(3000000.0, 5, 100, 0.1);

    // Warm up bounds with observed lag
    for (int i = 0; i < 10; i++) {
      costFunction.updateLagBounds(highLagMetrics.getAvgPartitionLag());
    }

    double cost5Tasks = costFunction.computeCost(highLagMetrics, 5, config);
    double cost10Tasks = costFunction.computeCost(highLagMetrics, 10, config);
    double cost20Tasks = costFunction.computeCost(highLagMetrics, 20, config);

    // With high lag, more tasks should reduce cost due to reduced predicted lag
    Assert.assertTrue(
        "More tasks should reduce cost when lag is high (10 tasks < 5 tasks)",
        cost10Tasks < cost5Tasks
    );
    Assert.assertTrue(
        "More tasks should reduce cost when lag is high (20 tasks < 10 tasks)",
        cost20Tasks < cost10Tasks
    );
  }


  @Test
  public void testNormalize_handlesIdenticalBounds()
  {
    // When min equals max, normalization should return 0.5
    CostMetrics metrics = createMetrics(1000.0, 10, 100, 0.3);

    // First call initializes bounds to the same value
    double cost = costFunction.computeCost(metrics, 10, config);

    // Should not throw exception or return NaN
    Assert.assertTrue("Cost should be finite even with identical bounds", Double.isFinite(cost));
  }

  @Test
  public void testComputeCost_withDifferentWeights()
  {
    CostBasedAutoScalerConfig lagHeavyConfig = CostBasedAutoScalerConfig.builder()
                                                                        .taskCountMax(100)
                                                                        .taskCountMin(1)
                                                                        .enableTaskAutoScaler(true)
                                                                        .lagWeight(0.9)
                                                                        .idleWeight(0.1)
                                                                        .build();

    CostBasedAutoScalerConfig idleHeavyConfig = CostBasedAutoScalerConfig.builder()
                                                                         .taskCountMax(100)
                                                                         .taskCountMin(1)
                                                                         .enableTaskAutoScaler(true)
                                                                         .lagWeight(0.1)
                                                                         .idleWeight(0.9)
                                                                         .build();

    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.5);

    // Warm up bounds with observed lag
    for (int i = 0; i < 5; i++) {
      costFunction.updateLagBounds(metrics.getAvgPartitionLag());
    }

    double costLagHeavy = costFunction.computeCost(metrics, 10, lagHeavyConfig);
    double costIdleHeavy = costFunction.computeCost(metrics, 10, idleHeavyConfig);

    // Both should be valid costs
    Assert.assertTrue("Lag-heavy config should produce finite cost", Double.isFinite(costLagHeavy));
    Assert.assertTrue("Idle-heavy config should produce finite cost", Double.isFinite(costIdleHeavy));
  }

  @Test
  public void testComputeCost_consistencyWithRepeatedCalls()
  {
    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.3);

    // Warm up bounds to stabilize
    for (int i = 0; i < 20; i++) {
      costFunction.updateLagBounds(metrics.getAvgPartitionLag());
    }

    double cost1 = costFunction.computeCost(metrics, 10, config);
    double cost2 = costFunction.computeCost(metrics, 10, config);
    double cost3 = costFunction.computeCost(metrics, 10, config);

    // Costs should be very similar (within small epsilon due to floating point)
    // after bounds stabilize
    Assert.assertEquals("Repeated calls should produce consistent results", cost1, cost2, 0.001);
    Assert.assertEquals("Repeated calls should produce consistent results", cost2, cost3, 0.001);
  }

  /**
   * Helper method to create CostMetrics with correct constructor signature.
   */
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
        pollIdleRatio
    );
  }
}
