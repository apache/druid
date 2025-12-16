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
                                      .lagWeight(0.3)
                                      .idleWeight(0.7)
                                      .defaultProcessingRate(1000.0)
                                      .build();
  }

  @Test
  public void testComputeCostInvalidInputs()
  {
    CostMetrics validMetrics = createMetrics(100000.0, 10, 100, 0.3);

    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(null, 10, config), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, 10, null), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, 0, config), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, -5, config), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(createMetrics(0.0, 10, 0, 0.3), 10, config), 0.0);
  }

  @Test
  public void testScaleDownHasHigherLagCostThanCurrent()
  {
    CostBasedAutoScalerConfig lagOnlyConfig = CostBasedAutoScalerConfig.builder()
        .taskCountMax(100)
        .taskCountMin(1)
        .enableTaskAutoScaler(true)
        .lagWeight(1.0)
        .idleWeight(0.0)
        .defaultProcessingRate(100.0)
        .build();

    CostMetrics metrics = createMetrics(200000.0, 10, 200, 0.3);

    double costCurrent = costFunction.computeCost(metrics, 10, lagOnlyConfig);
    double costScaleDown = costFunction.computeCost(metrics, 5, lagOnlyConfig);

    // Scale down uses absolute model: lag / (5 * rate) = higher recovery time
    // Current uses absolute model: lag / (10 * rate) = lower recovery time
    Assert.assertTrue(
        "Scale-down should have higher lag cost than current",
        costScaleDown > costCurrent
    );
  }

  @Test
  public void testLagCostWithMarginalModel()
  {
    // With lag-only config (no idle penalty), the marginal model is used for scale-up:
    // lagRecoveryTime = aggregateLag / (taskCountDiff * rate)
    CostBasedAutoScalerConfig lagOnlyConfig = CostBasedAutoScalerConfig.builder()
        .taskCountMax(100)
        .taskCountMin(1)
        .enableTaskAutoScaler(true)
        .lagWeight(1.0)
        .idleWeight(0.0)
        .defaultProcessingRate(1000.0)
        .build();

    // aggregateLag = 100000 * 100 = 10,000,000
    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.3);

    // Current (10 tasks): uses absolute model = 10M / (10 * 1000) = 1000s
    double costCurrent = costFunction.computeCost(metrics, 10, lagOnlyConfig);
    Assert.assertEquals("Cost at current tasks", 1000., costCurrent, 0.1);

    // Scale up by 5 (to 15): marginal model = 10M / (15 * 1000) = 666
    double costUp5 = costFunction.computeCost(metrics, 15, lagOnlyConfig);
    Assert.assertEquals("Cost when scaling up by 5", 666.7, costUp5, 0.1);

    // Scale up by 10 (to 20): marginal model = 10M / (20 * 1000) = 500s
    double costUp10 = costFunction.computeCost(metrics, 20, lagOnlyConfig);
    Assert.assertEquals("Cost when scaling up by 10", 500.0, costUp10, 0.01);

    // Adding more tasks reduces lag recovery time
    Assert.assertTrue("Adding more tasks reduces lag cost", costUp10 < costUp5);
  }

  @Test
  public void testBalancedWeightsFavorStabilityOverScaleUp()
  {
    // With the marginal lag model and corrected idle ratio, balanced weights
    // favor stability because idle cost increases significantly with more tasks
    // This is intentional behavior: the algorithm is conservative about scale-up.
    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.3);

    double costCurrent = costFunction.computeCost(metrics, 10, config);
    double costScaleUp = costFunction.computeCost(metrics, 20, config);

    // With balanced weights (0.3 lag, 0.7 idle), the idle cost increase from
    // scaling up dominates the lag recovery benefit
    Assert.assertTrue(
        "With balanced weights, staying at current count is cheaper than scale-up",
        costCurrent < costScaleUp
    );
  }

  @Test
  public void testWeightsAffectCost()
  {
    CostBasedAutoScalerConfig lagOnly = CostBasedAutoScalerConfig.builder()
                                                                 .taskCountMax(100)
                                                                 .taskCountMin(1)
                                                                 .enableTaskAutoScaler(true)
                                                                 .lagWeight(1.0)
                                                                 .idleWeight(0.0)
                                                                 .defaultProcessingRate(1000.0)
                                                                 .build();

    CostBasedAutoScalerConfig idleOnly = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(100)
                                                                  .taskCountMin(1)
                                                                  .enableTaskAutoScaler(true)
                                                                  .lagWeight(0.0)
                                                                  .idleWeight(1.0)
                                                                  .defaultProcessingRate(1000.0)
                                                                  .build();

    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.1);

    double costLag = costFunction.computeCost(metrics, 10, lagOnly);
    double costIdle = costFunction.computeCost(metrics, 10, idleOnly);

    Assert.assertNotEquals("Different weights should produce different costs", costLag, costIdle, 0.0001);
    Assert.assertTrue("Lag-only cost should be positive", costLag > 0.0);
    Assert.assertTrue("Idle-only cost should be positive", costIdle > 0.0);
  }

  @Test
  public void testNoProcessingRateFavorsCurrentTaskCount()
  {
    // When the processing rate is unavailable (0), the cost function should favor
    // maintaining the current task count, rather to scale up decisions with incomplete data.
    int currentTaskCount = 10;
    CostMetrics metricsNoRate = createMetricsWithRate(50000.0, currentTaskCount, 100, 0.3, 0.0);

    double costAtCurrent = costFunction.computeCost(metricsNoRate, currentTaskCount, config);
    double costScaleUp = costFunction.computeCost(metricsNoRate, currentTaskCount + 5, config);
    double costScaleDown = costFunction.computeCost(metricsNoRate, currentTaskCount - 5, config);

    Assert.assertTrue(
        "Cost at current should be less than cost for scale up",
        costAtCurrent < costScaleUp
    );
    Assert.assertTrue(
        "Cost at current should be less than cost for scale down",
        costAtCurrent < costScaleDown
    );
  }

  @Test
  public void testNoProcessingRateDeviationPenaltyIsSymmetric()
  {
    // Deviation penalty should be symmetric around current task count
    int currentTaskCount = 10;
    CostMetrics metricsNoRate = createMetricsWithRate(50000.0, currentTaskCount, 100, 0.5, 0.0);

    // Use lag-only config to isolate the lag recovery time component
    CostBasedAutoScalerConfig lagOnlyConfig = CostBasedAutoScalerConfig.builder()
        .taskCountMax(100)
        .taskCountMin(1)
        .enableTaskAutoScaler(true)
        .lagWeight(1.0)
        .idleWeight(0.0)
        .defaultProcessingRate(1000.0)
        .build();

    double costUp5 = costFunction.computeCost(metricsNoRate, currentTaskCount + 5, lagOnlyConfig);
    double costDown5 = costFunction.computeCost(metricsNoRate, currentTaskCount - 5, lagOnlyConfig);

    Assert.assertEquals(
        "Lag cost for +5 and -5 deviation should be equal",
        costUp5,
        costDown5,
        0.001
    );
  }

  @Test
  public void testIdleCostMonotonicWithTaskCount()
  {
    // Test that idle cost increases monotonically with task count.
    // With fixed load, adding more tasks means each task has less work, so idle increases.
    CostBasedAutoScalerConfig idleOnlyConfig = CostBasedAutoScalerConfig.builder()
        .taskCountMax(100)
        .taskCountMin(1)
        .enableTaskAutoScaler(true)
        .lagWeight(0.0)
        .idleWeight(1.0)
        .defaultProcessingRate(1000.0)
        .build();

    // Current: 10 tasks with 40% idle (60% busy)
    CostMetrics metrics = createMetrics(0.0, 10, 100, 0.4);

    double costAt5 = costFunction.computeCost(metrics, 5, idleOnlyConfig);
    double costAt10 = costFunction.computeCost(metrics, 10, idleOnlyConfig);
    double costAt15 = costFunction.computeCost(metrics, 15, idleOnlyConfig);
    double costAt20 = costFunction.computeCost(metrics, 20, idleOnlyConfig);

    // Monotonically increasing idle cost as tasks increase
    Assert.assertTrue("cost(5) < cost(10)", costAt5 < costAt10);
    Assert.assertTrue("cost(10) < cost(15)", costAt10 < costAt15);
    Assert.assertTrue("cost(15) < cost(20)", costAt15 < costAt20);
  }

  @Test
  public void testIdleRatioClampingAtBoundaries()
  {
    CostBasedAutoScalerConfig idleOnlyConfig = CostBasedAutoScalerConfig.builder()
        .taskCountMax(100)
        .taskCountMin(1)
        .enableTaskAutoScaler(true)
        .lagWeight(0.0)
        .idleWeight(1.0)
        .defaultProcessingRate(1000.0)
        .build();

    // Extreme scale-down: 10 tasks → 2 tasks with 40% idle
    // busyFraction = 0.6, taskRatio = 0.2
    // predictedIdle = 1 - 0.6/0.2 = 1 - 3 = -2 → clamped to 0
    CostMetrics metrics = createMetrics(0.0, 10, 100, 0.4);
    double costAt2 = costFunction.computeCost(metrics, 2, idleOnlyConfig);

    // idlenessCost = taskCount * taskDuration * 0.0 (clamped) = 0
    Assert.assertEquals("Idle cost should be 0 when predicted idle is clamped to 0", 0.0, costAt2, 0.0001);

    // Extreme scale-up shouldn't exceed 1.0 for idle ratio
    // 10 tasks → 100 tasks with 10% idle
    // busyFraction = 0.9, taskRatio = 10
    // predictedIdle = 1 - 0.9/10 = 1 - 0.09 = 0.91 (within bounds)
    CostMetrics lowIdle = createMetrics(0.0, 10, 100, 0.1);
    double costAt100 = costFunction.computeCost(lowIdle, 100, idleOnlyConfig);
    // idlenessCost = 100 * 3600 * 0.91 = 327600
    Assert.assertTrue("Cost should be finite and positive", Double.isFinite(costAt100) && costAt100 > 0);
  }

  @Test
  public void testIdleRatioWithMissingData()
  {
    CostBasedAutoScalerConfig idleOnlyConfig = CostBasedAutoScalerConfig.builder()
        .taskCountMax(100)
        .taskCountMin(1)
        .enableTaskAutoScaler(true)
        .lagWeight(0.0)
        .idleWeight(1.0)
        .defaultProcessingRate(1000.0)
        .build();

    // Negative idle ratio indicates missing data → should default to 0.5
    CostMetrics missingIdleData = createMetrics(0.0, 10, 100, -1.0);

    double cost10 = costFunction.computeCost(missingIdleData, 10, idleOnlyConfig);
    double cost20 = costFunction.computeCost(missingIdleData, 20, idleOnlyConfig);

    // With missing data, predicted idle = 0.5 for all task counts
    // idlenessCost at 10 = 10 * 3600 * 0.5 = 18000
    // idlenessCost at 20 = 20 * 3600 * 0.5 = 36000
    Assert.assertEquals("Cost at 10 tasks with missing idle data", 10 * 3600 * 0.5, cost10, 0.0001);
    Assert.assertEquals("Cost at 20 tasks with missing idle data", 20 * 3600 * 0.5, cost20, 0.0001);
  }

  private CostMetrics createMetrics(double avgPartitionLag, int currentTaskCount, int partitionCount, double pollIdleRatio)
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

  private CostMetrics createMetricsWithRate(
      double avgPartitionLag,
      int currentTaskCount,
      int partitionCount,
      double pollIdleRatio,
      double avgProcessingRate
  )
  {
    return new CostMetrics(
        avgPartitionLag,
        currentTaskCount,
        partitionCount,
        pollIdleRatio,
        3600,
        avgProcessingRate
    );
  }
}
