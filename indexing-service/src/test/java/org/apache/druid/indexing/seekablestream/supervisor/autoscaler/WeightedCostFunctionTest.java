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

import org.apache.druid.error.DruidException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class WeightedCostFunctionTest
{
  private WeightedCostFunction costFunction;
  private CostBasedAutoScalerConfig config;

  @BeforeEach
  public void setUp()
  {
    costFunction = new WeightedCostFunction();
    config = CostBasedAutoScalerConfig.builder()
                                      .taskCountMax(100)
                                      .taskCountMin(1)
                                      .enableTaskAutoScaler(true)
                                      .lagWeight(0.3)
                                      .idleWeight(0.7)
                                      .build();
  }

  @Test
  public void testComputeCostInvalidInputs()
  {
    CostMetrics validMetrics = createMetrics(100000.0, 10, 100, 0.3);

    Assertions.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(null, 10, config).totalCost(), 0.0);
    Assertions.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, 10, null).totalCost(), 0.0);
    Assertions.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, 0, config).totalCost(), 0.0);
    Assertions.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, -5, config).totalCost(), 0.0);
    Assertions.assertEquals(
        Double.POSITIVE_INFINITY,
        costFunction.computeCost(createMetrics(0.0, 10, 0, 0.3), 10, config).totalCost(),
        0.0
    );
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
                                                                       .build();

    CostMetrics metrics = createMetrics(200000.0, 10, 200, 0.3);

    double costCurrent = costFunction.computeCost(metrics, 10, lagOnlyConfig).totalCost();
    double costScaleDown = costFunction.computeCost(metrics, 5, lagOnlyConfig).totalCost();

    // Scale down uses absolute model: lag / (5 * rate) = higher recovery time
    // Current uses absolute model: lag / (10 * rate) = lower recovery time
    Assertions.assertTrue(
        costScaleDown > costCurrent,
        "Scale-down should have higher lag cost than current"
    );
  }

  @Test
  public void testLagCostWithAbsoluteModel()
  {
    // With lag-only config (no idle penalty), cost uses absolute model:
    // lagRecoveryTime = aggregateLag / (taskCount * rate)
    CostBasedAutoScalerConfig lagOnlyConfig = CostBasedAutoScalerConfig.builder()
                                                                       .taskCountMax(100)
                                                                       .taskCountMin(1)
                                                                       .enableTaskAutoScaler(true)
                                                                       .lagWeight(1.0)
                                                                       .idleWeight(0.0)
                                                                       .build();

    // aggregateLag = 100000 * 100 = 10,000,000; lagPerPartition = 100,000
    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.3);
    double aggregateLag = 100000.0 * 100;
    double amplification = 1.0 + WeightedCostFunction.LAG_AMPLIFICATION_MULTIPLIER * Math.log(aggregateLag / 100);

    double costCurrent = costFunction.computeCost(metrics, 10, lagOnlyConfig).totalCost();
    Assertions.assertEquals(
        aggregateLag * amplification / (10 * WeightedCostFunction.MIN_PROCESSING_RATE),
        costCurrent,
        0.1,
        "Cost of current tasks"
    );

    double costUp5 = costFunction.computeCost(metrics, 15, lagOnlyConfig).totalCost();
    Assertions.assertEquals(
        aggregateLag * amplification / (15 * WeightedCostFunction.MIN_PROCESSING_RATE),
        costUp5,
        0.1,
        "Cost when scaling up by 5"
    );

    double costUp10 = costFunction.computeCost(metrics, 20, lagOnlyConfig).totalCost();
    Assertions.assertEquals(
        aggregateLag * amplification / (20 * WeightedCostFunction.MIN_PROCESSING_RATE),
        costUp10,
        0.1,
        "Cost when scaling up by 10"
    );

    // Adding more tasks reduces lag recovery time
    Assertions.assertTrue(costUp10 < costUp5, "Adding more tasks reduces lag cost");
  }

  @Test
  public void testBalancedWeightsFavorStabilityOverScaleUpOnSmallLag()
  {
    // Validate idle ratio estimation and ensure balanced weights still favor stability.
    CostMetrics metrics = createMetrics(100.0, 10, 100, 0.3);
    double costCurrent = costFunction.computeCost(metrics, 10, config).totalCost();
    double costScaleUp = costFunction.computeCost(metrics, 20, config).totalCost();

    Assertions.assertTrue(
        costCurrent < costScaleUp,
        "With balanced weights, staying at current count is cheaper than scale-up"
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
                                                                 .build();

    CostBasedAutoScalerConfig idleOnly = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(100)
                                                                  .taskCountMin(1)
                                                                  .enableTaskAutoScaler(true)
                                                                  .lagWeight(0.0)
                                                                  .idleWeight(1.0)
                                                                  .build();

    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.1);

    double costLag = costFunction.computeCost(metrics, 10, lagOnly).totalCost();
    double costIdle = costFunction.computeCost(metrics, 10, idleOnly).totalCost();

    Assertions.assertNotEquals(costLag, costIdle, 0.0001, "Different weights should produce different costs");
    Assertions.assertTrue(costLag > 0.0, "Lag-only cost should be positive");
    Assertions.assertTrue(costIdle > 0.0, "Idle-only cost should be positive");
  }

  @Test
  public void testZeroProcessingRateUsesDefaultRate()
  {
    // When the processing rate is zero, the cost function uses a default rate and tries to recover lag
    int currentTaskCount = 10;
    CostMetrics metricsNoRate = createMetricsWithRate(50000.0, currentTaskCount, 100, 1.0, 0.0);

    final CostBasedAutoScalerConfig config = CostBasedAutoScalerConfig
        .builder()
        .taskCountMin(1)
        .taskCountMax(100)
        .idleWeight(0)
        .lagWeight(1)
        .build();
    double costAtCurrent = costFunction.computeCost(metricsNoRate, currentTaskCount, config).totalCost();
    double costScaleUp = costFunction.computeCost(metricsNoRate, currentTaskCount + 5, config).totalCost();
    double costScaleDown = costFunction.computeCost(metricsNoRate, currentTaskCount - 5, config).totalCost();

    Assertions.assertTrue(
        costAtCurrent > costScaleUp,
        "Cost at current should be less than cost for scale up"
    );
    Assertions.assertTrue(
        costAtCurrent < costScaleDown,
        "Cost at current should be less than cost for scale down"
    );
  }

  @Test
  public void testNegativeProcessingRate_throwsDefensiveException()
  {
    final CostMetrics metrics = createMetricsWithRate(50000.0, 1, 100, 0.5, -1);
    Assertions.assertThrows(
        DruidException.class,
        () -> costFunction.computeCost(metrics, 2, config)
    );
  }

  @Test
  public void testIdleCostIsUShapedAroundOptimalTaskIdleRatio()
  {
    // U-shaped cost: minimum near OPTIMAL_TASK_IDLE_RATIO=0.25, higher on both sides.
    // Current: 10 tasks with 25% idle (already at optimum).
    CostBasedAutoScalerConfig idleOnlyConfig = CostBasedAutoScalerConfig.builder()
                                                                        .taskCountMax(100)
                                                                        .taskCountMin(1)
                                                                        .enableTaskAutoScaler(true)
                                                                        .lagWeight(0.0)
                                                                        .idleWeight(1.0)
                                                                        .build();

    CostMetrics metrics = createMetrics(0.0, 10, 100, 0.25);

    // At current (idle=0.25=ideal): baseline cost only, penalty=0
    double costAtIdeal = costFunction.computeCost(metrics, 10, idleOnlyConfig).totalCost();

    // Scale down → predicted idle falls below ideal → under-provisioning penalty
    double costScaleDown = costFunction.computeCost(metrics, 5, idleOnlyConfig).totalCost();

    // Scale up → predicted idle rises above ideal → over-provisioning penalty
    double costScaleUp = costFunction.computeCost(metrics, 20, idleOnlyConfig).totalCost();

    Assertions.assertTrue(costScaleDown > costAtIdeal, "scale-down costs more than ideal");
    Assertions.assertTrue(costScaleUp > costAtIdeal, "scale-up costs more than ideal");
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
                                                                        .build();

    // Extreme scale-down: 10 tasks → 2 tasks with 40% idle
    // busyFraction = 0.6; projectedBusy = 0.6 * (10/2)^0.32 ≈ 1.004
    // predictedIdle = 1 - 1.004 ≈ -0.004 → still clamped to 0 at this extreme
    CostMetrics metrics = createMetrics(0.0, 10, 100, 0.4);
    double costAt2 = costFunction.computeCost(metrics, 2, idleOnlyConfig).totalCost();

    // idle = 0: max under-provisioning penalty; cost = taskCount * (IDEAL + UNDER_PENALTY)
    double expectedAt2 = 2 * (
        WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO
        + WeightedCostFunction.UNDER_PROVISIONING_PENALTY
    );
    Assertions.assertEquals(expectedAt2, costAt2, 0.0001, "Idle cost at clamped-to-zero idle ratio should reflect full under-provisioning penalty");

    // Extreme scale-up shouldn't exceed 1.0 for idle ratio
    // 10 tasks → 100 tasks with 10% idle
    // busyFraction = 0.9; projectedBusy = 0.9 * (10/100)^0.32 ≈ 0.431
    // predictedIdle = 1 - 0.431 ≈ 0.569 (within bounds)
    CostMetrics lowIdle = createMetrics(0.0, 10, 100, 0.1);
    double costAt100 = costFunction.computeCost(lowIdle, 100, idleOnlyConfig).totalCost();
    Assertions.assertTrue(Double.isFinite(costAt100) && costAt100 > 0, "Cost should be finite and positive");
  }

  @Test
  public void testModerateConsolidationProjectsHealthyIdle()
  {
    // The regime the linear model got wrong: a healthy 2x consolidation from 40% idle.
    // Sublinear gives predictedIdle ≈ 0.25 (near ideal, no overrun), not the linear -0.2 that clamps to 0.
    CostBasedAutoScalerConfig idleOnlyConfig = CostBasedAutoScalerConfig.builder()
                                                                        .taskCountMax(100)
                                                                        .taskCountMin(1)
                                                                        .enableTaskAutoScaler(true)
                                                                        .lagWeight(0.0)
                                                                        .idleWeight(1.0)
                                                                        .build();

    CostMetrics metrics = createMetrics(0.0, 10, 100, 0.4);
    double costCurrent = costFunction.computeCost(metrics, 10, idleOnlyConfig).totalCost();
    double costScaleDown = costFunction.computeCost(metrics, 5, idleOnlyConfig).totalCost();

    Assertions.assertTrue(
        costScaleDown < costCurrent,
        "Healthy 2x consolidation should be cheaper than staying at the current count"
    );

    // Far below the clamped-to-zero cost the linear model produced: idle is healthy, not clamped.
    double clampedToZeroCost =
        5 * (WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO + WeightedCostFunction.UNDER_PROVISIONING_PENALTY);
    Assertions.assertTrue(
        costScaleDown < clampedToZeroCost,
        "Predicted idle should be healthy, not clamped to zero"
    );
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
                                                                        .build();

    // Negative idle ratio indicates missing data → should default to 0.5
    CostMetrics missingIdleData = createMetrics(0.0, 10, 100, -1.0);

    double cost10 = costFunction.computeCost(missingIdleData, 10, idleOnlyConfig).totalCost();
    double cost20 = costFunction.computeCost(missingIdleData, 20, idleOnlyConfig).totalCost();

    // With missing data, predicted idle = 0.5 for all task counts regardless of proposed count.
    // U-shaped cost at idle=0.5: idle > IDEAL(0.25), norm=(0.5-0.25)/0.75=1/3, penalty=1*(1/3)^2=1/9
    double expectedCostPerTask = WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO
                                 + WeightedCostFunction.OVER_PROVISIONING_PENALTY * (1.0 / 3.0) * (1.0 / 3.0);
    Assertions.assertEquals(10 * expectedCostPerTask, cost10, 0.0001, "Cost at 10 tasks with missing idle data");
    Assertions.assertEquals(20 * expectedCostPerTask, cost20, 0.0001, "Cost at 20 tasks with missing idle data");
    Assertions.assertEquals(2 * cost10, cost20, 0.0001, "Cost scales linearly with task count at fixed idle ratio");
  }

  @Test
  public void testNormalLagCostUsesUnamplifiedRecoveryTime()
  {
    CostBasedAutoScalerConfig lagOnly = CostBasedAutoScalerConfig.builder()
                                                                 .taskCountMax(100)
                                                                 .taskCountMin(1)
                                                                 .enableTaskAutoScaler(true)
                                                                 .lagWeight(1.0)
                                                                 .idleWeight(0.0)
                                                                 .build();

    int currentTaskCount = 10;
    int proposedTaskCount = 10;
    int partitionCount = 10;
    double pollIdleRatio = 0.1;

    // Normal lag uses raw recovery time; high lag is tested separately below.
    CostMetrics metrics = createMetrics(150.0, currentTaskCount, partitionCount, pollIdleRatio);

    double costWithAmp = costFunction.computeCost(metrics, proposedTaskCount, lagOnly).totalCost();

    double aggregateLag = 150.0 * partitionCount;
    double expected = aggregateLag / (proposedTaskCount * WeightedCostFunction.MIN_PROCESSING_RATE);

    Assertions.assertEquals(expected, costWithAmp, 0.0001, "Normal lag cost should use raw recovery time");
  }

  @Test
  public void testHighLagThresholdMaxesOutCostFactor()
  {
    int currentTaskCount = 10;
    int proposedTaskCount = 10;
    int partitionCount = 10;
    double avgPartitionLag = 150.0;
    double aggregateLag = avgPartitionLag * partitionCount;

    CostMetrics metrics = createMetrics(avgPartitionLag, currentTaskCount, partitionCount, 0.1);

    // aggregateLag sits at exactly tier1Fraction (75%) of this threshold.
    long tier1Threshold = (long) (aggregateLag / WeightedCostFunction.HIGH_LAG_THRESHOLD_FRACTION);

    CostBasedAutoScalerConfig noThreshold = CostBasedAutoScalerConfig.builder()
                                                                      .taskCountMax(100)
                                                                      .taskCountMin(1)
                                                                      .enableTaskAutoScaler(true)
                                                                      .lagWeight(1.0)
                                                                      .idleWeight(0.0)
                                                                      .build();
    CostBasedAutoScalerConfig belowTier1 = CostBasedAutoScalerConfig.builder()
                                                                     .taskCountMax(100)
                                                                     .taskCountMin(1)
                                                                     .enableTaskAutoScaler(true)
                                                                     .lagWeight(1.0)
                                                                     .idleWeight(0.0)
                                                                     .criticalLagThreshold(tier1Threshold + 100)
                                                                     .build();
    CostBasedAutoScalerConfig atTier1 = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(100)
                                                                  .taskCountMin(1)
                                                                  .enableTaskAutoScaler(true)
                                                                  .lagWeight(1.0)
                                                                  .idleWeight(0.0)
                                                                  .criticalLagThreshold(tier1Threshold)
                                                                  .build();

    double costBelowTier1 = costFunction.computeCost(metrics, proposedTaskCount, belowTier1).totalCost();
    Assertions.assertEquals(
        costFunction.computeCost(metrics, proposedTaskCount, noThreshold).totalCost(),
        costBelowTier1,
        0.0001,
        "Below tier1, amplification uses the default multiplier"
    );

    double lagPerPartition = aggregateLag / partitionCount;
    double highLagCostFactor =
        1.0 + WeightedCostFunction.DEFAULT_HIGH_LAG_COST_FACTOR * Math.log(lagPerPartition);
    double costAtTier1 = costFunction.computeCost(metrics, proposedTaskCount, atTier1).totalCost();
    Assertions.assertEquals(
        aggregateLag * highLagCostFactor / (proposedTaskCount * WeightedCostFunction.MIN_PROCESSING_RATE),
        costAtTier1,
        0.0001,
        "At/above the high-lag threshold, the cost factor maxes out at DEFAULT_HIGH_LAG_COST_FACTOR"
    );
    Assertions.assertTrue(
        costAtTier1 > costBelowTier1,
        "High-lag cost should exceed the default-multiplier cost for the same lag"
    );
  }

  @Test
  public void testNormalLagCostScalesLinearlyWithLag()
  {
    // Without normal-path amplification, cost grows linearly with lag.
    CostBasedAutoScalerConfig lagOnly = CostBasedAutoScalerConfig.builder()
                                                                 .taskCountMax(100)
                                                                 .taskCountMin(1)
                                                                 .enableTaskAutoScaler(true)
                                                                 .lagWeight(1.0)
                                                                 .idleWeight(0.0)
                                                                 .build();

    int currentTaskCount = 10;
    int proposedTaskCount = 10;
    int partitionCount = 30;
    double pollIdleRatio = 0.1;

    CostMetrics lowLag = createMetrics(100.0, currentTaskCount, partitionCount, pollIdleRatio);
    CostMetrics highLag = createMetrics(10_000.0, currentTaskCount, partitionCount, pollIdleRatio);

    double lowCost = costFunction.computeCost(lowLag, proposedTaskCount, lagOnly).totalCost();
    double highCost = costFunction.computeCost(highLag, proposedTaskCount, lagOnly).totalCost();

    Assertions.assertTrue(highCost > lowCost, "Higher lag should produce higher cost");

    // The ratio of costs matches the ratio of raw lags.
    double lagRatio = 10_000.0 / 100.0;
    double costRatio = highCost / lowCost;
    Assertions.assertEquals(
        lagRatio,
        costRatio,
        0.0001,
        "Normal lag cost should grow linearly with lag"
    );
  }


  @Test
  public void testUShapedIdleCostFormula()
  {
    int n = 10;

    // At optimal ratio: penalty = 0, cost = n * OPTIMAL_TASK_IDLE_RATIO
    Assertions.assertEquals(
        n * WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO,
        costFunction.uShapedIdleCost(
            WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO,
            n,
            WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO
        ),
        1e-9
    );

    // At idle = 0 (fully under-provisioned): norm = 1, penalty = UNDER_PROVISIONING_PENALTY
    Assertions.assertEquals(
        n * (WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO + WeightedCostFunction.UNDER_PROVISIONING_PENALTY),
        costFunction.uShapedIdleCost(0.0, n, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        1e-9
    );

    // At idle = 1 (fully over-provisioned): norm = 1, penalty = OVER_PROVISIONING_PENALTY
    Assertions.assertEquals(
        n * (WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO + WeightedCostFunction.OVER_PROVISIONING_PENALTY),
        costFunction.uShapedIdleCost(1.0, n, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        1e-9
    );

    // Both extremes exceed the optimal cost
    final double optimalCost = costFunction.uShapedIdleCost(
        WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO,
        n,
        WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO
    );
    Assertions.assertTrue(
        costFunction.uShapedIdleCost(0.0, n, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO) > optimalCost,
        "idle=0 costs more than optimal"
    );
    Assertions.assertTrue(
        costFunction.uShapedIdleCost(1.0, n, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO) > optimalCost,
        "idle=1 costs more than optimal"
    );

    // Over-provisioning is penalized more than under-provisioning (OVER > UNDER)
    Assertions.assertTrue(
        costFunction.uShapedIdleCost(1.0, n, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO)
        > costFunction.uShapedIdleCost(0.0, n, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        "over-provisioning penalty exceeds under-provisioning penalty"
    );

    // Cost scales linearly with task count at any fixed idle ratio
    Assertions.assertEquals(
        2 * costFunction.uShapedIdleCost(0.5, n, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        costFunction.uShapedIdleCost(0.5, 2 * n, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        1e-9
    );
  }

  @Test
  public void testEstimateIdleRatioFromProcessingRate()
  {
    // 75% utilization (750/1000) -> idle = 0.25
    final CostMetrics utilized = createMetricsWithMaxObservedRate(750.0, 1000.0, 0.3);
    Assertions.assertEquals(
        0.25,
        utilized.estimateIdleRatioFromProcessingRate(),
        0.0001
    );

    // Utilization above 100% (rate exceeds the watermark) clamps idle to 0, not negative
    final CostMetrics overUtilized = createMetricsWithMaxObservedRate(1500.0, 1000.0, 0.3);
    Assertions.assertEquals(
        0.0,
        overUtilized.estimateIdleRatioFromProcessingRate(),
        0.0001
    );

    // No throughput baseline yet (maxObservedRate=0) -> return negative (unknown), never NaN from 0/0
    final CostMetrics noBaseline = createMetricsWithMaxObservedRate(0.0, 0.0, 0.3);
    Assertions.assertTrue(noBaseline.estimateIdleRatioFromProcessingRate() < 0);
  }

  @Test
  public void testComputeCostSwitchesBetweenPollIdleRatioAndUtilizationRatio()
  {
    CostBasedAutoScalerConfig idleOnlyConfig = CostBasedAutoScalerConfig.builder()
                                                                        .taskCountMax(100)
                                                                        .taskCountMin(1)
                                                                        .enableTaskAutoScaler(true)
                                                                        .lagWeight(0.0)
                                                                        .idleWeight(1.0)
                                                                        .build();
    CostBasedAutoScalerConfig utilizationConfig = CostBasedAutoScalerConfig.builder()
                                                                           .taskCountMax(100)
                                                                           .taskCountMin(1)
                                                                           .enableTaskAutoScaler(true)
                                                                           .lagWeight(0.0)
                                                                           .idleWeight(1.0)
                                                                           .usePollIdleRatio(false)
                                                                           .build();

    // pollIdleRatio says 90% idle, but utilization (100/1000 used) says 90% idle too -- pick values that
    // diverge so the two code paths are distinguishable.
    CostMetrics metrics = createMetricsWithMaxObservedRate(100.0, 1000.0, 0.9);

    double costWithPollIdleRatio = costFunction.computeCost(metrics, 10, idleOnlyConfig).totalCost();
    Assertions.assertEquals(
        costFunction.uShapedIdleCost(0.9, 10, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        costWithPollIdleRatio,
        0.0001,
        "Default config should cost using the raw pollIdleRatio"
    );

    double costWithUtilizationRatio = costFunction.computeCost(metrics, 10, utilizationConfig).totalCost();
    Assertions.assertEquals(
        costFunction.uShapedIdleCost(0.9, 10, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        costWithUtilizationRatio,
        0.0001,
        "usePollIdleRatio=false should cost using the rate-derived idle ratio instead of pollIdleRatio"
    );

    // Now diverge pollIdleRatio from the utilization-derived value to prove the flag actually switches sources.
    CostMetrics divergingMetrics = createMetricsWithMaxObservedRate(100.0, 1000.0, 0.1);
    double costStillPollIdle = costFunction.computeCost(divergingMetrics, 10, idleOnlyConfig).totalCost();
    double costStillUtilization = costFunction.computeCost(divergingMetrics, 10, utilizationConfig).totalCost();
    Assertions.assertEquals(
        costFunction.uShapedIdleCost(0.1, 10, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        costStillPollIdle,
        0.0001
    );
    Assertions.assertEquals(
        costFunction.uShapedIdleCost(0.9, 10, WeightedCostFunction.OPTIMAL_TASK_IDLE_RATIO),
        costStillUtilization,
        0.0001,
        "Utilization-derived idle ratio (0.9) should be used instead of the diverging pollIdleRatio (0.1)"
    );
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
        avgPartitionLag * partitionCount,
        currentTaskCount,
        partitionCount,
        pollIdleRatio,
        3600,
        1000.0,
        1000.0
    );
  }

  private CostMetrics createMetricsWithMaxObservedRate(
      double avgProcessingRate,
      double maxObservedRate,
      double pollIdleRatio
  )
  {
    return new CostMetrics(0.0, 0.0, 10, 100, pollIdleRatio, 3600, avgProcessingRate, maxObservedRate);
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
        avgPartitionLag * partitionCount,
        currentTaskCount,
        partitionCount,
        pollIdleRatio,
        3600,
        avgProcessingRate,
        1000.0
    );
  }
}
