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
                                      .build();
  }

  @Test
  public void testComputeCostReturnsInfinityForInvalidInputs()
  {
    CostMetrics validMetrics = createMetrics(100000.0, 10, 100, 0.3);
    CostMetrics zeroPartitions = createMetrics(0.0, 10, 0, 0.3);

    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(null, 10, config), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, 10, null), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, 0, config), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(validMetrics, -5, config), 0.0);
    Assert.assertEquals(Double.POSITIVE_INFINITY, costFunction.computeCost(zeroPartitions, 10, config), 0.0);
  }

  @Test
  public void testComputeCostReturnsFiniteNonNegativeForValidInput()
  {
    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.3);

    double cost = costFunction.computeCost(metrics, 10, config);

    Assert.assertTrue(Double.isFinite(cost));
    Assert.assertTrue(cost >= 0.0);
  }

  @Test
  public void testComputeCostDecreaseWithMoreTasksWhenLagIsHigh()
  {
    // High lag scenario: starting with 5 tasks, high lag (3000 > 1000 threshold)
    // and high idle (0.7 = above ideal), we should scale up to reduce idle
    // In high lag mode, more tasks = less idle (inverted relationship)
    // So adding tasks should move idle from 0.7 toward ideal range [0.2, 0.6]
    CostMetrics metrics = createMetrics(3000.0, 5, 100, 0.7);

    double cost5 = costFunction.computeCost(metrics, 5, config);
    double cost10 = costFunction.computeCost(metrics, 10, config);
    double cost20 = costFunction.computeCost(metrics, 20, config);

    // With high lag and high idle (above ideal), more tasks decrease idle toward ideal
    Assert.assertTrue("cost(10) < cost(5)", cost10 < cost5);
    Assert.assertTrue("cost(20) < cost(10)", cost20 < cost10);
  }

  @Test
  public void testComputeCostWithZeroLag()
  {
    // With zero lag, lag cost should be 0 and only idle cost matters
    // Use idle=0.5 which when scaled by log1p(1)/log1p(2)=0.63 gives ~0.32, still in ideal range
    CostMetrics metrics = createMetrics(0.0, 10, 100, 0.5);

    double cost = costFunction.computeCost(metrics, 10, config);

    // With 0 lag and idle that stays in ideal range after estimation, cost should be very low
    Assert.assertTrue(Double.isFinite(cost));
    Assert.assertTrue("Cost should be near zero with 0 lag and ideal idle", cost < 0.1);
  }

  @Test
  public void testComputeCostWeightsAffectResult()
  {
    CostBasedAutoScalerConfig lagHeavy = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(100)
                                                                  .taskCountMin(1)
                                                                  .enableTaskAutoScaler(true)
                                                                  .lagWeight(1.0)
                                                                  .idleWeight(0.0)
                                                                  .build();

    CostBasedAutoScalerConfig idleHeavy = CostBasedAutoScalerConfig.builder()
                                                                   .taskCountMax(100)
                                                                   .taskCountMin(1)
                                                                   .enableTaskAutoScaler(true)
                                                                   .lagWeight(0.0)
                                                                   .idleWeight(1.0)
                                                                   .build();

    // Use 100k lag and idle that produces non-zero idle cost when estimated
    // With idle=0.1 and 10 tasks, estimated idle = 0.1 * log1p(1)/log1p(2) ≈ 0.063
    // idleCost = (0.2 - 0.063) / 0.2 ≈ 0.685
    CostMetrics metrics = createMetrics(100000.0, 10, 100, 0.1);

    double costLag = costFunction.computeCost(metrics, 10, lagHeavy);
    double costIdle = costFunction.computeCost(metrics, 10, idleHeavy);

    // costLag should be ~1.0 (100000 / 100000 ABSOLUTE_HIGH_LAG_THRESHOLD)
    // costIdle incorporates idle estimation scaling
    Assert.assertNotEquals("Different weights should produce different costs", costLag, costIdle, 0.0001);
    Assert.assertEquals("Lag-only cost should be lag/threshold", 1.0, costLag, 0.1);
    Assert.assertTrue("Idle-only cost should be positive", costIdle > 0.0);
  }

  @Test
  public void testEstimateIdleRatioReturnsNeutralForMissingData()
  {
    CostMetrics metrics = createMetrics(1000.0, 5, 10, -1.0);

    Assert.assertEquals(0.5, costFunction.estimateIdleRatio(metrics, 5), 0.0001);
  }

  @Test
  public void testEstimateIdleRatioReturnsZeroWhenFullyBusy()
  {
    // Exactly zero
    Assert.assertEquals(0.0, costFunction.estimateIdleRatio(createMetrics(1000.0, 5, 10, 0.0), 10), 0.0001);
    // Near zero (below threshold)
    Assert.assertEquals(0.0, costFunction.estimateIdleRatio(createMetrics(1000.0, 5, 10, 0.0005), 10), 0.0001);
  }

  @Test
  public void testEstimateIdleRatioReturnsCurrentRatioForInvalidTaskCount()
  {
    CostMetrics metrics = createMetrics(1000.0, 0, 10, 0.5);

    Assert.assertEquals(0.5, costFunction.estimateIdleRatio(metrics, 5), 0.0001);
  }

  @Test
  public void testEstimateIdleRatioLowLagLogarithmicScaling()
  {
    // Low lag: idle scales logarithmically with task ratio
    // Formula: scaleFactor = log1p(taskRatio) / log1p(2.0)
    CostMetrics metrics = createMetrics(1000.0, 10, 20, 0.2);
    final double log2 = Math.log1p(2.0);

    // Same tasks -> same idle (ratio = 1.0, scaleFactor = log1p(1)/log1p(2) ≈ 0.63)
    double expected10 = 0.2 * Math.log1p(1.0) / log2;
    Assert.assertEquals(expected10, costFunction.estimateIdleRatio(metrics, 10), 0.0001);

    // Double tasks (ratio = 2.0, scaleFactor = log1p(2)/log1p(2) = 1.0)
    double expected20 = 0.2 * Math.log1p(2.0) / log2;
    Assert.assertEquals(expected20, costFunction.estimateIdleRatio(metrics, 20), 0.0001);

    // Verify diminishing returns: 4x tasks doesn't give 4x idle
    double expected40 = 0.2 * Math.log1p(4.0) / log2;
    Assert.assertEquals(expected40, costFunction.estimateIdleRatio(metrics, 40), 0.0001);
    Assert.assertTrue(
        "4x tasks should give less than 2x the idle of 2x tasks",
        expected40 < 2 * expected20
    );
  }

  @Test
  public void testEstimateIdleRatioHighLagWithHighIdleInverseScaling()
  {
    // Inverted mode only applies when: high lag (> 1000) AND high idle (> 0.6)
    // This represents the strange case where tasks are idle despite a backlog
    CostMetrics metrics = createMetrics(50000.0, 10, 20, 0.7);
    final double log2 = Math.log1p(2.0);

    // Same tasks (ratio = 1.0, scaleFactor = 1 / (log1p(1)/log1p(2)) = 1.59)
    // predicted = 0.7 / 0.63 = 1.11 -> clamped to 1.0
    double idle10 = costFunction.estimateIdleRatio(metrics, 10);
    Assert.assertEquals(1.0, idle10, 0.0001); // Clamped to max

    // Double tasks (ratio = 2.0, scaleFactor = 1 / 1.0 = 1.0)
    // predicted = 0.7 / 1.0 = 0.7
    double logScale20 = Math.log1p(2.0) / log2;
    double expected20 = 0.7 / logScale20;
    Assert.assertEquals(expected20, costFunction.estimateIdleRatio(metrics, 20), 0.0001);

    Assert.assertTrue(
        "More tasks should decrease idle in high lag with high idle scenario",
        costFunction.estimateIdleRatio(metrics, 20) < costFunction.estimateIdleRatio(metrics, 10)
    );
  }

  @Test
  public void testEstimateIdleRatioHighLagWithLowIdleNormalScaling()
  {
    // When idle is low/ideal (not above 0.6) even with high lag, normal mode applies
    // This represents tasks that are already busy processing the backlog
    CostMetrics metrics = createMetrics(50000.0, 10, 20, 0.3);
    final double log2 = Math.log1p(2.0);

    // Normal mode: more tasks = more idle (work is divided)
    double logScale10 = Math.log1p(1.0) / log2;
    double expected10 = 0.3 * logScale10;
    Assert.assertEquals(expected10, costFunction.estimateIdleRatio(metrics, 10), 0.0001);

    double logScale20 = Math.log1p(2.0) / log2;
    double expected20 = 0.3 * logScale20;
    Assert.assertEquals(expected20, costFunction.estimateIdleRatio(metrics, 20), 0.0001);

    Assert.assertTrue(
        "More tasks should increase idle when high lag but low idle (tasks already busy)",
        costFunction.estimateIdleRatio(metrics, 20) > costFunction.estimateIdleRatio(metrics, 10)
    );
  }

  @Test
  public void testEstimateIdleRatioClampedToValidRange()
  {
    // Extreme scale-up with low lag could exceed 1.0
    CostMetrics metrics = createMetrics(1000.0, 1, 10, 0.9);

    double result = costFunction.estimateIdleRatio(metrics, 100);

    Assert.assertTrue(result >= 0.0);
    Assert.assertTrue(result <= 1.0);
  }

  @Test
  public void testEstimateIdleRatioUnstableHighIdleInvertsScaling()
  {
    // Unstable high idle scenario: idle >= UNSTABLE_HIGH_IDLE_THRESHOLD (0.95)
    // with lag > UNSTABLE_IDLE_LAG_THRESHOLD (100) but below HIGH_LAG_THRESHOLD (1000).
    // This indicates metrics haven't stabilized yet (e.g., task just started).
    // The scaling should be inverted: more tasks = less idle.
    CostMetrics metrics = createMetrics(500.0, 10, 20, 0.97);

    // With inverted scaling, more tasks should decrease predicted idle
    double idle10 = costFunction.estimateIdleRatio(metrics, 10);
    double idle20 = costFunction.estimateIdleRatio(metrics, 20);

    Assert.assertTrue(
        "More tasks should decrease idle in an unstable high idle scenario",
        idle20 < idle10
    );
  }

  @Test
  public void testEstimateIdleRatioNormalScalingWhenIdleBelowUnstableThreshold()
  {
    // When idle is high but below UNSTABLE_HIGH_IDLE_THRESHOLD (0.95),
    // and lag is moderate, normal scaling should apply: more tasks = more idle.
    CostMetrics metrics = createMetrics(500.0, 10, 20, 0.90);

    // With normal scaling, more tasks should increase predicted idle
    double idle10 = costFunction.estimateIdleRatio(metrics, 10);
    double idle20 = costFunction.estimateIdleRatio(metrics, 20);

    Assert.assertTrue(
        "More tasks should increase idle in a normal low-lag scenario",
        idle20 > idle10
    );
  }

  @Test
  public void testEstimateIdleRatioNormalScalingWhenLagBelow100()
  {
    // When idle is very high (>= 0.95) but lag is very low (<= 100),
    // this is a legitimately idle system; normal scaling should apply
    CostMetrics metrics = createMetrics(50.0, 10, 20, 0.97);

    // With normal scaling, more tasks should increase predicted idle
    double idle10 = costFunction.estimateIdleRatio(metrics, 10);
    double idle20 = costFunction.estimateIdleRatio(metrics, 20);

    Assert.assertTrue(
        "More tasks should increase idle when lag is low (system is genuinely idle)",
        idle20 > idle10
    );
  }

  @Test
  public void testComputeIdleCostZeroWithinIdealRange()
  {
    // The ideal range is [0.2, 0.6] - cost should be 0 within this range
    Assert.assertEquals(0.0, costFunction.computeIdleCost(0.2), 0.0001);
    Assert.assertEquals(0.0, costFunction.computeIdleCost(0.3), 0.0001);
    Assert.assertEquals(0.0, costFunction.computeIdleCost(0.4), 0.0001);
    Assert.assertEquals(0.0, costFunction.computeIdleCost(0.5), 0.0001);
    Assert.assertEquals(0.0, costFunction.computeIdleCost(0.6), 0.0001);
  }

  @Test
  public void testComputeIdleCostPenalizesBelowIdealRange()
  {
    // Below 0.2 (overloaded tasks) - cost increases linearly to 1.0 at 0.0
    Assert.assertEquals(1.0, costFunction.computeIdleCost(0.0), 0.0001);
    Assert.assertEquals(0.5, costFunction.computeIdleCost(0.1), 0.0001);
    Assert.assertEquals(0.25, costFunction.computeIdleCost(0.15), 0.0001);
    Assert.assertEquals(0.0, costFunction.computeIdleCost(0.2), 0.0001);
  }

  @Test
  public void testComputeIdleCostPenalizesAboveIdealRange()
  {
    // Above 0.6 (underutilized tasks) - cost increases linearly to 1.0 at 1.0
    Assert.assertEquals(0.0, costFunction.computeIdleCost(0.6), 0.0001);
    Assert.assertEquals(0.5, costFunction.computeIdleCost(0.8), 0.0001);
    Assert.assertEquals(0.75, costFunction.computeIdleCost(0.9), 0.0001);
    Assert.assertEquals(1.0, costFunction.computeIdleCost(1.0), 0.0001);
  }

  @Test
  public void testIsIdleInIdealRange()
  {
    // Ideal range is [0.2, 0.6]
    // Below ideal range
    Assert.assertFalse(WeightedCostFunction.isIdleInIdealRange(0.0));
    Assert.assertFalse(WeightedCostFunction.isIdleInIdealRange(0.1));
    Assert.assertFalse(WeightedCostFunction.isIdleInIdealRange(0.19));

    // At and within ideal range boundaries
    Assert.assertTrue(WeightedCostFunction.isIdleInIdealRange(0.2));
    Assert.assertTrue(WeightedCostFunction.isIdleInIdealRange(0.3));
    Assert.assertTrue(WeightedCostFunction.isIdleInIdealRange(0.4));
    Assert.assertTrue(WeightedCostFunction.isIdleInIdealRange(0.5));
    Assert.assertTrue(WeightedCostFunction.isIdleInIdealRange(0.6));

    // Above ideal range
    Assert.assertFalse(WeightedCostFunction.isIdleInIdealRange(0.61));
    Assert.assertFalse(WeightedCostFunction.isIdleInIdealRange(0.8));
    Assert.assertFalse(WeightedCostFunction.isIdleInIdealRange(1.0));
  }

  @Test
  public void testComputeCostPrefersIdealIdleRange()
  {
    // Test that computeIdleCost properly penalizes values outside ideal range [0.2, 0.6]
    // Use idle cost directly to verify the target range behavior
    double idealCost = costFunction.computeIdleCost(0.4);      // In ideal range
    double overloadedCost = costFunction.computeIdleCost(0.1); // Below ideal
    double underutilizedCost = costFunction.computeIdleCost(0.8); // Above ideal

    Assert.assertEquals("Ideal range should have zero cost", 0.0, idealCost, 0.0001);
    Assert.assertTrue("Overloaded should have positive cost", overloadedCost > 0.0);
    Assert.assertTrue("Underutilized should have positive cost", underutilizedCost > 0.0);
    Assert.assertTrue("Ideal should have lower cost than overloaded", idealCost < overloadedCost);
    Assert.assertTrue("Ideal should have lower cost than underutilized", idealCost < underutilizedCost);
  }

  @Test
  public void testExtremeLagForcesScaleUpEvenWithIdealIdle()
  {
    // With 1M lag, even with perfect idle, we should scale up
    // lagCost = 1,000,000 / 100,000 = 10.0 (unbounded, dominates idle)
    // idle=0.4 -> estimated ~0.25 (in ideal range) -> idleCost=0
    CostMetrics extremeLagMetrics = createMetrics(1_000_000.0, 10, 100, 0.4);

    double cost10 = costFunction.computeCost(extremeLagMetrics, 10, config);
    double cost50 = costFunction.computeCost(extremeLagMetrics, 50, config);

    // Even with ideal idle (cost = 0), the extreme lag should force scale up
    Assert.assertTrue("More tasks should reduce cost even with ideal idle", cost50 < cost10);

    // With lagWeight=0.3, lagCost=10 -> lagComponent = 3.0
    // With idleWeight=0.7, idleCost=0 -> idleComponent = 0
    // Total cost = 3.0
    Assert.assertEquals("Extreme lag should produce cost of 3.0", 3.0, cost10, 0.01);
  }

  @Test
  public void testLagCostUsesAbsoluteThreshold()
  {
    // Verify lag cost = predictedLag / ABSOLUTE_HIGH_LAG_THRESHOLD (100,000)
    CostBasedAutoScalerConfig lagOnly = CostBasedAutoScalerConfig.builder()
                                                                  .taskCountMax(100)
                                                                  .taskCountMin(1)
                                                                  .enableTaskAutoScaler(true)
                                                                  .lagWeight(1.0)
                                                                  .idleWeight(0.0)
                                                                  .build();

    // With taskCount == currentTaskCount, predictedLag == avgPartitionLag (scaleFactor = 1.0)
    // So lagCost = avgPartitionLag / 100,000

    // 100k lag should give lagCost = 1.0
    CostMetrics metrics100k = createMetrics(100_000.0, 10, 100, 0.4);
    double cost100k = costFunction.computeCost(metrics100k, 10, lagOnly);
    Assert.assertEquals("100k lag should give cost 1.0", 1.0, cost100k, 0.0001);

    // 1M lag should give lagCost = 10.0
    CostMetrics metrics1M = createMetrics(1_000_000.0, 10, 100, 0.4);
    double cost1M = costFunction.computeCost(metrics1M, 10, lagOnly);
    Assert.assertEquals("1M lag should give cost 10.0", 10.0, cost1M, 0.0001);

    // 10k lag should give lagCost = 0.1
    CostMetrics metrics10k = createMetrics(10_000.0, 10, 100, 0.4);
    double cost10k = costFunction.computeCost(metrics10k, 10, lagOnly);
    Assert.assertEquals("10k lag should give cost 0.1", 0.1, cost10k, 0.0001);
  }

  @Test
  public void testHighLagThresholdConstant()
  {
    // Verify the ABSOLUTE_HIGH_LAG_THRESHOLD is 100,000
    Assert.assertEquals(100_000.0, WeightedCostFunction.ABSOLUTE_HIGH_LAG_THRESHOLD, 0.0);
  }

  private CostMetrics createMetrics(
      double avgPartitionLag,
      int currentTaskCount,
      int partitionCount,
      double pollIdleRatio
  )
  {
    return new CostMetrics(avgPartitionLag, currentTaskCount, partitionCount, pollIdleRatio);
  }
}
