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

import org.apache.druid.java.util.common.logger.Logger;

/**
 * Weighted cost function using compute time as the core metric.
 * Costs represent actual time in seconds, making them intuitive and debuggable.
 * Uses linear scaling without mode inversions for predictable behavior.
 */
public class WeightedCostFunction
{
  private static final Logger log = new Logger(WeightedCostFunction.class);


  /**
   * Ideal idle ratio range boundaries.
   * Idle ratio below MIN indicates tasks are overloaded (scale up needed).
   * Idle ratio above MAX indicates tasks are underutilized (scale down needed).
   */
  static final double IDEAL_IDLE_MIN = 0.2;
  static final double IDEAL_IDLE_MAX = 0.6;

  /**
   * Checks if the given idle ratio is within the ideal range [{@value #IDEAL_IDLE_MIN}, {@value #IDEAL_IDLE_MAX}].
   * When idle is in this range, optimal utilization has been achieved and no scaling is needed.
   */
  public static boolean isIdleInIdealRange(double idleRatio)
  {
    return idleRatio >= IDEAL_IDLE_MIN && idleRatio <= IDEAL_IDLE_MAX;
  }

  /**
   * Computes cost for a given task count using compute time metrics.
   * <p>
   * Costs are measured in 'seconds':
   * <ul>
   *   <li><b>lagCost</b>: Expected time (seconds) to recover current lag</li>
   *   <li><b>idleCost</b>: Total compute time (seconds) wasted being idle per task duration</li>
   * </ul>
   * <p>
   * Formula: {@code lagWeight * lagRecoveryTime + idleWeight * idlenessCost}.
   * This approach directly connects costs to operational metrics.
   *
   * @return CostResult containing totalCost, lagCost, and idleCost,
   * or result with {@link Double#POSITIVE_INFINITY} for invalid inputs
   */
  public CostResult computeCost(CostMetrics metrics, int proposedTaskCount, CostBasedAutoScalerConfig config)
  {
    if (metrics == null || config == null || proposedTaskCount <= 0 || metrics.getPartitionCount() <= 0) {
      return new CostResult(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    final double avgProcessingRate = metrics.getAvgProcessingRate();
    final double lagRecoveryTime;
    if (avgProcessingRate <= 0) {
      // Metrics are unavailable - favor maintaining the current task count.
      // We're conservative about scale up, but won't let an unlikey scale down to happen.
      if (proposedTaskCount == metrics.getCurrentTaskCount()) {
        return new CostResult(0.01d, 0.0, 0.0);
      } else {
        return new CostResult(Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY, Double.POSITIVE_INFINITY);
      }
    } else {
      // Lag recovery time is decreasing by adding tasks and increasing by ejecting tasks.
      // Caution: we rely only on the metrics, the real issues may be absolutely different, up to hardware failure.
      lagRecoveryTime = metrics.getAggregateLag() / (proposedTaskCount * avgProcessingRate);
    }

    final double predictedIdleRatio = estimateIdleRatio(metrics, proposedTaskCount);
    final double idleCost = proposedTaskCount * metrics.getTaskDurationSeconds() * predictedIdleRatio;
    final double lagCost = config.getLagWeight() * lagRecoveryTime;
    final double weightedIdleCost = config.getIdleWeight() * idleCost;
    final double cost = lagCost + weightedIdleCost;

    log.debug(
        "Cost for taskCount[%d]: lagCost[%.2fs], idleCost[%.2fs], "
        + "predictedIdle[%.3f], finalCost[%.2fs]",
        proposedTaskCount,
        lagCost,
        weightedIdleCost,
        predictedIdleRatio,
        cost
    );

    return new CostResult(cost, lagCost, weightedIdleCost);
  }


  /**
   * Estimates the idle ratio for a given task count using a capacity-based linear model.
   * <p>
   * Formula: {@code predictedIdle = 1 - busyFraction / taskRatio}
   * where {@code busyFraction = 1 - currentIdleRatio} and {@code taskRatio = targetTaskCount / currentTaskCount}.
   *
   * @param metrics   current system metrics containing idle ratio and task count
   * @param taskCount target task count to estimate an idle ratio for
   * @return estimated idle ratio in range [0.0, 1.0]
   */
  private double estimateIdleRatio(CostMetrics metrics, int taskCount)
  {
    final double currentPollIdleRatio = metrics.getPollIdleRatio();

    // Handle edge cases
    if (currentPollIdleRatio < 0) {
      // No idle data available, assume moderate idle
      return 0.5;
    }

    final int currentTaskCount = metrics.getCurrentTaskCount();
    if (currentTaskCount <= 0 || taskCount == currentTaskCount) {
      return currentPollIdleRatio;
    }

    // Capacity-based model: idle ratio reflects spare capacity per task
    final double busyFraction = 1.0 - currentPollIdleRatio;
    final double taskRatio = (double) taskCount / currentTaskCount;
    final double predictedIdleRatio = 1.0 - busyFraction / taskRatio;

    // Clamp to valid range [0, 1]
    return Math.max(0.0, Math.min(1.0, predictedIdleRatio));
  }

}
