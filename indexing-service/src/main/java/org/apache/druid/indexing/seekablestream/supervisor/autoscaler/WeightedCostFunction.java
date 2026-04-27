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
import org.apache.druid.java.util.common.logger.Logger;

/**
 * Weighted cost function using compute time as the core metric.
 * Lag cost is based on recovery time in seconds; idle cost is a penalty derived from
 * the predicted idle ratio.
 *
 * <p>Idle cost uses a U-shaped penalty with minimum at {@link #IDEAL_IDLE_RATIO}.
 * This penalizes both under-provisioning (low idle, no safety margin, lag risk) and
 * over-provisioning (high idle, wasted capacity), with asymmetric severity controlled by
 * {@link #UNDER_PROVISIONING_PENALTY} and {@link #OVER_PROVISIONING_PENALTY}.
 */
public class WeightedCostFunction
{
  private static final Logger log = new Logger(WeightedCostFunction.class);

  /**
   * Multiplier for a lag amplification factor; it was carefully chosen
   * during extensive testing as the most balanced multiplier for high-lag recovery.
   */
  static final double LAG_AMPLIFICATION_MULTIPLIER = 0.4;

  /**
   * Minimum rate of processing for any task in records per second. This is used
   * as a placeholder if avg rate is not available to ensure that cost computations
   * do not return infinitely large lag recovery times.
   */
  static final double MIN_PROCESSING_RATE = 1_000;

  /**
   * Target idle ratio representing the optimal operating point for the U-shaped idle cost.
   * At this ratio the idle cost is at its minimum; both lower (risk) and higher (waste) are penalized.
   */
  static final double IDEAL_IDLE_RATIO = 0.25;

  /**
   * Penalty magnitude applied when idle ratio is 0 (no safety margin).
   * Controls the steepness of the U-shape on the under-provisioning side.
   */
  static final double UNDER_PROVISIONING_PENALTY = 2.0;

  /**
   * Penalty magnitude applied when idle ratio is 1 (fully wasted capacity).
   * Controls the steepness of the U-shape on the over-provisioning side.
   */
  static final double OVER_PROVISIONING_PENALTY = 1.0;

  /**
   * Computes cost for a given task count using compute time metrics.
   * <p>
   * Cost components are derived from:
   * <ul>
   *   <li><b>lagCost</b>: weighted expected time to recover current lag</li>
   *   <li><b>idleCost</b>: weighted U-shaped penalty for the predicted idle ratio</li>
   * </ul>
   * <p>
   * Formula: {@code lagWeight * lagRecoveryTime + idleWeight * idleRatioPenalty}.
   *
   * @return CostResult containing totalCost, lagCost, and idleCost,
   * or result with {@link Double#POSITIVE_INFINITY} for invalid inputs
   */
  public CostResult computeCost(
      CostMetrics metrics,
      int proposedTaskCount,
      CostBasedAutoScalerConfig config
  )
  {
    if (metrics == null || config == null || proposedTaskCount <= 0 || metrics.getPartitionCount() <= 0) {
      return CostResult.INFINITE_COST;
    }

    final double avgProcessingRate = metrics.getAvgProcessingRate();
    if (avgProcessingRate < 0) {
      throw DruidException.defensive("Avg processing rate[%.2f] must not be negative.", avgProcessingRate);
    }

    // Lag recovery time is decreasing by adding tasks and increasing by ejecting tasks.
    // In case of increasing lag, we apply an amplification factor to reflect the urgency of addressing lag.
    // Caution: we rely only on the metrics, the real issues may be absolutely different, up to hardware failure.
    final double lagRecoveryTime;
    if (metrics.getAggregateLag() <= 0) {
      lagRecoveryTime = 0;
    } else {
      final double lagPerPartition = metrics.getAggregateLag() / metrics.getPartitionCount();
      final double amplification = Math.max(1.0, 1.0 + LAG_AMPLIFICATION_MULTIPLIER * Math.log(lagPerPartition));
      final double adjustedProcessingRate = Math.max(avgProcessingRate, MIN_PROCESSING_RATE);
      lagRecoveryTime = metrics.getAggregateLag() * amplification / (proposedTaskCount * adjustedProcessingRate);
    }

    // Capacity-based idle prediction. When the proposed count would oversaturate the cluster
    // (busy work exceeds available capacity), the unmet demand becomes a virtual lag-recovery
    // time on the same axis as real lag — so the optimizer treats predicted saturation as
    // predicted lag, not as "perfect utilization".
    final double currentPollIdleRatio = metrics.getPollIdleRatio();
    final int currentTaskCount = metrics.getCurrentTaskCount();
    final double predictedIdleRatio;
    final double overrun;
    if (currentPollIdleRatio < 0) {
      predictedIdleRatio = 0.5;
      overrun = 0.0;
    } else if (currentTaskCount <= 0 || proposedTaskCount == currentTaskCount) {
      predictedIdleRatio = currentPollIdleRatio;
      overrun = 0.0;
    } else {
      final double busyFraction = 1.0 - currentPollIdleRatio;
      final double taskRatio = (double) proposedTaskCount / currentTaskCount;
      final double rawIdle = 1.0 - busyFraction / taskRatio;
      if (rawIdle >= 0) {
        predictedIdleRatio = Math.min(1.0, rawIdle);
        overrun = 0.0;
      } else {
        predictedIdleRatio = 0.0;
        overrun = -rawIdle;
      }
    }
    final double virtualLagRecoveryTime = overrun * metrics.getTaskDurationSeconds();

    final double idleCost = uShapedIdleCost(predictedIdleRatio, proposedTaskCount);
    final double lagCost = config.getLagWeight() * (lagRecoveryTime + virtualLagRecoveryTime);
    final double weightedIdleCost = config.getIdleWeight() * idleCost;
    final double cost = lagCost + weightedIdleCost;

    log.debug(
        "Cost for taskCount[%d]: lagCost[%.2fs], idleCost[%.2fs], "
        + "predictedIdle[%.3f], overrun[%.3f], finalCost[%.2fs]",
        proposedTaskCount,
        lagCost,
        weightedIdleCost,
        predictedIdleRatio,
        overrun,
        cost
    );

    return new CostResult(cost, lagCost, weightedIdleCost);
  }

  /**
   * U-shaped idle cost with minimum at {@link #IDEAL_IDLE_RATIO}.
   *
   * <ul>
   *   <li>idle &lt; ideal: under-provisioning penalty, no safety margin, lag risk</li>
   *   <li>idle = ideal: baseline cost only ({@code taskCount * IDEAL_IDLE_RATIO})</li>
   *   <li>idle &gt; ideal: over-provisioning penalty, wasted capacity</li>
   * </ul>
   * <p>
   * The ideal-idle baseline keeps cost non-zero at the optimum so the optimizer
   * always has a finite trade-off against lag cost.
   */
  double uShapedIdleCost(double predictedIdleRatio, int taskCount)
  {
    final double penalty;
    if (predictedIdleRatio < IDEAL_IDLE_RATIO) {
      final double norm = (IDEAL_IDLE_RATIO - predictedIdleRatio) / IDEAL_IDLE_RATIO;
      penalty = UNDER_PROVISIONING_PENALTY * norm * norm;
    } else {
      final double norm = (predictedIdleRatio - IDEAL_IDLE_RATIO) / (1.0 - IDEAL_IDLE_RATIO);
      penalty = OVER_PROVISIONING_PENALTY * norm * norm;
    }
    return taskCount * (IDEAL_IDLE_RATIO + penalty);
  }

}
