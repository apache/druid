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
 * Weighted cost function combining lag, idle time, and change distance metrics.
 * Uses adaptive bounds for normalization based on recent history.
 */
public class WeightedCostFunction
{
  private static final Logger log = new Logger(WeightedCostFunction.class);
  private static final double HIGH_LAG_THRESHOLD = 200_000.0;
  private static final double MIN_SCALE_FACTOR = 0.1;

  private final AdaptiveBounds lagBounds;

  public WeightedCostFunction()
  {
    this.lagBounds = new AdaptiveBounds();
  }

  private static double clamp(double value)
  {
    return Math.max(0.0, Math.min(1.0, value));
  }

  /**
   * Computes cost for a given task count (lower is better).
   * Formula: <code>lagWeight * normalizedLag + idleWeight * predictedIdleRatio</code>
   *
   * @return cost score, or {@link Double#POSITIVE_INFINITY} for invalid inputs
   */
  public double computeCost(CostMetrics metrics, int taskCount, CostBasedAutoScalerConfig config)
  {
    if (metrics == null || config == null || taskCount <= 0 || metrics.getPartitionCount() <= 0) {
      return Double.POSITIVE_INFINITY;
    }

    final double predictedLag = predictLag(metrics, taskCount);
    final double normalizedLag = normalize(predictedLag, lagBounds);
    final double predictedIdleRatio = estimateIdleRatio(metrics, taskCount);

    // Note: idle ratio is already in ranges [0,1], no normalization needed
    final double cost = config.getLagWeight() * normalizedLag + config.getIdleWeight() * predictedIdleRatio;

    log.debug(
        "Cost calculation for taskCount[%d]: predictedLag[%.2f], normalizedLag[%.4f], predictedIdleRatio[%.4f], cost[%.4f]",
        taskCount,
        predictedLag,
        normalizedLag,
        predictedIdleRatio,
        cost
    );

    return cost;
  }

  /**
   * Predicts lag for a given task count using linear scaling.
   * Assumes lag is inversely proportional to task count.
   *
   * @param metrics   current system metrics
   * @param taskCount target task count
   * @return predicted average partition lag
   */
  private double predictLag(CostMetrics metrics, int taskCount)
  {
    final int currentTaskCount = Math.max(1, metrics.getCurrentTaskCount());
    final double scaleFactor = (double) currentTaskCount / taskCount;
    return metrics.getAvgPartitionLag() * scaleFactor;
  }

  /**
   * Estimates the idle ratio for a given task count.
   * Considers current idle ratio, partition distribution, and lag levels.
   * <p>
   * The relationship between task count and idle ratio depends on lag:
   * - High lag: More tasks = less idle (inverse relationship)
   * - Low lag: More tasks = more idle (normal relationship)
   *
   * @param metrics   current system metrics
   * @param taskCount target task count
   * @return estimated idle ratio in range [0.0, 1.0]
   */
  private double estimateIdleRatio(CostMetrics metrics, int taskCount)
  {
    final double currentPollIdleRatio = metrics.getPollIdleRatio();
    final double newPartitionsPerTask = (double) metrics.getPartitionCount() / taskCount;
    final double currentPartitionsPerTask = metrics.getCurrentTaskCount() > 0
                                            ? (double) metrics.getPartitionCount() / metrics.getCurrentTaskCount()
                                            : metrics.getPartitionCount();

    // If no idle ratio data, estimate based on partition distribution
    if (currentPollIdleRatio < 0) {
      // Inverse relationship: fewer partitions per task = more idle time
      return clamp(1.0 / newPartitionsPerTask);
    }

    // When pollIdleRatio is exactly 0, tasks are busy processing (not missing data)
    // In this case, assume tasks will remain busy regardless of task count changes
    if (currentPollIdleRatio == 0) {
      return 0.0;
    }

    // When there's significant lag, MORE tasks = LESS idle (all working on backlog)
    // When there's moderate/low lag, MORE tasks = MORE idle (waiting for data)
    final boolean hasHighLag = metrics.getAvgPartitionLag() > HIGH_LAG_THRESHOLD;

    if (hasHighLag) {
      // High lag: Inverse relationship - more tasks = less idle (everyone busy processing backlog)
      final double ratio = newPartitionsPerTask / currentPartitionsPerTask;
      final double scaleFactor = Math.sqrt(Math.max(MIN_SCALE_FACTOR, ratio));
      return clamp(currentPollIdleRatio * scaleFactor);
    } else {
      // Moderate/low lag: Normal relationship - more tasks = more idle (waiting for data)
      final double ratio = currentPartitionsPerTask / newPartitionsPerTask;
      final double scaleFactor = Math.sqrt(Math.max(MIN_SCALE_FACTOR, ratio));
      return clamp(currentPollIdleRatio * scaleFactor);
    }
  }

  /**
   * Normalizes a value to the range [0.0, 1.0] using adaptive bounds.
   */
  private double normalize(double value, AdaptiveBounds bounds)
  {
    final double min = bounds.getMin();
    final double max = bounds.getMax();
    final double range = max - min;

    // Handle edge cases
    if (range <= 0 || Double.isInfinite(range) || Double.isNaN(range)) {
      // If bounds are invalid or identical, return middle value
      return 0.5;
    }

    return clamp((value - min) / range);
  }

  /**
   * Updates the lag bounds with an observed lag value from actual metrics.
   * This should ONLY be called with observed lag values, NOT predicted values.
   * The bounds are used for normalization tracking in cost calculations.
   */
  public void updateLagBounds(double observedLag)
  {
    lagBounds.update(observedLag);
  }

  /**
   * Maintains min/max bounds for normalization.
   */
  static class AdaptiveBounds
  {
    private double min;
    private double max;

    AdaptiveBounds()
    {
      this.min = Float.POSITIVE_INFINITY;
      this.max = 0.0;
    }

    void update(double value)
    {
      if (value < min) {
        min = value;
      }
      if (value > max) {
        max = value;
      }
    }

    double getMin()
    {
      return min;
    }

    double getMax()
    {
      return max;
    }
  }
}
