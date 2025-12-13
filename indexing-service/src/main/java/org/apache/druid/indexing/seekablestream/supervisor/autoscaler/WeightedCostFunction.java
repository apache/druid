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

  /**
   * Lag threshold above which we consider the system to have significant backlog.
   */
  private static final double HIGH_LAG_THRESHOLD = 1_000.0;

  /**
   * Minimum lag threshold for unstable high idle detection.
   * When idle is very high (≥95%) but the lag exceeds this, metrics likely haven't stabilized.
   */
  private static final double UNSTABLE_IDLE_LAG_THRESHOLD = 100.0;

  /**
   * Threshold for considering a task "fully busy" (effectively zero idle).
   */
  private static final double FULLY_BUSY_IDLE_THRESHOLD = 0.001;
  /**
   * Threshold for detecting unstable high idle (metrics not yet stabilized).
   */
  private static final double UNSTABLE_HIGH_IDLE_THRESHOLD = 0.95;


  /**
   * Ideal idle ratio range boundaries.
   * Idle ratio below MIN indicates tasks are overloaded (scale up needed).
   * Idle ratio above MAX indicates tasks are underutilized (scale down needed).
   */
  static final double IDEAL_IDLE_MIN = 0.2;
  static final double IDEAL_IDLE_MAX = 0.6;

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
   * Checks if the given idle ratio is within the ideal range [{@value #IDEAL_IDLE_MIN}, {@value #IDEAL_IDLE_MAX}].
   * When idle is in this range, optimal utilization has been achieved and no scaling is needed.
   */
  public static boolean isIdleInIdealRange(double idleRatio)
  {
    return idleRatio >= IDEAL_IDLE_MIN && idleRatio <= IDEAL_IDLE_MAX;
  }

  /**
   * Computes cost for a given task count (lower is better).
   * <p>
   * Formula: {@code lagWeight * normalizedLag + idleWeight * idleCost}
   * <p>
   * The idle cost uses a target range approach where idle ratios within
   * [{@value #IDEAL_IDLE_MIN}, {@value #IDEAL_IDLE_MAX}] have zero cost,
   * while values outside this range incur increasing penalties.
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
    final double idleCost = computeIdleCost(predictedIdleRatio);

    final double cost = config.getLagWeight() * normalizedLag + config.getIdleWeight() * idleCost;

    log.info(
        "Cost for taskCount[%d]: lag[%.2f -> %.4f], idle[%.4f -> %.4f], final cost[%.4f]",
        taskCount,
        predictedLag,
        normalizedLag,
        predictedIdleRatio,
        idleCost,
        cost
    );

    return cost;
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
      // If bounds are invalid or identical, return zero value
      return 0.0;
    }

    return clamp((value - min) / range);
  }

  /**
   * Predicts lag for a given task count using logarithmic scaling.
   * <p>
   * Models the real-world observation that adding more tasks has diminishing returns
   * on lag reduction due to processing overhead, coordination costs, and I/O bottlenecks.
   * <p>
   * <b>Formula:</b> {@code predictedLag = currentLag * log1p(2.0) / log1p(1 + taskRatio)}
   * <p>
   * <b>Scaling behavior</b> (50 partitions, starting from 10 tasks with lag of 100):
   * <pre>
   * | Task Ratio | Tasks | Predicted Lag | Effect             |
   * |------------|-------|---------------|--------------------|
   * | 0.2x       | 2     | 229           | Increased lag      |
   * | 0.5x       | 5     | 158           | Increased lag      |
   * | 1.0x       | 10    | 100           | Same (baseline)    |
   * | 1.7x       | 17    | 100           | Conservative       |
   * | 2.5x       | 25    | 91            | Diminishing returns|
   * | 5.0x       | 50    | 74            | Diminishing returns|
   * </pre>
   * <p>
   * The formula is normalized so that doubling tasks (2x) produces a scale factor of 1.0,
   * meaning the predicted lag equals the current lag. This conservative baseline accounts for
   * the overhead of task coordination.
   *
   * @param metrics   current system metrics containing current lag and task count
   * @param taskCount target task count to predict lag for
   * @return predicted average partition lag (always positive)
   */
  private double predictLag(CostMetrics metrics, int taskCount)
  {
    final int currentTaskCount = Math.max(1, metrics.getCurrentTaskCount());
    final double taskRatio = (double) taskCount / currentTaskCount;

    // Inverse logarithmic scaling: more tasks = less lag, with diminishing returns
    // Normalized so that 2x tasks gives scaleFactor = 1.0
    final double logScaleFactor = Math.log1p(2.0) / Math.log1p(1.0 + taskRatio);

    return metrics.getAvgPartitionLag() * logScaleFactor;
  }

  /**
   * Computes cost based on how far the idle ratio deviates from the ideal range.
   * Deviations increase the cost linearly.
   * @param idleRatio predicted idle ratio in range [0.0, 1.0]
   * @return cost in range [0.0, 1.0], where 0 means ideal utilization
   */
  double computeIdleCost(double idleRatio)
  {
    if (idleRatio < IDEAL_IDLE_MIN) {
      // Below ideal: scale from 0 at IDEAL_IDLE_MIN to 1.0 at 0.0
      return (IDEAL_IDLE_MIN - idleRatio) / IDEAL_IDLE_MIN;
    } else if (idleRatio > IDEAL_IDLE_MAX) {
      // Above ideal: scale from 0 at IDEAL_IDLE_MAX to 1.0 at 1.0
      return (idleRatio - IDEAL_IDLE_MAX) / (1.0 - IDEAL_IDLE_MAX);
    } else {
      // Within the ideal range: no cost
      return 0.0;
    }
  }

  /**
   * Estimates the idle ratio for a given task count using logarithmic scaling.
   * <p>
   * The relationship between task count and idle ratio depends on current idle and lag levels:
   * <ul>
   *   <li><b>Normal mode:</b> More tasks = more idle (work is divided among more workers)</li>
   *   <li><b>Inverted mode:</b> More tasks = less idle (all busy processing backlog)</li>
   * </ul>
   * <p>
   * <b>Mode selection:</b>
   * <ul>
   *   <li>Normal mode: low lag, OR high lag with low/ideal idle (tasks already busy)</li>
   *   <li>Inverted mode: high lag (&gt; {@value #HIGH_LAG_THRESHOLD}) with high idle (&gt; {@value #IDEAL_IDLE_MAX}),
   *       OR unstable high idle (≥ {@value #UNSTABLE_HIGH_IDLE_THRESHOLD} with lag &gt; {@value #UNSTABLE_IDLE_LAG_THRESHOLD})</li>
   * </ul>
   * <p>
   * <b>Formula:</b> {@code scaleFactor = log1p(taskRatio) / log1p(2.0)}
   * <p>
   * <b>Normal mode scaling</b> (50 partitions, starting from 10 tasks with 0.2 idle ratio):
   * <pre>
   * | Task Ratio | Tasks | Predicted Idle | Effect             |
   * |------------|-------|----------------|--------------------|
   * | 0.2x       | 2     | 0.06           | Decreased idle     |
   * | 0.5x       | 5     | 0.09           | Decreased idle     |
   * | 1.0x       | 10    | 0.13           | Baseline           |
   * | 1.7x       | 17    | 0.18           | Slight increase    |
   * | 2.5x       | 25    | 0.23           | Moderate increase  |
   * | 5.0x       | 50    | 0.33           | Diminishing returns|
   * </pre>
   * <p>
   * <b>Inverted mode scaling</b> (high lag with high idle - tasks idle despite backlog):
   * <pre>
   * | Task Ratio | Tasks | Idle Change                    |
   * |------------|-------|--------------------------------|
   * | 0.2x       | 2     | Idle increases (fewer workers) |
   * | 2.5x       | 25    | Idle decreases (more workers)  |
   * | 5.0x       | 50    | Idle decreases further         |
   * </pre>
   * <p>
   * <b>Unstable high idle detection:</b> When tasks report very high idle
   * (≥ {@value #UNSTABLE_HIGH_IDLE_THRESHOLD}) but there's significant lag
   * (&gt; {@value #UNSTABLE_IDLE_LAG_THRESHOLD}), this typically indicates metrics haven't
   * stabilized yet (e.g., a task just started). The scaling relationship is inverted because
   * the idle ratio is artificially inflated and will decrease as tasks start processing the backlog.
   * <p>
   * The logarithmic scaling prevents extreme predictions and models the diminishing
   * returns of adding more tasks.
   *
   * @param metrics   current system metrics containing idle ratio, lag, and task count
   * @param taskCount target task count to estimate an idle ratio for
   * @return estimated idle ratio in range [0.0, 1.0]
   */
  double estimateIdleRatio(CostMetrics metrics, int taskCount)
  {
    final double currentPollIdleRatio = metrics.getPollIdleRatio();

    if (currentPollIdleRatio < 0) {
      return 0.5;
    }

    if (currentPollIdleRatio < FULLY_BUSY_IDLE_THRESHOLD) {
      return 0.0;
    }

    final int currentTaskCount = metrics.getCurrentTaskCount();
    if (currentTaskCount <= 0) {
      return currentPollIdleRatio;
    }

    // Use logarithmic scaling for diminishing returns effect
    final double taskRatio = (double) taskCount / currentTaskCount;
    final double logScaleFactor = Math.log1p(taskRatio) / Math.log1p(2.0);

    final double avgLag = metrics.getAvgPartitionLag();
    final boolean shouldInvertScaling =
        (avgLag > HIGH_LAG_THRESHOLD && currentPollIdleRatio > IDEAL_IDLE_MAX)
        || (currentPollIdleRatio >= UNSTABLE_HIGH_IDLE_THRESHOLD && avgLag > UNSTABLE_IDLE_LAG_THRESHOLD);

    final double scaleFactor = shouldInvertScaling ? (1.0 / logScaleFactor) : logScaleFactor;

    return clamp(currentPollIdleRatio * scaleFactor);
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
