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

package org.apache.druid.indexing.seekablestream.supervisor.autoscaler.plugins;

public final class BurstScaleUpOnHighLagPlugin
{

  // Base PPT lag threshold allowing to activate a burst scaleup to eliminate high lag.
  public static final int EXTRA_SCALING_LAG_PER_PARTITION_THRESHOLD = 50_000;

  /**
   * Controls how fast the additional tasks grow with the square root of current tasks.
   * This allows bigger jumps when under-provisioned, but growth slows down as the task count increases.
   */
  private static final int SQRT_TASK_COUNT_SCALE_FACTOR = 3;

  private final int lagThreshold;

  public BurstScaleUpOnHighLagPlugin(int lagThreshold)
  {
    this.lagThreshold = lagThreshold;
  }

  public int lagThreshold()
  {
    return lagThreshold;
  }

  /**
   * Computes extra allowed increase in partitions-per-task in scenarios when the average per-partition lag
   * is above the configured threshold.
   * <p>
   * This uses a capped sqrt-based formula:
   * {@code additionalTasks = min(MAX_JUMP, BASE + sqrt(currentTasks) * SQRT_COEFF) * lagFactor * headroom}
   * <p>
   * This ensures:
   * 1. Bigger jumps are allowed when under-provisioned.
   * 2. Sqrt growth (additional tasks grow slower than task count).
   * 3. Self-damping (headroomRatio reduces jumps near max capacity).
   */
  public int computeScaleUpBoost(
      double aggregateLag,
      int partitionCount,
      int currentTaskCount,
      int taskCountMax
  )
  {
    if (partitionCount <= 0 || taskCountMax <= 0 || currentTaskCount <= 0) {
      return 0;
    }

    final double lagPerPartition = aggregateLag / partitionCount;
    if (lagPerPartition < lagThreshold) {
      return 0;
    }

    final double lagSeverity = lagPerPartition / lagThreshold;
    final double lagFactor = lagSeverity / (lagSeverity + 1.0);
    // Use quadratic headroom damping to maintain higher pressure near capacity
    final double headroomRatio = Math.max(0.0, 1.0 - Math.pow((double) currentTaskCount / taskCountMax, 2));

    // Compute target additional tasks (sqrt-based growth)
    final double rawAdditional = 1.0 + Math.sqrt(currentTaskCount) * SQRT_TASK_COUNT_SCALE_FACTOR;
    final double deltaTasks = rawAdditional * lagFactor * headroomRatio;

    final double targetTaskCount = Math.min((double) taskCountMax, (double) currentTaskCount + deltaTasks);

    // Compute precise PPT reduction to avoid early integer truncation artifacts
    final double currentPPT = (double) partitionCount / currentTaskCount;
    final double targetPPT = (double) partitionCount / targetTaskCount;

    return Math.max(0, (int) Math.floor(currentPPT - targetPPT));
  }
}
