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

@SuppressWarnings("ClassCanBeRecord")
public final class BurstScaleUpOnHighLagPlugin
{

  // Base PPT lag threshold allowing to activate a burst scaleup to eliminate high lag.
  public static final int EXTRA_SCALING_LAG_PER_PARTITION_THRESHOLD = 50_000;

  /**
   * Divisor for partition count in the K formula: K = (partitionCount / K_PARTITION_DIVISOR) / sqrt(currentTaskCount).
   * This controls how aggressive the scaling is relative to partition count.
   * That value was chosen by carefully analyzing the math model behind the implementation.
   */
  private static final double K_PARTITION_DIVISOR = 6.4;

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
   * This uses a logarithmic formula for consistent absolute growth:
   * {@code deltaTasks = K * ln(lagSeverity)}
   * where {@code K = (partitionCount / 6.4) / sqrt(currentTaskCount)}
   * <p>
   * This ensures:
   * 1. Partition-aware scaling: larger datasets get more aggressive scaling.
   * 2. Small taskCount's get a massive relative boost, while large taskCount's receive more measured, stable increases.
   * 3. Logarithmic lag response: diminishing returns at extreme lag values.
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


    // Logarithmic growth: ln(lagSeverity) is positive when lagSeverity > 1
    // First multoplier decreases with sqrt(currentTaskCount): aggressive when small, conservative when large
    final double deltaTasks = (partitionCount / K_PARTITION_DIVISOR) / Math.sqrt(currentTaskCount) * Math.log(lagSeverity);

    final double targetTaskCount = Math.min(taskCountMax, (double) currentTaskCount + deltaTasks);

    // Compute precise PPT reduction to avoid early integer truncation artifacts
    final double currentPPT = (double) partitionCount / currentTaskCount;
    final double targetPPT = (double) partitionCount / targetTaskCount;

    return Math.max(0, (int) Math.floor(currentPPT - targetPPT));
  }
}
