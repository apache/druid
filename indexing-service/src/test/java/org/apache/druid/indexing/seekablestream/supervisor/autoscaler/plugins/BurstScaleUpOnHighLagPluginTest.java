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

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link BurstScaleUpOnHighLagPlugin}.
 * <p>
 * The plugin uses a logarithmic formula for burst scaling:
 * {@code deltaTasks = K * ln(lagSeverity)}
 * where {@code K = (partitionCount / 6.4) / sqrt(currentTaskCount)}
 */
public class BurstScaleUpOnHighLagPluginTest
{
  private static final int LAG_THRESHOLD = 50_000;
  private static final int PARTITION_COUNT = 48;
  private static final int TASK_COUNT_MAX = 48;

  /**
   * Tests scaling behavior across different lag levels and task counts.
   * <p>
   * Expected behavior for 48 partitions with threshold=50K:
   * <pre>
   * | Current | Lag/Part | Boost | Notes                                      |
   * |---------|----------|-------|---------------------------------------------|
   * | any     | <50K     | 0     | Below threshold                             |
   * | any     | =50K     | 0     | ln(1) = 0                                   |
   * | 1       | 100K     | 40    | Significant boost for emergency recovery    |
   * | 1       | 200K     | 43    | Large boost                                 |
   * | 4       | 200K     | 6     | Moderate boost (K decreases with sqrt(C))   |
   * | 12      | 200K     | 0     | Delta too small for PPT change              |
   * | 24      | 200K     | 0     | Delta too small for PPT change              |
   * </pre>
   * At high task counts (C=12, C=24), the delta tasks from the formula is small
   * (due to K decreasing with sqrt(C)), resulting in no PPT reduction.
   */
  @Test
  public void testComputeScaleUpBoost()
  {
    BurstScaleUpOnHighLagPlugin plugin = new BurstScaleUpOnHighLagPlugin(LAG_THRESHOLD);

    // Below threshold: no boost
    Assert.assertEquals(
        "Below threshold should return 0",
        0,
        plugin.computeScaleUpBoost(PARTITION_COUNT * 40_000L, PARTITION_COUNT, 4, TASK_COUNT_MAX)
    );

    // At threshold (lagSeverity=1, ln(1)=0): no boost
    Assert.assertEquals(
        "At threshold should return 0",
        0,
        plugin.computeScaleUpBoost(PARTITION_COUNT * 50_000L, PARTITION_COUNT, 4, TASK_COUNT_MAX)
    );

    // C=1, 100K lag (2x threshold): significant boost for emergency recovery
    int boost1_100k = plugin.computeScaleUpBoost(PARTITION_COUNT * 100_000L, PARTITION_COUNT, 1, TASK_COUNT_MAX);
    Assert.assertEquals("C=1, 100K lag boost", 40, boost1_100k);

    // C=1, 200K lag (4x threshold): large boost
    int boost1_200k = plugin.computeScaleUpBoost(PARTITION_COUNT * 200_000L, PARTITION_COUNT, 1, TASK_COUNT_MAX);
    Assert.assertEquals("C=1, 200K lag boost", 43, boost1_200k);

    // C=4, 200K lag: moderate boost (K decreases with sqrt(C))
    int boost4_200k = plugin.computeScaleUpBoost(PARTITION_COUNT * 200_000L, PARTITION_COUNT, 4, TASK_COUNT_MAX);
    Assert.assertEquals("C=4, 200K lag boost", 6, boost4_200k);

    // C=12, 200K lag: delta too small to change PPT
    int boost12_200k = plugin.computeScaleUpBoost(PARTITION_COUNT * 200_000L, PARTITION_COUNT, 12, TASK_COUNT_MAX);
    Assert.assertEquals("C=12, 200K lag boost", 0, boost12_200k);

    // C=24, 200K lag: delta too small to change PPT
    int boost24_200k = plugin.computeScaleUpBoost(PARTITION_COUNT * 200_000L, PARTITION_COUNT, 24, TASK_COUNT_MAX);
    Assert.assertEquals("C=24, 200K lag boost", 0, boost24_200k);
  }

  @Test
  public void testComputeScaleUpBoostInvalidInputs()
  {
    BurstScaleUpOnHighLagPlugin plugin = new BurstScaleUpOnHighLagPlugin(LAG_THRESHOLD);

    Assert.assertEquals(0, plugin.computeScaleUpBoost(1_000_000, 0, 4, 48));
    Assert.assertEquals(0, plugin.computeScaleUpBoost(1_000_000, 48, 0, 48));
    Assert.assertEquals(0, plugin.computeScaleUpBoost(1_000_000, 48, 4, 0));
    Assert.assertEquals(0, plugin.computeScaleUpBoost(1_000_000, -1, 4, 48));
  }

  @Test
  public void testLagThreshold()
  {
    int customThreshold = 100_000;
    BurstScaleUpOnHighLagPlugin plugin = new BurstScaleUpOnHighLagPlugin(customThreshold);
    Assert.assertEquals(customThreshold, plugin.lagThreshold());
  }
}
