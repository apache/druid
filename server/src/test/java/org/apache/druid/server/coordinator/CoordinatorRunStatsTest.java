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

package org.apache.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.CoordinatorStat;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CoordinatorRunStatsTest
{
  private static final CoordinatorStat STAT_1 = new CoordinatorStat("stat1", "s1");
  private static final CoordinatorStat STAT_2 = new CoordinatorStat("stat2", "s2");
  private static final CoordinatorStat STAT_3 = new CoordinatorStat("stat3", "s3");

  private static final CoordinatorStat DEBUG_STAT_1
      = new CoordinatorStat("debugStat1", CoordinatorStat.Level.DEBUG);
  private static final CoordinatorStat DEBUG_STAT_2
      = new CoordinatorStat("debugStat2", CoordinatorStat.Level.DEBUG);

  private CoordinatorRunStats stats;

  @Before
  public void setUp()
  {
    stats = new CoordinatorRunStats();
  }

  @After
  public void tearDown()
  {
    stats = null;
  }

  @Test
  public void testAdd()
  {
    Assert.assertEquals(0, stats.get(STAT_1));
    stats.add(STAT_1, 1);
    Assert.assertEquals(1, stats.get(STAT_1));
    stats.add(STAT_1, -11);
    Assert.assertEquals(-10, stats.get(STAT_1));
  }

  @Test
  public void testAddForRowKey()
  {
    stats.add(STAT_1, Key.TIER_1, 1);
    stats.add(STAT_1, Key.TIER_2, 1);
    stats.add(STAT_1, Key.TIER_1, -5);
    stats.add(STAT_2, Key.TIER_1, 1);
    stats.add(STAT_1, Key.TIER_2, 1);

    Assert.assertFalse(stats.hasStat(STAT_3));

    Assert.assertEquals(-4, stats.get(STAT_1, Key.TIER_1));
    Assert.assertEquals(2, stats.get(STAT_1, Key.TIER_2));
    Assert.assertEquals(1, stats.get(STAT_2, Key.TIER_1));
  }

  @Test
  public void testAccumulate()
  {
    stats.add(STAT_1, 1);
    stats.add(STAT_2, 1);
    stats.add(STAT_1, Key.TIER_1, 1);
    stats.add(STAT_1, Key.TIER_2, 1);
    stats.add(STAT_2, Key.TIER_1, 1);

    stats.add(STAT_1, Key.DUTY_1, 1);
    stats.add(STAT_2, Key.DUTY_1, 1);
    stats.add(STAT_1, Key.DUTY_2, 1);

    final CoordinatorRunStats stats2 = new CoordinatorRunStats();
    stats2.add(STAT_1, 1);
    stats2.add(STAT_1, Key.TIER_2, 1);
    stats2.add(STAT_2, Key.TIER_2, 1);
    stats2.add(STAT_3, Key.TIER_1, 1);
    stats2.add(STAT_1, Key.DUTY_2, 1);
    stats2.add(STAT_2, Key.DUTY_2, 1);
    stats2.add(STAT_3, Key.DUTY_1, 1);

    stats.accumulate(stats2);

    Assert.assertEquals(2, stats.get(STAT_1));
    Assert.assertEquals(1, stats.get(STAT_2));
    Assert.assertEquals(1, stats.get(STAT_1, Key.TIER_1));
    Assert.assertEquals(2, stats.get(STAT_1, Key.TIER_2));
    Assert.assertEquals(1, stats.get(STAT_2, Key.TIER_1));
    Assert.assertEquals(1, stats.get(STAT_2, Key.TIER_2));
    Assert.assertEquals(1, stats.get(STAT_3, Key.TIER_1));
    Assert.assertEquals(1, stats.get(STAT_1, Key.DUTY_1));
    Assert.assertEquals(2, stats.get(STAT_1, Key.DUTY_2));
    Assert.assertEquals(1, stats.get(STAT_2, Key.DUTY_1));
    Assert.assertEquals(1, stats.get(STAT_2, Key.DUTY_2));
    Assert.assertEquals(1, stats.get(STAT_3, Key.DUTY_1));
  }

  @Test
  public void testUpdateMax()
  {
    stats.updateMax(STAT_1, Key.TIER_1, 2);
    stats.updateMax(STAT_1, Key.TIER_1, 6);
    stats.updateMax(STAT_1, Key.TIER_1, 5);

    stats.updateMax(STAT_2, Key.TIER_1, 5);
    stats.updateMax(STAT_2, Key.TIER_1, 4);
    stats.updateMax(STAT_2, Key.TIER_1, 5);

    stats.updateMax(STAT_1, Key.TIER_2, 7);
    stats.updateMax(STAT_1, Key.TIER_2, 9);
    stats.updateMax(STAT_1, Key.TIER_2, 10);

    Assert.assertFalse(stats.hasStat(STAT_3));

    Assert.assertEquals(6, stats.get(STAT_1, Key.TIER_1));
    Assert.assertEquals(5, stats.get(STAT_2, Key.TIER_1));
    Assert.assertEquals(10, stats.get(STAT_1, Key.TIER_2));
  }

  @Test
  public void testAddToDutyStat()
  {
    stats.add(STAT_1, Key.DUTY_1, 1);
    stats.add(STAT_1, Key.DUTY_2, 1);
    stats.add(STAT_1, Key.DUTY_1, -5);
    stats.add(STAT_2, Key.DUTY_1, 1);
    stats.add(STAT_1, Key.DUTY_2, 1);

    Assert.assertFalse(stats.hasStat(STAT_3));
    Assert.assertEquals(-4, stats.get(STAT_1, Key.DUTY_1));
    Assert.assertEquals(2, stats.get(STAT_1, Key.DUTY_2));
    Assert.assertEquals(1, stats.get(STAT_2, Key.DUTY_1));
  }

  @Test
  public void testForEachStat()
  {
    final Map<String, Long> expected = ImmutableMap.of(
        "duty1", 1L,
        "duty2", 2L,
        "duty3", 3L
    );
    expected.forEach(
        (duty, count) ->
            stats.add(STAT_1, RowKey.builder().add(Dimension.DUTY, duty).build(), count)
    );

    final Map<String, Long> actual = new HashMap<>();
    stats.forEachStat(
        (dimensionValues, stat, value) -> {
          if (stat.equals(STAT_1)) {
            actual.put(dimensionValues.get(Dimension.DUTY), value);
          }
        }
    );
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testAddWithDebugDimensions()
  {
    stats.add(DEBUG_STAT_1, 1);
    Assert.assertFalse(stats.hasStat(DEBUG_STAT_1));

    stats.add(DEBUG_STAT_1, Key.TIER_1, 1);
    Assert.assertFalse(stats.hasStat(DEBUG_STAT_1));

    final CoordinatorRunStats debugStats
        = new CoordinatorRunStats(Key.TIER_1.getValues());
    debugStats.add(DEBUG_STAT_1, 1);
    Assert.assertFalse(stats.hasStat(DEBUG_STAT_1));

    debugStats.add(DEBUG_STAT_1, Key.TIER_1, 1);
    Assert.assertTrue(debugStats.hasStat(DEBUG_STAT_1));

    debugStats.addToDatasourceStat(DEBUG_STAT_2, "wiki", 1);
    Assert.assertFalse(debugStats.hasStat(DEBUG_STAT_2));
  }

  /**
   * Dimension keys for reporting stats.
   */
  private static class Key
  {
    static final RowKey TIER_1 = RowKey.forTier("tier1");
    static final RowKey TIER_2 = RowKey.forTier("tier2");

    static final RowKey DUTY_1 = RowKey.builder().add(Dimension.DUTY, "duty1").build();
    static final RowKey DUTY_2 = RowKey.builder().add(Dimension.DUTY, "duty2").build();
  }

}
