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
  public void addToGlobalStat()
  {
    Assert.assertEquals(0, stats.get(STAT_1));
    stats.add(STAT_1, 1);
    Assert.assertEquals(1, stats.get(STAT_1));
    stats.add(STAT_1, -11);
    Assert.assertEquals(-10, stats.get(STAT_1));
  }

  @Test
  public void testAddToTieredStat()
  {
    stats.addToTieredStat(STAT_1, "tier1", 1);
    stats.addToTieredStat(STAT_1, "tier2", 1);
    stats.addToTieredStat(STAT_1, "tier1", -5);
    stats.addToTieredStat(STAT_2, "tier1", 1);
    stats.addToTieredStat(STAT_1, "tier2", 1);

    Assert.assertFalse(stats.hasStat(STAT_3));

    Assert.assertEquals(-4, stats.getTieredStat(STAT_1, "tier1"));
    Assert.assertEquals(2, stats.getTieredStat(STAT_1, "tier2"));
    Assert.assertEquals(1, stats.getTieredStat(STAT_2, "tier1"));
  }

  @Test
  public void testForEachTieredStat()
  {
    final Map<String, Long> expected = ImmutableMap.of(
        "tier1", 1L,
        "tier2", 2L,
        "tier3", 3L
    );

    expected.forEach(
        (tier, count) -> stats.addToTieredStat(STAT_1, tier, count)
    );

    final Map<String, Long> actual = new HashMap<>();
    stats.forEachStat(
        (dimensionValues, stat, value) -> {
          if (stat.equals(STAT_1)) {
            actual.put(dimensionValues.get(Dimension.TIER), value);
          }
        }
    );
    Assert.assertEquals(expected, actual);
  }


  @Test
  public void testAccumulate()
  {
    stats.add(STAT_1, 1);
    stats.add(STAT_2, 1);
    stats.addToTieredStat(STAT_1, "tier1", 1);
    stats.addToTieredStat(STAT_1, "tier2", 1);
    stats.addToTieredStat(STAT_2, "tier1", 1);
    stats.addToDutyStat(STAT_1, "duty1", 1);
    stats.addToDutyStat(STAT_1, "duty2", 1);
    stats.addToDutyStat(STAT_2, "duty1", 1);

    final CoordinatorRunStats stats2 = new CoordinatorRunStats();
    stats2.add(STAT_1, 1);
    stats2.addToTieredStat(STAT_1, "tier2", 1);
    stats2.addToTieredStat(STAT_2, "tier2", 1);
    stats2.addToTieredStat(STAT_3, "tier1", 1);
    stats2.addToDutyStat(STAT_1, "duty2", 1);
    stats2.addToDutyStat(STAT_2, "duty2", 1);
    stats2.addToDutyStat(STAT_3, "duty1", 1);

    stats.accumulate(stats2);

    Assert.assertEquals(2, stats.get(STAT_1));
    Assert.assertEquals(1, stats.get(STAT_2));
    Assert.assertEquals(1, stats.getTieredStat(STAT_1, "tier1"));
    Assert.assertEquals(2, stats.getTieredStat(STAT_1, "tier2"));
    Assert.assertEquals(1, stats.getTieredStat(STAT_2, "tier1"));
    Assert.assertEquals(1, stats.getTieredStat(STAT_2, "tier2"));
    Assert.assertEquals(1, stats.getTieredStat(STAT_3, "tier1"));
    Assert.assertEquals(1, stats.getDutyStat(STAT_1, "duty1"));
    Assert.assertEquals(2, stats.getDutyStat(STAT_1, "duty2"));
    Assert.assertEquals(1, stats.getDutyStat(STAT_2, "duty1"));
    Assert.assertEquals(1, stats.getDutyStat(STAT_2, "duty2"));
    Assert.assertEquals(1, stats.getDutyStat(STAT_3, "duty1"));
  }

  @Test
  public void testAccumulateMaxToTieredStat()
  {
    stats.accumulateMaxTieredStat(STAT_1, "tier1", 2);
    stats.accumulateMaxTieredStat(STAT_1, "tier1", 6);
    stats.accumulateMaxTieredStat(STAT_1, "tier1", 5);

    stats.accumulateMaxTieredStat(STAT_2, "tier1", 5);
    stats.accumulateMaxTieredStat(STAT_2, "tier1", 4);
    stats.accumulateMaxTieredStat(STAT_2, "tier1", 5);

    stats.accumulateMaxTieredStat(STAT_1, "tier2", 7);
    stats.accumulateMaxTieredStat(STAT_1, "tier2", 9);
    stats.accumulateMaxTieredStat(STAT_1, "tier2", 10);

    Assert.assertFalse(stats.hasStat(STAT_3));

    Assert.assertEquals(6, stats.getTieredStat(STAT_1, "tier1"));
    Assert.assertEquals(5, stats.getTieredStat(STAT_2, "tier1"));
    Assert.assertEquals(10, stats.getTieredStat(STAT_1, "tier2"));

  }

  @Test
  public void testAddToDutyStat()
  {
    stats.addToDutyStat(STAT_1, "duty1", 1);
    stats.addToDutyStat(STAT_1, "duty2", 1);
    stats.addToDutyStat(STAT_1, "duty1", -5);
    stats.addToDutyStat(STAT_2, "duty1", 1);
    stats.addToDutyStat(STAT_1, "duty2", 1);

    Assert.assertFalse(stats.hasStat(STAT_3));
    Assert.assertEquals(-4, stats.getDutyStat(STAT_1, "duty1"));
    Assert.assertEquals(2, stats.getDutyStat(STAT_1, "duty2"));
    Assert.assertEquals(1, stats.getDutyStat(STAT_2, "duty1"));
  }

  @Test
  public void testForEachDutyStat()
  {
    final Map<String, Long> expected = ImmutableMap.of(
        "duty1", 1L,
        "duty2", 2L,
        "duty3", 3L
    );
    expected.forEach(
        (duty, count) -> stats.addToDutyStat(STAT_1, duty, count)
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
}
