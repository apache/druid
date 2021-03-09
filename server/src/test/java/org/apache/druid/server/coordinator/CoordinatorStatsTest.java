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

import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.druid.com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class CoordinatorStatsTest
{
  private CoordinatorStats stats;

  @Before
  public void setUp()
  {
    stats = new CoordinatorStats();
  }

  @After
  public void tearDown()
  {
    stats = null;
  }

  @Test
  public void addToGlobalStat()
  {
    Assert.assertEquals(0, stats.getGlobalStat("stats"));
    stats.addToGlobalStat("stats", 1);
    Assert.assertEquals(1, stats.getGlobalStat("stats"));
    stats.addToGlobalStat("stats", -11);
    Assert.assertEquals(-10, stats.getGlobalStat("stats"));
  }

  @Test(expected = NullPointerException.class)
  public void testAddToTieredStatNonexistentStat()
  {
    stats.getTieredStat("stat", "tier");
  }

  @Test
  public void testAddToTieredStat()
  {
    Assert.assertFalse(stats.hasPerTierStats());
    stats.addToTieredStat("stat1", "tier1", 1);
    stats.addToTieredStat("stat1", "tier2", 1);
    stats.addToTieredStat("stat1", "tier1", -5);
    stats.addToTieredStat("stat2", "tier1", 1);
    stats.addToTieredStat("stat1", "tier2", 1);
    Assert.assertTrue(stats.hasPerTierStats());

    Assert.assertEquals(
        Sets.newHashSet("tier1", "tier2"),
        stats.getTiers("stat1")
    );
    Assert.assertEquals(
        Sets.newHashSet("tier1"),
        stats.getTiers("stat2")
    );
    Assert.assertTrue(stats.getTiers("stat3").isEmpty());

    Assert.assertEquals(-4, stats.getTieredStat("stat1", "tier1"));
    Assert.assertEquals(2, stats.getTieredStat("stat1", "tier2"));
    Assert.assertEquals(1, stats.getTieredStat("stat2", "tier1"));
  }

  @Test
  public void testForEachTieredStat()
  {
    final Map<String, Long> expected = ImmutableMap.of(
        "tier1", 1L,
        "tier2", 2L,
        "tier3", 3L
    );
    final Map<String, Long> actual = new HashMap<>();

    expected.forEach(
        (tier, count) -> stats.addToTieredStat("stat", tier, count)
    );

    stats.forEachTieredStat("stat0", (tier, count) -> Assert.fail());
    stats.forEachTieredStat("stat", actual::put);

    Assert.assertEquals(expected, actual);
  }


  @Test
  public void testAccumulate()
  {
    stats.addToGlobalStat("stat1", 1);
    stats.addToGlobalStat("stat2", 1);
    stats.addToTieredStat("stat1", "tier1", 1);
    stats.addToTieredStat("stat1", "tier2", 1);
    stats.addToTieredStat("stat2", "tier1", 1);
    stats.addToDutyStat("stat1", "duty1", 1);
    stats.addToDutyStat("stat1", "duty2", 1);
    stats.addToDutyStat("stat2", "duty1", 1);

    final CoordinatorStats stats2 = new CoordinatorStats();
    stats2.addToGlobalStat("stat1", 1);
    stats2.addToTieredStat("stat1", "tier2", 1);
    stats2.addToTieredStat("stat2", "tier2", 1);
    stats2.addToTieredStat("stat3", "tier1", 1);
    stats2.addToDutyStat("stat1", "duty2", 1);
    stats2.addToDutyStat("stat2", "duty2", 1);
    stats2.addToDutyStat("stat3", "duty1", 1);

    stats.accumulate(stats2);

    Assert.assertEquals(2, stats.getGlobalStat("stat1"));
    Assert.assertEquals(1, stats.getGlobalStat("stat2"));
    Assert.assertEquals(1, stats.getTieredStat("stat1", "tier1"));
    Assert.assertEquals(2, stats.getTieredStat("stat1", "tier2"));
    Assert.assertEquals(1, stats.getTieredStat("stat2", "tier1"));
    Assert.assertEquals(1, stats.getTieredStat("stat2", "tier2"));
    Assert.assertEquals(1, stats.getTieredStat("stat3", "tier1"));
    Assert.assertEquals(1, stats.getDutyStat("stat1", "duty1"));
    Assert.assertEquals(2, stats.getDutyStat("stat1", "duty2"));
    Assert.assertEquals(1, stats.getDutyStat("stat2", "duty1"));
    Assert.assertEquals(1, stats.getDutyStat("stat2", "duty2"));
    Assert.assertEquals(1, stats.getDutyStat("stat3", "duty1"));
  }

  @Test
  public void testAccumulateMaxToTieredStat()
  {
    Assert.assertFalse(stats.hasPerTierStats());
    stats.accumulateMaxTieredStat("stat1", "tier1", 2);
    stats.accumulateMaxTieredStat("stat1", "tier1", 6);
    stats.accumulateMaxTieredStat("stat1", "tier1", 5);

    stats.accumulateMaxTieredStat("stat2", "tier1", 5);
    stats.accumulateMaxTieredStat("stat2", "tier1", 4);
    stats.accumulateMaxTieredStat("stat2", "tier1", 5);

    stats.accumulateMaxTieredStat("stat1", "tier2", 7);
    stats.accumulateMaxTieredStat("stat1", "tier2", 9);
    stats.accumulateMaxTieredStat("stat1", "tier2", 10);

    Assert.assertTrue(stats.hasPerTierStats());

    Assert.assertEquals(
        Sets.newHashSet("tier1", "tier2"),
        stats.getTiers("stat1")
    );
    Assert.assertEquals(
        Sets.newHashSet("tier1"),
        stats.getTiers("stat2")
    );
    Assert.assertTrue(stats.getTiers("stat3").isEmpty());

    Assert.assertEquals(6, stats.getTieredStat("stat1", "tier1"));
    Assert.assertEquals(5, stats.getTieredStat("stat2", "tier1"));
    Assert.assertEquals(10, stats.getTieredStat("stat1", "tier2"));

  }

  @Test(expected = NullPointerException.class)
  public void testGetNonexistentDutyStat()
  {
    stats.getDutyStat("stat", "duty");
  }

  @Test
  public void testAddToDutyStat()
  {
    Assert.assertFalse(stats.hasPerDutyStats());
    stats.addToDutyStat("stat1", "duty1", 1);
    stats.addToDutyStat("stat1", "duty2", 1);
    stats.addToDutyStat("stat1", "duty1", -5);
    stats.addToDutyStat("stat2", "duty1", 1);
    stats.addToDutyStat("stat1", "duty2", 1);
    Assert.assertTrue(stats.hasPerDutyStats());

    Assert.assertEquals(
        Sets.newHashSet("duty1", "duty2"),
        stats.getDuties("stat1")
    );
    Assert.assertEquals(
        Sets.newHashSet("duty1"),
        stats.getDuties("stat2")
    );
    Assert.assertTrue(stats.getDuties("stat3").isEmpty());

    Assert.assertEquals(-4, stats.getDutyStat("stat1", "duty1"));
    Assert.assertEquals(2, stats.getDutyStat("stat1", "duty2"));
    Assert.assertEquals(1, stats.getDutyStat("stat2", "duty1"));
  }

  @Test
  public void testForEachDutyStat()
  {
    final Map<String, Long> expected = ImmutableMap.of(
        "duty1", 1L,
        "duty2", 2L,
        "duty3", 3L
    );
    final Map<String, Long> actual = new HashMap<>();

    expected.forEach(
        (duty, count) -> stats.addToDutyStat("stat", duty, count)
    );

    stats.forEachDutyStat("stat0", (duty, count) -> Assert.fail());
    stats.forEachDutyStat("stat", actual::put);

    Assert.assertEquals(expected, actual);
  }
}
