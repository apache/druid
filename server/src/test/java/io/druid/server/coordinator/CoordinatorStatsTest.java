/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class CoordinatorStatsTest
{
  private CoordinatorStats stats;

  @Before
  public void setUp() throws Exception
  {
    stats = new CoordinatorStats();
  }

  @After
  public void tearDown() throws Exception
  {
    stats = null;
  }

  @Test
  public void addToGlobalStat() throws Exception
  {
    Assert.assertEquals(0, stats.getGlobalStat("stats"));
    stats.addToGlobalStat("stats", 1);
    Assert.assertEquals(1, stats.getGlobalStat("stats"));
    stats.addToGlobalStat("stats", -11);
    Assert.assertEquals(-10, stats.getGlobalStat("stats"));
  }

  @Test(expected = NullPointerException.class)
  public void testAddToTieredStatNonexistentStat() throws Exception
  {
    stats.getTieredStat("stat", "tier");
  }

  @Test
  public void testAddToTieredStat() throws Exception
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
  public void testForEachTieredStat() throws Exception
  {
    final Map<String, Long> expected = ImmutableMap.of(
        "tier1", 1L,
        "tier2", 2L,
        "tier3", 3L
    );
    final Map<String, Long> actual = Maps.newHashMap();

    expected.forEach(
        (tier, count) -> stats.addToTieredStat("stat", tier, count)
    );

    stats.forEachTieredStat("stat0", (tier, count) -> Assert.fail());
    stats.forEachTieredStat("stat", actual::put);

    Assert.assertEquals(expected, actual);
  }


  @Test
  public void testAccumulate() throws Exception
  {
    stats.addToGlobalStat("stat1", 1);
    stats.addToGlobalStat("stat2", 1);
    stats.addToTieredStat("stat1", "tier1", 1);
    stats.addToTieredStat("stat1", "tier2", 1);
    stats.addToTieredStat("stat2", "tier1", 1);

    final CoordinatorStats stats2 = new CoordinatorStats();
    stats2.addToGlobalStat("stat1", 1);
    stats2.addToTieredStat("stat1", "tier2", 1);
    stats2.addToTieredStat("stat2", "tier2", 1);
    stats2.addToTieredStat("stat3", "tier1", 1);

    stats.accumulate(stats2);

    Assert.assertEquals(2, stats.getGlobalStat("stat1"));
    Assert.assertEquals(1, stats.getGlobalStat("stat2"));
    Assert.assertEquals(1, stats.getTieredStat("stat1", "tier1"));
    Assert.assertEquals(2, stats.getTieredStat("stat1", "tier2"));
    Assert.assertEquals(1, stats.getTieredStat("stat2", "tier1"));
    Assert.assertEquals(1, stats.getTieredStat("stat2", "tier2"));
    Assert.assertEquals(1, stats.getTieredStat("stat3", "tier1"));
  }
}
