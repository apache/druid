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
    Assert.assertEquals(0, stats.get(Stat.ERROR_1));
    stats.add(Stat.ERROR_1, 1);
    Assert.assertEquals(1, stats.get(Stat.ERROR_1));
    stats.add(Stat.ERROR_1, -11);
    Assert.assertEquals(-10, stats.get(Stat.ERROR_1));
  }

  @Test
  public void testAddForRowKey()
  {
    stats.add(Stat.ERROR_1, Key.TIER_1, 1);
    stats.add(Stat.ERROR_1, Key.TIER_2, 1);
    stats.add(Stat.ERROR_1, Key.TIER_1, -5);
    stats.add(Stat.INFO_1, Key.TIER_1, 1);
    stats.add(Stat.ERROR_1, Key.TIER_2, 1);

    Assert.assertFalse(stats.hasStat(Stat.INFO_2));

    Assert.assertEquals(-4, stats.get(Stat.ERROR_1, Key.TIER_1));
    Assert.assertEquals(2, stats.get(Stat.ERROR_1, Key.TIER_2));
    Assert.assertEquals(1, stats.get(Stat.INFO_1, Key.TIER_1));
  }

  @Test
  public void testUpdateMax()
  {
    stats.updateMax(Stat.ERROR_1, Key.TIER_1, 2);
    stats.updateMax(Stat.ERROR_1, Key.TIER_1, 6);
    stats.updateMax(Stat.ERROR_1, Key.TIER_1, 5);

    stats.updateMax(Stat.INFO_1, Key.TIER_1, 5);
    stats.updateMax(Stat.INFO_1, Key.TIER_1, 4);
    stats.updateMax(Stat.INFO_1, Key.TIER_1, 5);

    stats.updateMax(Stat.ERROR_1, Key.TIER_2, 7);
    stats.updateMax(Stat.ERROR_1, Key.TIER_2, 9);
    stats.updateMax(Stat.ERROR_1, Key.TIER_2, 10);

    Assert.assertFalse(stats.hasStat(Stat.INFO_2));

    Assert.assertEquals(6, stats.get(Stat.ERROR_1, Key.TIER_1));
    Assert.assertEquals(5, stats.get(Stat.INFO_1, Key.TIER_1));
    Assert.assertEquals(10, stats.get(Stat.ERROR_1, Key.TIER_2));
  }

  @Test
  public void testAddToDutyStat()
  {
    stats.add(Stat.ERROR_1, Key.DUTY_1, 1);
    stats.add(Stat.ERROR_1, Key.DUTY_2, 1);
    stats.add(Stat.ERROR_1, Key.DUTY_1, -5);
    stats.add(Stat.INFO_1, Key.DUTY_1, 1);
    stats.add(Stat.ERROR_1, Key.DUTY_2, 1);

    Assert.assertFalse(stats.hasStat(Stat.INFO_2));
    Assert.assertEquals(-4, stats.get(Stat.ERROR_1, Key.DUTY_1));
    Assert.assertEquals(2, stats.get(Stat.ERROR_1, Key.DUTY_2));
    Assert.assertEquals(1, stats.get(Stat.INFO_1, Key.DUTY_1));
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
            stats.add(Stat.ERROR_1, RowKey.of(Dimension.DUTY, duty), count)
    );

    final Map<String, Long> actual = new HashMap<>();
    stats.forEachStat(
        (stat, rowKey, value) -> {
          if (stat.equals(Stat.ERROR_1)) {
            actual.put(rowKey.getValues().get(Dimension.DUTY), value);
          }
        }
    );
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testBuildStatsTable()
  {
    stats.add(Stat.ERROR_1, Key.DUTY_1, 10);
    stats.add(Stat.INFO_1, Key.DUTY_1, 20);
    stats.add(Stat.DEBUG_1, Key.DUTY_1, 30);

    final String expectedTable
        = "\nError: {duty=duty1} ==> {error1=10}"
          + "\nInfo : {duty=duty1} ==> {info1=20}"
          + "\nDebug: 1 hidden stats. Set 'debugDimensions' to see these."
          + "\nTOTAL: 3 stats for 1 dimension keys";

    Assert.assertEquals(expectedTable, stats.buildStatsTable());
  }

  @Test
  public void testBuildStatsTableWithDebugDimensions()
  {
    final CoordinatorRunStats debugStats = new CoordinatorRunStats(Key.DUTY_1.getValues());
    debugStats.add(Stat.ERROR_1, Key.DUTY_1, 10);
    debugStats.add(Stat.INFO_1, Key.DUTY_1, 20);
    debugStats.add(Stat.DEBUG_1, Key.DUTY_1, 30);

    final String expectedTable
        = "\nError: {duty=duty1} ==> {error1=10}"
          + "\nInfo : {duty=duty1} ==> {info1=20}"
          + "\nDebug: {duty=duty1} ==> {debug1=30}"
          + "\nTOTAL: 3 stats for 1 dimension keys";

    Assert.assertEquals(expectedTable, debugStats.buildStatsTable());
  }

  @Test
  public void testAddToEmptyThrowsException()
  {
    CoordinatorRunStats runStats = CoordinatorRunStats.empty();
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> runStats.add(Stat.ERROR_1, 10)
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> runStats.add(Stat.ERROR_1, Key.DUTY_1, 10)
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> runStats.addToSegmentStat(Stat.ERROR_1, "t", "ds", 10)
    );
    Assert.assertThrows(
        UnsupportedOperationException.class,
        () -> runStats.updateMax(Stat.INFO_1, Key.TIER_1, 10)
    );
  }

  /**
   * Dimension keys for reporting stats.
   */
  private static class Key
  {
    static final RowKey TIER_1 = RowKey.of(Dimension.TIER, "tier1");
    static final RowKey TIER_2 = RowKey.of(Dimension.TIER, "tier2");

    static final RowKey DUTY_1 = RowKey.of(Dimension.DUTY, "duty1");
    static final RowKey DUTY_2 = RowKey.of(Dimension.DUTY, "duty2");
  }

  private static class Stat
  {
    static final CoordinatorStat ERROR_1
        = CoordinatorStat.toLogAndEmit("error1", "e1", CoordinatorStat.Level.ERROR);
    static final CoordinatorStat INFO_1
        = CoordinatorStat.toLogAndEmit("info1", "i1", CoordinatorStat.Level.INFO);
    static final CoordinatorStat INFO_2
        = CoordinatorStat.toLogAndEmit("info2", "i2", CoordinatorStat.Level.INFO);
    static final CoordinatorStat DEBUG_1
        = CoordinatorStat.toDebugAndEmit("debug1", "d1");
  }
}
