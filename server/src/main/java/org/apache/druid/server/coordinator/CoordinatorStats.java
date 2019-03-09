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

import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongMap.Entry;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.function.ObjLongConsumer;

/**
 */
public class CoordinatorStats
{
  private final Map<Stat, Object2LongOpenHashMap<String>> perTierStats;
  private final Map<Stat, Object2LongOpenHashMap<String>> perDataSourceStats;
  private final Object2LongOpenHashMap<Stat> globalStats;

  public enum Stat
  {
    INITIAL_COST,
    NORMALIZATION,
    NORMALIZED_INITIAL_COST_TIMES_ONE_THOUSAND,
    UNMOVED_COUNT,
    DELETED_COUNT,
    MOVED_COUNT,
    UNNEEDED_COUNT,
    ASSIGNED_COUNT,
    UNASSIGNED_COUNT,
    UNASSIGNED_SIZE,
    DROPPED_COUNT,
    SEGMENT_SIZE_WAIT_COMPACT,
    SEGMENTS_WAIT_COMPACT,
    COMPACT_TASK_COUNT,
    OVER_SHADOWED_COUNT,
    MERGED_COUNT
  }

  public CoordinatorStats()
  {
    perTierStats = new EnumMap<>(Stat.class);
    perDataSourceStats = new EnumMap<>(Stat.class);
    globalStats = new Object2LongOpenHashMap<>();
  }

  public boolean hasPerTierStats()
  {
    return !perTierStats.isEmpty();
  }

  public boolean hasPerDataSourceStats()
  {
    return !perDataSourceStats.isEmpty();
  }

  public Set<String> getTiers(final Stat stat)
  {
    final Object2LongOpenHashMap<String> theStat = perTierStats.get(stat);
    if (theStat == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(theStat.keySet());
  }

  public Set<String> getDataSources(Stat stat)
  {
    final Object2LongOpenHashMap<String> theStat = perDataSourceStats.get(stat);
    if (theStat == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(theStat.keySet());
  }

  /**
   *
   * @param stat the statistics
   * @param tier the tier
   * @return the value for the statistics {@code statName} under {@code tier} tier
   * @throws NullPointerException if {@code statName} is not found
   */
  public long getTieredStat(final Stat stat, final String tier)
  {
    return perTierStats.get(stat).getLong(tier);
  }

  public void forEachTieredStat(final Stat stat, final ObjLongConsumer<String> consumer)
  {
    final Object2LongOpenHashMap<String> theStat = perTierStats.get(stat);
    if (theStat != null) {
      for (final Object2LongMap.Entry<String> entry : theStat.object2LongEntrySet()) {
        consumer.accept(entry.getKey(), entry.getLongValue());
      }
    }
  }

  public long getDataSourceStat(Stat stat, String dataSource)
  {
    return perDataSourceStats.get(stat).getLong(dataSource);
  }

  public void forEachDataSourceStat(Stat stat, ObjLongConsumer<String> consumer)
  {
    final Object2LongOpenHashMap<String> theStat = perDataSourceStats.get(stat);
    if (theStat != null) {
      for (Entry<String> entry : theStat.object2LongEntrySet()) {
        consumer.accept(entry.getKey(), entry.getLongValue());
      }
    }
  }

  public long getGlobalStat(final Stat stat)
  {
    return globalStats.getLong(stat);
  }

  public void addToTieredStat(final Stat stat, final String tier, final long value)
  {
    perTierStats.computeIfAbsent(stat, ignored -> new Object2LongOpenHashMap<>())
                .addTo(tier, value);
  }

  public void addToDataSourceStat(Stat stat, String dataSource, long value)
  {
    perDataSourceStats.computeIfAbsent(stat, k -> new Object2LongOpenHashMap<>())
                      .addTo(dataSource, value);
  }

  public void addToGlobalStat(final Stat stat, final long value)
  {
    globalStats.addTo(stat, value);
  }

  public CoordinatorStats accumulate(final CoordinatorStats stats)
  {
    stats.perTierStats.forEach(
        (stat, urStat) -> {
          final Object2LongOpenHashMap<String> myStat = perTierStats.computeIfAbsent(
              stat,
              name -> new Object2LongOpenHashMap<>()
          );

          for (final Object2LongMap.Entry<String> entry : urStat.object2LongEntrySet()) {
            myStat.addTo(entry.getKey(), entry.getLongValue());
          }
        }
    );

    stats.perDataSourceStats.forEach(
        (stat, urStat) -> {
          final Object2LongOpenHashMap<String> myStat = perDataSourceStats.computeIfAbsent(
              stat,
              k -> new Object2LongOpenHashMap<>()
          );

          for (Entry<String> entry : urStat.object2LongEntrySet()) {
            myStat.addTo(entry.getKey(), entry.getLongValue());
          }
        }
    );

    for (final Object2LongMap.Entry<Stat> entry : stats.globalStats.object2LongEntrySet()) {
      globalStats.addTo(entry.getKey(), entry.getLongValue());
    }

    return this;
  }
}
