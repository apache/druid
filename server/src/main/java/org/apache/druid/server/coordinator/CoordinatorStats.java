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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.ObjLongConsumer;

/**
 */
public class CoordinatorStats
{
  // List of stat names
  public static final String CANCELLED_MOVES = "cancelMoveCount";
  public static final String CANCELLED_LOADS = "cancelLoadCount";
  public static final String CANCELLED_DROPS = "cancelDropCount";

  public static final String ASSIGNED_COUNT = "assignedCount";
  public static final String DROPPED_COUNT = "droppedCount";
  public static final String DELETED_COUNT = "deletedCount";
  public static final String UNNEEDED_COUNT = "unneededCount";
  public static final String OVERSHADOWED_COUNT = "overshadowedCount";
  public static final String UNDER_REPLICATED_COUNT = "underReplicatedCount";

  public static final String ASSIGN_SKIP_COUNT = "assignSkip";
  public static final String DROP_SKIP_COUNT = "dropSkip";

  public static final String MOVED_COUNT = "movedCount";
  public static final String UNMOVED_COUNT = "unmovedCount";

  public static final String BROADCAST_LOADS = "broadcastLoad";
  public static final String BROADCAST_DROPS = "broadcastDrop";

  public static final String REQUIRED_CAPACITY = "requiredCapacity";
  public static final String TOTAL_CAPACITY = "totalCapacity";
  public static final String MAX_REPLICATION_FACTOR = "maxReplicationFactor";

  private final Map<String, Object2LongOpenHashMap<String>> perTierStats;
  private final Map<String, Object2LongOpenHashMap<String>> perDataSourceStats;
  private final Map<String, Object2LongOpenHashMap<String>> perDutyStats;
  private final Object2LongOpenHashMap<String> globalStats;

  public CoordinatorStats()
  {
    perTierStats = new HashMap<>();
    perDataSourceStats = new HashMap<>();
    perDutyStats = new HashMap<>();
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

  public boolean hasPerDutyStats()
  {
    return !perDutyStats.isEmpty();
  }

  public Set<String> getTiers(final String statName)
  {
    final Object2LongOpenHashMap<String> theStat = perTierStats.get(statName);
    if (theStat == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(theStat.keySet());
  }

  public Set<String> getDataSources(String statName)
  {
    final Object2LongOpenHashMap<String> stat = perDataSourceStats.get(statName);
    if (stat == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(stat.keySet());
  }

  public Set<String> getDuties(String statName)
  {
    final Object2LongOpenHashMap<String> stat = perDutyStats.get(statName);
    if (stat == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(stat.keySet());
  }

  /**
   *
   * @param statName the name of the statistics
   * @param tier the tier
   * @return the value for the statistics {@code statName} under {@code tier} tier
   * @throws NullPointerException if {@code statName} is not found
   */
  public long getTieredStat(final String statName, final String tier)
  {
    return perTierStats.get(statName).getLong(tier);
  }

  public void forEachTieredStat(final String statName, final ObjLongConsumer<String> consumer)
  {
    final Object2LongOpenHashMap<String> theStat = perTierStats.get(statName);
    if (theStat != null) {
      for (final Object2LongMap.Entry<String> entry : theStat.object2LongEntrySet()) {
        consumer.accept(entry.getKey(), entry.getLongValue());
      }
    }
  }

  public long getDataSourceStat(String statName, String dataSource)
  {
    return perDataSourceStats.get(statName).getLong(dataSource);
  }

  public void forEachDataSourceStat(String statName, ObjLongConsumer<String> consumer)
  {
    final Object2LongOpenHashMap<String> stat = perDataSourceStats.get(statName);
    if (stat != null) {
      for (Entry<String> entry : stat.object2LongEntrySet()) {
        consumer.accept(entry.getKey(), entry.getLongValue());
      }
    }
  }

  public long getDutyStat(String statName, String duty)
  {
    return perDutyStats.get(statName).getLong(duty);
  }

  public void forEachDutyStat(String statName, ObjLongConsumer<String> consumer)
  {
    final Object2LongOpenHashMap<String> stat = perDutyStats.get(statName);
    if (stat != null) {
      for (Entry<String> entry : stat.object2LongEntrySet()) {
        consumer.accept(entry.getKey(), entry.getLongValue());
      }
    }
  }

  public long getGlobalStat(final String statName)
  {
    return globalStats.getLong(statName);
  }

  public void addToTieredStat(final String statName, final String tier, final long value)
  {
    perTierStats.computeIfAbsent(statName, ignored -> new Object2LongOpenHashMap<>())
                .addTo(tier, value);
  }

  public void accumulateMaxTieredStat(final String statName, final String tier, final long value)
  {
    perTierStats.computeIfAbsent(statName, ignored -> new Object2LongOpenHashMap<>())
                .mergeLong(tier, value, Math::max);
  }

  public void addToDataSourceStat(String statName, String dataSource, long value)
  {
    perDataSourceStats.computeIfAbsent(statName, k -> new Object2LongOpenHashMap<>())
                      .addTo(dataSource, value);
  }

  public void addToDutyStat(String statName, String duty, long value)
  {
    perDutyStats.computeIfAbsent(statName, k -> new Object2LongOpenHashMap<>())
                .addTo(duty, value);
  }

  public void addToGlobalStat(final String statName, final long value)
  {
    globalStats.addTo(statName, value);
  }

  public CoordinatorStats accumulate(final CoordinatorStats stats)
  {
    stats.perTierStats.forEach(
        (final String statName, final Object2LongOpenHashMap<String> urStat) -> {

          final Object2LongOpenHashMap<String> myStat = perTierStats.computeIfAbsent(
              statName,
              name -> new Object2LongOpenHashMap<>()
          );

          for (final Object2LongMap.Entry<String> entry : urStat.object2LongEntrySet()) {
            myStat.addTo(entry.getKey(), entry.getLongValue());
          }
        }
    );

    stats.perDataSourceStats.forEach(
        (statName, urStat) -> {
          final Object2LongOpenHashMap<String> myStat = perDataSourceStats.computeIfAbsent(
              statName,
              k -> new Object2LongOpenHashMap<>()
          );

          for (Entry<String> entry : urStat.object2LongEntrySet()) {
            myStat.addTo(entry.getKey(), entry.getLongValue());
          }
        }
    );

    stats.perDutyStats.forEach(
        (statName, urStat) -> {
          final Object2LongOpenHashMap<String> myStat = perDutyStats.computeIfAbsent(
              statName,
              k -> new Object2LongOpenHashMap<>()
          );

          for (Entry<String> entry : urStat.object2LongEntrySet()) {
            myStat.addTo(entry.getKey(), entry.getLongValue());
          }
        }

    );

    for (final Object2LongMap.Entry<String> entry : stats.globalStats.object2LongEntrySet()) {
      globalStats.addTo(entry.getKey(), entry.getLongValue());
    }

    return this;
  }

  public Map<String, Long> getSortedTierStats(String tier)
  {
    final Map<String, Long> sortedTierStats = new TreeMap<>();
    perTierStats.forEach((statName, statsPerTier) -> {
      if (statsPerTier.containsKey(tier)) {
        sortedTierStats.put(statName, statsPerTier.getLong(tier));
      }
    });

    return sortedTierStats;
  }
}
