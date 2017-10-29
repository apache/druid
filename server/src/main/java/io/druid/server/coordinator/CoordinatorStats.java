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

import com.google.common.collect.Maps;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.function.ObjLongConsumer;

/**
 */
public class CoordinatorStats
{
  private final Map<String, Object2LongOpenHashMap<String>> perTierStats;
  private final Object2LongOpenHashMap<String> globalStats;

  public CoordinatorStats()
  {
    perTierStats = Maps.newHashMap();
    globalStats = new Object2LongOpenHashMap<>();
  }

  public boolean hasPerTierStats()
  {
    return !perTierStats.isEmpty();
  }

  public Set<String> getTiers(final String statName)
  {
    final Object2LongOpenHashMap<String> theStat = perTierStats.get(statName);
    if (theStat == null) {
      return Collections.emptySet();
    }
    return Collections.unmodifiableSet(theStat.keySet());
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

  public long getGlobalStat(final String statName)
  {
    return globalStats.getLong(statName);
  }

  public void addToTieredStat(final String statName, final String tier, final long value)
  {
    perTierStats.computeIfAbsent(statName, ignored -> new Object2LongOpenHashMap<>())
                .addTo(tier, value);
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
              statName, ignored -> new Object2LongOpenHashMap<>()
          );

          for (final Object2LongMap.Entry<String> entry : urStat.object2LongEntrySet()) {
            myStat.addTo(entry.getKey(), entry.getLongValue());
          }
        }
    );

    for (final Object2LongMap.Entry<String> entry : stats.globalStats.object2LongEntrySet()) {
      globalStats.addTo(entry.getKey(), entry.getLongValue());
    }

    return this;
  }
}
