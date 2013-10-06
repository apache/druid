/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.coordinator;

import com.google.common.collect.Maps;
import io.druid.collections.CountingMap;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class CoordinatorStats
{
  private final Map<String, CountingMap<String>> perTierStats;
  private final CountingMap<String> globalStats;

  public CoordinatorStats()
  {
    perTierStats = Maps.newHashMap();
    globalStats = new CountingMap<String>();
  }

  public Map<String, CountingMap<String>> getPerTierStats()
  {
    return perTierStats;
  }

  public CountingMap<String> getGlobalStats()
  {
    return globalStats;
  }

  public void addToTieredStat(String statName, String tier, long value)
  {
    CountingMap<String> theStat = perTierStats.get(statName);
    if (theStat == null) {
      theStat = new CountingMap<String>();
      perTierStats.put(statName, theStat);
    }
    theStat.add(tier, value);
  }

  public void addToGlobalStat(String statName, long value)
  {
    globalStats.add(statName, value);
  }

  public CoordinatorStats accumulate(CoordinatorStats stats)
  {
    for (Map.Entry<String, CountingMap<String>> entry : stats.perTierStats.entrySet()) {
      CountingMap<String> theStat = perTierStats.get(entry.getKey());
      if (theStat == null) {
        theStat = new CountingMap<String>();
        perTierStats.put(entry.getKey(), theStat);
      }
      for (Map.Entry<String, AtomicLong> tiers : entry.getValue().entrySet()) {
        theStat.add(tiers.getKey(), tiers.getValue().get());
      }
    }
    for (Map.Entry<String, AtomicLong> entry : stats.globalStats.entrySet()) {
      globalStats.add(entry.getKey(), entry.getValue().get());
    }
    return this;
  }
}
