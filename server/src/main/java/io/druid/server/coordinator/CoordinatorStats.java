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
