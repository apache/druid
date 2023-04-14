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

package org.apache.druid.server.coordinator.stats;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;

/**
 * Contains statistics tracked during a single coordinator run or the runtime of
 * a single coordinator duty.
 */
@ThreadSafe
public class CoordinatorRunStats
{
  private final ConcurrentHashMap<RowKey, Object2LongOpenHashMap<CoordinatorStat>> allStats
      = new ConcurrentHashMap<>();

  public long getTieredStat(CoordinatorStat stat, String tier)
  {
    return get(stat, RowKey.builder().add(Dimension.TIER, tier).build());
  }

  public long getSegmentStat(CoordinatorStat stat, String tier, String datasource)
  {
    return get(stat, RowKey.builder().add(Dimension.DATASOURCE, datasource).add(Dimension.TIER, tier).build());
  }

  public long getDataSourceStat(CoordinatorStat stat, String dataSource)
  {
    return get(stat, RowKey.builder().add(Dimension.DATASOURCE, dataSource).build());
  }

  public long getDutyStat(CoordinatorStat stat, String duty)
  {
    return get(stat, RowKey.builder().add(Dimension.DUTY, duty).build());
  }

  public long get(CoordinatorStat stat)
  {
    return get(stat, RowKey.EMPTY);
  }

  public void forEachStat(StatHandler handler)
  {
    allStats.forEach(
        (rowKey, stats) -> stats.object2LongEntrySet().fastForEach(
            stat -> handler.handle(rowKey.getValues(), stat.getKey(), stat.getLongValue())
        )
    );
  }

  /**
   * Applies the given consumer to each row of the stats table. A single row is
   * identified by a unique combination of dimension values and may have multiple
   * stats (aka metrics) in it.
   */
  public void forEachRow(BiConsumer<RowKey, Map<CoordinatorStat, Long>> consumer)
  {
    allStats.forEach(consumer);
  }

  public boolean hasStat(CoordinatorStat stat)
  {
    for (Object2LongOpenHashMap<CoordinatorStat> statValues : allStats.values()) {
      if (statValues.containsKey(stat)) {
        return true;
      }
    }
    return false;
  }

  public int rowCount()
  {
    return allStats.size();
  }

  public void clear()
  {
    allStats.clear();
  }

  public void add(CoordinatorStat stat, long value)
  {
    add(value, stat, RowKey.EMPTY);
  }

  public void add(CoordinatorStat stat, RowKey rowKey, long value)
  {
    add(value, stat, rowKey);
  }

  public void addToTieredStat(CoordinatorStat stat, String tier, long value)
  {
    add(value, stat, RowKey.builder().add(Dimension.TIER, tier).build());
  }

  public void addToServerStat(CoordinatorStat stat, String serverName, long value)
  {
    add(value, stat, RowKey.builder().add(Dimension.SERVER, serverName).build());
  }

  public void addToDutyStat(CoordinatorStat stat, String duty, long value)
  {
    add(value, stat, RowKey.builder().add(Dimension.DUTY, duty).build());
  }

  public void addToDatasourceStat(CoordinatorStat stat, String dataSource, long value)
  {
    add(value, stat, RowKey.builder().add(Dimension.DATASOURCE, dataSource).build());
  }

  public void addToSegmentStat(CoordinatorStat stat, String tier, String datasource, long value)
  {
    RowKey rowKey = RowKey.builder()
                          .add(Dimension.TIER, tier)
                          .add(Dimension.DATASOURCE, datasource).build();
    add(value, stat, rowKey);
  }

  public void accumulateMaxTieredStat(CoordinatorStat stat, final String tier, final long value)
  {
    final RowKey rowKey = RowKey.builder().add(Dimension.TIER, tier).build();
    allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>())
            .mergeLong(stat, value, Math::max);
  }

  public void accumulate(final CoordinatorRunStats other)
  {
    other.allStats.forEach(
        (rowKey, otherStatValues) -> {
          Object2LongOpenHashMap<CoordinatorStat> statValues =
              this.allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>());

          otherStatValues.object2LongEntrySet().fastForEach(
              stat -> statValues.addTo(stat.getKey(), stat.getLongValue())
          );
        }
    );
  }

  private long get(CoordinatorStat stat, RowKey rowKey)
  {
    Object2LongOpenHashMap<CoordinatorStat> statValues = allStats.get(rowKey);
    return statValues == null ? 0 : statValues.getLong(stat);
  }

  private void add(long value, CoordinatorStat stat, RowKey rowKey)
  {
    allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>())
            .addTo(stat, value);
  }

  public interface StatHandler
  {
    void handle(Map<Dimension, String> dimensionValues, CoordinatorStat stat, long statValue);
  }

}
