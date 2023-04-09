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
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.utils.CollectionUtils;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Contains statistics tracked during a single coordinator run or the runtime of
 * a single coordinator duty.
 */
public class CoordinatorRunStats
{
  private static final RowKey EMPTY_ROW_KEY = new RowKey(Collections.emptyMap());

  private final Map<RowKey, Object2LongOpenHashMap<CoordinatorStat>> allStats = new HashMap<>();

  /**
   * @param statName the name of the statistics
   * @param tier     the tier
   * @return the value for the statistics {@code statName} under {@code tier} tier
   * @throws NullPointerException if {@code statName} is not found
   */
  public long getTieredStat(CoordinatorStat statName, String tier)
  {
    return get(statName, RowKey.builder().add(Dimension.TIER, tier).build());
  }

  public void forEachStat(StatHandler handler)
  {
    allStats.forEach(
        (rowKey, stats) -> stats.object2LongEntrySet().fastForEach(
            stat -> handler.handle(
                stat.getKey(),
                CollectionUtils.mapKeys(rowKey.values, Dimension::dimensionName),
                stat.getLongValue()
            )
        )
    );
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

  public long getDataSourceStat(CoordinatorStat statName, String dataSource)
  {
    return get(statName, RowKey.builder().add(Dimension.DATASOURCE, dataSource).build());
  }

  private long get(CoordinatorStat stat, RowKey rowKey)
  {
    Object2LongOpenHashMap<CoordinatorStat> statValues = allStats.get(rowKey);
    return statValues == null ? 0 : statValues.getLong(stat);
  }

  public long getDutyStat(CoordinatorStat statName, String duty)
  {
    return get(statName, RowKey.builder().add(Dimension.DUTY, duty).build());
  }

  public long get(CoordinatorStat statName)
  {
    return get(statName, EMPTY_ROW_KEY);
  }

  public void accumulateMaxTieredStat(CoordinatorStat stat, final String tier, final long value)
  {
    final RowKey rowKey = RowKey.builder().add(Dimension.TIER, tier).build();
    allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>())
            .mergeLong(stat, value, Math::max);
  }

  public void accumulate(final CoordinatorRunStats stats)
  {
    stats.allStats.forEach(
        (rowKey, otherStatValues) -> {
          Object2LongOpenHashMap<CoordinatorStat> statValues =
              this.allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>());

          otherStatValues.object2LongEntrySet().fastForEach(
              stat -> statValues.addTo(stat.getKey(), stat.getLongValue())
          );
        }
    );
  }

  private void add(long value, CoordinatorStat stat, RowKey rowKey)
  {
    allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>())
            .addTo(stat, value);
  }

  public void add(CoordinatorStat stat, long value)
  {
    add(value, stat, EMPTY_ROW_KEY);
  }

  public void addForTier(CoordinatorStat stat, String tier, long value)
  {
    add(value, stat, RowKey.builder().add(Dimension.TIER, tier).build());
  }

  public void addForServer(CoordinatorStat stat, String serverName, long value)
  {
    add(value, stat, RowKey.builder().add(Dimension.SERVER, serverName).build());
  }

  public void addForDuty(CoordinatorStat stat, String duty, long value)
  {
    add(value, stat, RowKey.builder().add(Dimension.DUTY, duty).build());
  }

  public void addForDatasource(CoordinatorStat stat, String dataSource, long value)
  {
    add(value, stat, RowKey.builder().add(Dimension.DATASOURCE, dataSource).build());
  }

  public void addSegmentStat(
      long value,
      CoordinatorStat stat,
      String server,
      String tier,
      String datasource
  )
  {
    RowKey dims = RowKey.builder()
                        .add(Dimension.TIER, tier).add(Dimension.SERVER, server)
                        .add(Dimension.DATASOURCE, datasource).build();
    add(value, stat, dims);
  }

  /**
   * Represents a row key against which stats are reported.
   */
  private static class RowKey
  {
    final Map<Dimension, String> values;
    final int hashCode;

    RowKey(Map<Dimension, String> values)
    {
      this.values = values;
      this.hashCode = Objects.hash(values);
    }

    static RowKeyBuilder builder()
    {
      return new RowKeyBuilder();
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RowKey that = (RowKey) o;
      return Objects.equals(values, that.values);
    }

    @Override
    public int hashCode()
    {
      return hashCode;
    }
  }

  private static class RowKeyBuilder
  {
    final Map<Dimension, String> values = new EnumMap<>(Dimension.class);

    RowKeyBuilder add(Dimension dimension, String value)
    {
      values.put(dimension, value);
      return this;
    }

    RowKey build()
    {
      return new RowKey(values);
    }
  }

  /**
   * Dimensions used while reporting coordinator run stats.
   */
  private enum Dimension
  {
    TIER(DruidMetrics.TIER),
    DATASOURCE(DruidMetrics.DATASOURCE),
    DUTY(DruidMetrics.DUTY),
    SERVER(DruidMetrics.SERVER);

    private final String dimName;

    Dimension(String name)
    {
      this.dimName = name;
    }

    private String dimensionName()
    {
      return dimName;
    }
  }

  public interface StatHandler
  {
    void handle(CoordinatorStat stat, Map<String, String> dimensionValues, long value);
  }

}
