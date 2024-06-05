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
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Contains statistics typically tracked during a single coordinator run or the
 * runtime of a single coordinator duty.
 */
@ThreadSafe
public class CoordinatorRunStats
{
  private static final CoordinatorRunStats EMPTY_INSTANCE = new CoordinatorRunStats()
  {
    @Override
    public void add(CoordinatorStat stat, RowKey rowKey, long value)
    {
      throw new UnsupportedOperationException("Cannot add stats to empty CoordinatorRunStats instance");
    }

    @Override
    public void updateMax(CoordinatorStat stat, RowKey rowKey, long value)
    {
      throw new UnsupportedOperationException("Cannot add stats to empty CoordinatorRunStats instance");
    }
  };

  private final ConcurrentHashMap<RowKey, Object2LongOpenHashMap<CoordinatorStat>>
      allStats = new ConcurrentHashMap<>();
  private final Map<Dimension, String> debugDimensions = new HashMap<>();

  public static CoordinatorRunStats empty()
  {
    return EMPTY_INSTANCE;
  }

  public CoordinatorRunStats()
  {
    this(null);
  }

  /**
   * Creates a new {@code CoordinatorRunStats}.
   *
   * @param debugDimensions Dimension values for which all metrics should be
   *                        collected and logged.
   */
  public CoordinatorRunStats(Map<Dimension, String> debugDimensions)
  {
    if (debugDimensions != null) {
      this.debugDimensions.putAll(debugDimensions);
    }
  }

  public long getSegmentStat(CoordinatorStat stat, String tier, String datasource)
  {
    return get(stat, RowKey.with(Dimension.DATASOURCE, datasource).and(Dimension.TIER, tier));
  }

  public long get(CoordinatorStat stat)
  {
    return get(stat, RowKey.EMPTY);
  }

  public long get(CoordinatorStat stat, RowKey rowKey)
  {
    Object2LongOpenHashMap<CoordinatorStat> statValues = allStats.get(rowKey);
    return statValues == null ? 0 : statValues.getLong(stat);
  }

  public void forEachStat(StatHandler handler)
  {
    allStats.forEach(
        (rowKey, stats) -> stats.object2LongEntrySet().fastForEach(
            stat -> handler.handle(stat.getKey(), rowKey, stat.getLongValue())
        )
    );
  }

  /**
   * Builds a printable table of all the collected error, info and debug level
   * stats (if there are qualifying debugDimensions) with non-zero values.
   */
  public String buildStatsTable()
  {
    final StringBuilder statsTable = new StringBuilder();
    final AtomicInteger hiddenStats = new AtomicInteger(0);
    final AtomicInteger totalStats = new AtomicInteger();

    allStats.forEach(
        (rowKey, statMap) -> {
          // Categorize the stats by level
          final Map<CoordinatorStat.Level, Map<CoordinatorStat, Long>> levelToStats
              = new EnumMap<>(CoordinatorStat.Level.class);

          statMap.object2LongEntrySet().fastForEach(
              stat -> levelToStats.computeIfAbsent(stat.getKey().getLevel(), l -> new HashMap<>())
                                  .put(stat.getKey(), stat.getLongValue())
          );

          // Add all the errors
          final Map<CoordinatorStat, Long> errorStats = levelToStats
              .getOrDefault(CoordinatorStat.Level.ERROR, Collections.emptyMap());
          totalStats.addAndGet(errorStats.size());
          if (!errorStats.isEmpty()) {
            statsTable.append(
                StringUtils.format("\nError: %s ==> %s", rowKey, errorStats)
            );
          }

          // Add all the info level stats
          final Map<CoordinatorStat, Long> infoStats = levelToStats
              .getOrDefault(CoordinatorStat.Level.INFO, Collections.emptyMap());
          totalStats.addAndGet(infoStats.size());
          if (!infoStats.isEmpty()) {
            statsTable.append(
                StringUtils.format("\nInfo : %s ==> %s", rowKey, infoStats)
            );
          }

          // Add all the debug level stats if the row key has a debug dimension
          final Map<CoordinatorStat, Long> debugStats = levelToStats
              .getOrDefault(CoordinatorStat.Level.DEBUG, Collections.emptyMap());
          totalStats.addAndGet(debugStats.size());
          if (!debugStats.isEmpty() && hasDebugDimension(rowKey)) {
            statsTable.append(
                StringUtils.format("\nDebug: %s ==> %s", rowKey, debugStats)
            );
          } else {
            hiddenStats.addAndGet(debugStats.size());
          }
        }
    );

    if (hiddenStats.get() > 0) {
      statsTable.append(
          StringUtils.format("\nDebug: %d hidden stats. Set 'debugDimensions' to see these.", hiddenStats.get())
      );
    }
    if (totalStats.get() > 0) {
      statsTable.append(
          StringUtils.format("\nTOTAL: %d stats for %d dimension keys", totalStats.get(), rowCount())
      );
    }

    return statsTable.toString();
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
    add(stat, RowKey.EMPTY, value);
  }

  public void add(CoordinatorStat stat, RowKey rowKey, long value)
  {
    allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>())
            .addTo(stat, value);
  }

  public void addToSegmentStat(CoordinatorStat stat, String tier, String datasource, long value)
  {
    RowKey rowKey = RowKey.with(Dimension.TIER, tier)
                          .and(Dimension.DATASOURCE, datasource);
    add(stat, rowKey, value);
  }

  /**
   * Updates the maximum value of the stat for the given RowKey if applicable.
   */
  public void updateMax(CoordinatorStat stat, RowKey rowKey, long value)
  {
    allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>())
            .mergeLong(stat, value, Math::max);
  }

  /**
   * Checks if the given rowKey has any of the debug dimensions.
   */
  private boolean hasDebugDimension(RowKey rowKey)
  {
    if (debugDimensions.isEmpty()) {
      return false;
    } else if (rowKey.getValues().isEmpty()) {
      return true;
    }

    for (Map.Entry<Dimension, String> entry : rowKey.getValues().entrySet()) {
      String expectedValue = debugDimensions.get(entry.getKey());
      if (Objects.equals(expectedValue, entry.getValue())) {
        return true;
      }
    }

    return false;
  }

  public interface StatHandler
  {
    void handle(CoordinatorStat stat, RowKey rowKey, long statValue);
  }

}
