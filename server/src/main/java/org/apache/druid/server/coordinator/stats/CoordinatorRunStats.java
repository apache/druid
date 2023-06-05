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
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains statistics typically tracked during a single coordinator run or the
 * runtime of a single coordinator duty.
 */
@ThreadSafe
public class CoordinatorRunStats
{
  private final ConcurrentHashMap<RowKey, Object2LongOpenHashMap<CoordinatorStat>>
      allStats = new ConcurrentHashMap<>();
  private final Map<Dimension, String> debugDimensions = new HashMap<>();

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
    return get(stat, RowKey.builder().add(Dimension.DATASOURCE, datasource).add(Dimension.TIER, tier).build());
  }

  public long getDataSourceStat(CoordinatorStat stat, String dataSource)
  {
    return get(stat, RowKey.forDatasource(dataSource));
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
            stat -> handler.handle(rowKey.getValues(), stat.getKey(), stat.getLongValue())
        )
    );
  }

  /**
   * Logs all the error, info and debug level stats (if applicable) with non-zero
   * values, using the given logger.
   */
  public void logStatsAndErrors(Logger log)
  {
    allStats.forEach(
        (rowKey, statMap) -> {
          // Categorize the stats by level
          final Map<CoordinatorStat.Level, Map<CoordinatorStat, Long>> levelToStats
              = new EnumMap<>(CoordinatorStat.Level.class);

          statMap.object2LongEntrySet().fastForEach(
              stat -> {
                if (stat.getLongValue() == 0) {
                  return;
                }

                levelToStats.computeIfAbsent(stat.getKey().getLevel(), l -> new HashMap<>())
                            .put(stat.getKey(), stat.getLongValue());
              }
          );

          // Log all the errors
          final Map<CoordinatorStat, Long> errorStats = levelToStats
              .getOrDefault(CoordinatorStat.Level.ERROR, Collections.emptyMap());
          if (!errorStats.isEmpty()) {
            log.error("Errors for dimensions [%s]: %s", rowKey, errorStats);
          }

          // Log all the infos
          final Map<CoordinatorStat, Long> infoStats = levelToStats
              .getOrDefault(CoordinatorStat.Level.INFO, Collections.emptyMap());
          if (!infoStats.isEmpty()) {
            log.info("Collected stats for dimensions [%s] are [%s].", rowKey, infoStats);
          }

          // Log all the debugs
          final Map<CoordinatorStat, Long> debugStats = levelToStats
              .getOrDefault(CoordinatorStat.Level.DEBUG, Collections.emptyMap());
          if (!debugStats.isEmpty() && hasDebugDimension(rowKey)) {
            log.info("Debug stats for dimensions [%s] are [%s].", rowKey, debugStats);
          }
        }
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
    // Do not add a stat which will neither be emitted nor logged
    if (!stat.shouldEmit()
        && stat.getLevel() == CoordinatorStat.Level.DEBUG
        && !hasDebugDimension(rowKey)) {
      return;
    }

    allStats.computeIfAbsent(rowKey, d -> new Object2LongOpenHashMap<>())
            .addTo(stat, value);
  }

  public void addToDatasourceStat(CoordinatorStat stat, String dataSource, long value)
  {
    add(stat, RowKey.forDatasource(dataSource), value);
  }

  public void addToSegmentStat(CoordinatorStat stat, String tier, String datasource, long value)
  {
    RowKey rowKey = RowKey.builder()
                          .add(Dimension.TIER, tier)
                          .add(Dimension.DATASOURCE, datasource).build();
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
   * Creates a new {@code CoordinatorRunStats} which represents the snapshot of
   * the stats collected so far in this instance.
   * <p>
   * While this method is in progress, any updates made to the stats of this
   * instance by another thread are not guaranteed to be present in the snapshot.
   * But the snapshots are consistent, i.e. stats present in the snapshot created
   * in one invocation of this method are permanently removed from this instance
   * and will not be present in subsequent snapshots.
   *
   * @return Snapshot of the current state of this {@code CoordinatorRunStats}.
   */
  public CoordinatorRunStats getSnapshotAndReset()
  {
    final CoordinatorRunStats snapshot = new CoordinatorRunStats(debugDimensions);

    // Get a snapshot of all the keys, remove and copy each of them atomically
    final Set<RowKey> keys = new HashSet<>(allStats.keySet());
    for (RowKey key : keys) {
      snapshot.allStats.put(key, allStats.remove(key));
    }

    return snapshot;
  }

  /**
   * Checks if the given rowKey has any of the debug dimensions.
   */
  private boolean hasDebugDimension(RowKey rowKey)
  {
    if (debugDimensions.isEmpty()) {
      return false;
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
    void handle(Map<Dimension, String> dimensionValues, CoordinatorStat stat, long statValue);
  }

}
