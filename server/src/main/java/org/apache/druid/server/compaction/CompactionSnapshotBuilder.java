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

package org.apache.druid.server.compaction;

import org.apache.druid.server.coordinator.AutoCompactionSnapshot;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.coordinator.stats.Dimension;
import org.apache.druid.server.coordinator.stats.RowKey;
import org.apache.druid.server.coordinator.stats.Stats;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds {@link AutoCompactionSnapshot} for multiple datasources using the
 * identified {@link CompactionCandidate} list.
 */
public class CompactionSnapshotBuilder
{
  private final CoordinatorRunStats stats;
  private final Map<String, AutoCompactionSnapshot.Builder> datasourceToBuilder = new HashMap<>();

  private final List<CompactionCandidate> completed = new ArrayList<>();
  private final List<CompactionCandidate> skipped = new ArrayList<>();

  public CompactionSnapshotBuilder(CoordinatorRunStats runStats)
  {
    this.stats = runStats;
  }

  public List<CompactionCandidate> getCompleted()
  {
    return completed;
  }

  public List<CompactionCandidate> getSkipped()
  {
    return skipped;
  }

  public void addToComplete(CompactionCandidate candidate)
  {
    getBuilderForDatasource(candidate.getDataSource())
        .incrementCompactedStats(candidate.getStats());
    completed.add(candidate);
  }

  public void addToPending(CompactionCandidate candidate)
  {
    getBuilderForDatasource(candidate.getDataSource())
        .incrementWaitingStats(getUncompactedStats(candidate));

    final CompactionStatistics compactedStats = candidate.getCompactedStats();
    if (compactedStats != null) {
      getBuilderForDatasource(candidate.getDataSource())
          .incrementCompactedStats(compactedStats);
    }
  }

  public void addToSkipped(CompactionCandidate candidate)
  {
    getBuilderForDatasource(candidate.getDataSource())
        .incrementSkippedStats(getUncompactedStats(candidate));
    skipped.add(candidate);
  }

  public void moveFromPendingToSkipped(CompactionCandidate candidate)
  {
    getBuilderForDatasource(candidate.getDataSource())
        .decrementWaitingStats(getUncompactedStats(candidate));
    addToSkipped(candidate);
  }

  public void moveFromPendingToCompleted(CompactionCandidate candidate)
  {
    getBuilderForDatasource(candidate.getDataSource())
        .decrementWaitingStats(getUncompactedStats(candidate));
    addToComplete(candidate);
  }

  public Map<String, AutoCompactionSnapshot> build()
  {
    final Map<String, AutoCompactionSnapshot> datasourceToSnapshot = new HashMap<>();
    datasourceToBuilder.forEach((dataSource, builder) -> {
      final AutoCompactionSnapshot autoCompactionSnapshot = builder.build();
      datasourceToSnapshot.put(dataSource, autoCompactionSnapshot);
      collectSnapshotStats(autoCompactionSnapshot);
    });

    return datasourceToSnapshot;
  }

  /**
   * Gets the stats for uncompacted segments in the given candidate.
   * If details of uncompacted segments is not available, all segments within the
   * candidate are considered to be uncompacted.
   */
  private CompactionStatistics getUncompactedStats(CompactionCandidate candidate)
  {
    final CompactionStatistics uncompacted = candidate.getUncompactedStats();
    return uncompacted == null ? candidate.getStats() : uncompacted;
  }

  private AutoCompactionSnapshot.Builder getBuilderForDatasource(String dataSource)
  {
    return datasourceToBuilder.computeIfAbsent(dataSource, AutoCompactionSnapshot::builder);
  }

  private void collectSnapshotStats(AutoCompactionSnapshot autoCompactionSnapshot)
  {
    final RowKey rowKey = RowKey.of(Dimension.DATASOURCE, autoCompactionSnapshot.getDataSource());

    stats.add(Stats.Compaction.PENDING_BYTES, rowKey, autoCompactionSnapshot.getBytesAwaitingCompaction());
    stats.add(Stats.Compaction.PENDING_SEGMENTS, rowKey, autoCompactionSnapshot.getSegmentCountAwaitingCompaction());
    stats.add(Stats.Compaction.PENDING_INTERVALS, rowKey, autoCompactionSnapshot.getIntervalCountAwaitingCompaction());
    stats.add(Stats.Compaction.COMPACTED_BYTES, rowKey, autoCompactionSnapshot.getBytesCompacted());
    stats.add(Stats.Compaction.COMPACTED_SEGMENTS, rowKey, autoCompactionSnapshot.getSegmentCountCompacted());
    stats.add(Stats.Compaction.COMPACTED_INTERVALS, rowKey, autoCompactionSnapshot.getIntervalCountCompacted());
    stats.add(Stats.Compaction.SKIPPED_BYTES, rowKey, autoCompactionSnapshot.getBytesSkipped());
    stats.add(Stats.Compaction.SKIPPED_SEGMENTS, rowKey, autoCompactionSnapshot.getSegmentCountSkipped());
    stats.add(Stats.Compaction.SKIPPED_INTERVALS, rowKey, autoCompactionSnapshot.getIntervalCountSkipped());
  }
}
