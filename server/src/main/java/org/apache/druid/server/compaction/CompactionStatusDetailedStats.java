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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.common.guava.GuavaUtils;
import org.apache.druid.error.DruidException;

import java.util.HashMap;
import java.util.Map;

/**
 * Collects detailed compaction statistics in table format during dry run mode in overlord.
 */
public class CompactionStatusDetailedStats
{
  private final Map<CompactionStatus.State, Table> stateToTable;
  private final String[] COLUMNS = new String[]{
      "dataSource",
      "interval",
      "numSegments",
      "bytes",
      "rows",
      "uncompactedSegments",
      "uncompactedBytes",
      "uncompactedRows",
      "reasonToCompactOrSkip",
      "mode"
  };

  public CompactionStatusDetailedStats()
  {
    this.stateToTable = new HashMap<>();
    stateToTable.put(CompactionStatus.State.COMPLETE, Table.withColumnNames(COLUMNS));
    stateToTable.put(CompactionStatus.State.RUNNING, Table.withColumnNames(COLUMNS));
    stateToTable.put(CompactionStatus.State.PENDING, Table.withColumnNames(COLUMNS));
    stateToTable.put(CompactionStatus.State.SKIPPED, Table.withColumnNames(COLUMNS));
  }

  @JsonCreator
  public CompactionStatusDetailedStats(
      @JsonProperty("compactionStates") Map<CompactionStatus.State, Table> compactionStates
  )
  {
    this.stateToTable = compactionStates != null ? new HashMap<>(compactionStates) : new HashMap<>();
  }

  public void recordCompactionStatus(CompactionCandidate candidate)
  {
    final CompactionStatus status = candidate.getCurrentStatus();
    CompactionStatistics stats = GuavaUtils.firstNonNull(candidate.getStats(), new CompactionStatistics());
    CompactionStatistics uncompactedStats = GuavaUtils.firstNonNull(
        candidate.getUncompactedStats(),
        new CompactionStatistics()
    );

    final Object[] baseRow = new Object[]{
        candidate.getDataSource(),
        candidate.getCompactionInterval(),
        candidate.numSegments(),
        stats.getTotalBytes(),
        stats.getTotalRows(),
        uncompactedStats.getNumSegments(),
        uncompactedStats.getTotalBytes(),
        uncompactedStats.getTotalRows(),
        status.getReason(),
        null,
    };

    final Table table = stateToTable.get(status.getState());

    switch (status.getState()) {
      case COMPLETE:
      case SKIPPED:
      case PENDING:
        table.addRow(baseRow);
        break;
      case RUNNING:
      default:
        throw DruidException.defensive("unexpected compaction status[%s]", status.getState());
    }
  }

  public void recordSubmittedTask(CompactionCandidate candidate, CompactionMode compactionMode)
  {
    Preconditions.checkNotNull(candidate.getStats(), "compaction stats");
    Preconditions.checkNotNull(candidate.getUncompactedStats(), "uncompacted stats");
    stateToTable.get(CompactionStatus.State.RUNNING).addRow(
        candidate.getDataSource(),
        candidate.getCompactionInterval(),
        candidate.numSegments(),
        candidate.getStats().getTotalBytes(),
        candidate.getStats().getTotalRows(),
        candidate.getUncompactedStats().getNumSegments(),
        candidate.getUncompactedStats().getTotalRows(),
        candidate.getCurrentStatus().getReason(),
        compactionMode
    );
  }

  @JsonProperty("compactionStates")
  public Map<CompactionStatus.State, Table> getCompactionStates()
  {
    final Map<CompactionStatus.State, Table> result = new HashMap<>();
    for (Map.Entry<CompactionStatus.State, Table> entry : stateToTable.entrySet()) {
      if (!entry.getValue().isEmpty()) {
        result.put(entry.getKey(), entry.getValue());
      }
    }
    return result;
  }
}
