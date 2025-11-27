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

package org.apache.druid.query.groupby;

import org.apache.druid.guice.LazySingleton;

/**
 * Aggregates {@link GroupByQueryMetrics} emitted by in-flight groupBy queries and exposes a snapshot that can be
 * periodically consumed by {@link org.apache.druid.server.metrics.GroupByStatsMonitor}. The provider keeps track of
 * aggregate counters such as merge-buffer acquisition time, spilled bytes, and the dictionary sizes used while
 * merging results.
 */
@LazySingleton
public class GroupByStatsProvider
{
  private final AggregateStats aggregateStatsContainer;

  public GroupByStatsProvider()
  {
    this.aggregateStatsContainer = AggregateStats.EMPTY_STATS;
  }

  /**
   * Adds the stats reported by a single query execution to the shared accumulator. Callers are expected to provide
   * the {@link GroupByQueryMetrics} associated with the query once all relevant numbers have been recorded on the
   * metrics instance.
   *
   * @param groupByQueryMetrics the query metrics to merge into the aggregate view
   */
  public void aggregateStats(GroupByQueryMetrics groupByQueryMetrics)
  {
    aggregateStatsContainer.addQueryStats(groupByQueryMetrics);
  }

  public synchronized AggregateStats getStatsSince()
  {
    return aggregateStatsContainer.reset();
  }

  /**
   * Immutable snapshot of the aggregated groupBy metrics captured between two {@link #getStatsSince()} calls.
   */
  public static class AggregateStats
  {
    private long mergeBufferQueries;
    private long mergeBufferAcquisitionTimeNs;
    private long spilledQueries;
    private long spilledBytes;
    private long mergeDictionarySize;

    public static final AggregateStats EMPTY_STATS = new AggregateStats(0L, 0L, 0L, 0L, 0L);

    public AggregateStats(
        long mergeBufferQueries,
        long mergeBufferAcquisitionTimeNs,
        long spilledQueries,
        long spilledBytes,
        long mergeDictionarySize
    )
    {
      this.mergeBufferQueries = mergeBufferQueries;
      this.mergeBufferAcquisitionTimeNs = mergeBufferAcquisitionTimeNs;
      this.spilledQueries = spilledQueries;
      this.spilledBytes = spilledBytes;
      this.mergeDictionarySize = mergeDictionarySize;
    }

    public long getMergeBufferQueries()
    {
      return mergeBufferQueries;
    }

    public long getMergeBufferAcquisitionTimeNs()
    {
      return mergeBufferAcquisitionTimeNs;
    }

    public long getSpilledQueries()
    {
      return spilledQueries;
    }

    public long getSpilledBytes()
    {
      return spilledBytes;
    }

    public long getMergeDictionarySize()
    {
      return mergeDictionarySize;
    }

    private void addQueryStats(GroupByQueryMetrics groupByQueryMetrics)
    {
      if (groupByQueryMetrics.getMergeBufferAcquisitionTime() > 0) {
        mergeBufferQueries++;
        mergeBufferAcquisitionTimeNs += groupByQueryMetrics.getMergeBufferAcquisitionTime();
      }

      if (groupByQueryMetrics.getSpilledBytes() > 0) {
        spilledQueries++;
        spilledBytes += groupByQueryMetrics.getSpilledBytes();
      }

      mergeDictionarySize += groupByQueryMetrics.getMergeDictionarySize();
    }

    private AggregateStats reset()
    {
      AggregateStats aggregateStats =
          new AggregateStats(
              mergeBufferQueries,
              mergeBufferAcquisitionTimeNs,
              spilledQueries,
              spilledBytes,
              mergeDictionarySize
          );

      this.mergeBufferQueries = 0;
      this.mergeBufferAcquisitionTimeNs = 0;
      this.spilledQueries = 0;
      this.spilledBytes = 0;
      this.mergeDictionarySize = 0;

      return aggregateStats;
    }
  }
}
