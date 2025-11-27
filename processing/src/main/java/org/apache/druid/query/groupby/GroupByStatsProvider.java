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

import javax.annotation.Nullable;

/**
 * Metrics collector for groupBy queries like spilled bytes, merge buffer acquistion time, dictionary size.
 */
@LazySingleton
public class GroupByStatsProvider
{
  private final AggregateStats aggregateStatsContainer;

  public GroupByStatsProvider()
  {
    this.aggregateStatsContainer = AggregateStats.EMPTY_STATS;
  }

  public void aggregateStats(GroupByQueryMetrics groupByQueryMetrics)
  {
    aggregateStatsContainer.addQueryStats(groupByQueryMetrics);
  }

  public synchronized AggregateStats getStatsSince()
  {
    return aggregateStatsContainer.reset();
  }

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
