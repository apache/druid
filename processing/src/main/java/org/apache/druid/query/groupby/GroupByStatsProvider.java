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
import org.apache.druid.query.QueryResourceId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collects groupBy query metrics (spilled bytes, merge buffer usage, dictionary size) per-query, then
 * aggregates them when queries complete. Stats are retrieved and reset periodically via {@link #getStatsSince()}.
 */
@LazySingleton
public class GroupByStatsProvider
{
  private final Map<QueryResourceId, PerQueryStats> perQueryStats;
  private final AggregateStats aggregateStatsContainer;

  public GroupByStatsProvider()
  {
    this.perQueryStats = new ConcurrentHashMap<>();
    this.aggregateStatsContainer = new AggregateStats();
  }

  public PerQueryStats getPerQueryStatsContainer(QueryResourceId resourceId)
  {
    if (resourceId == null) {
      return null;
    }
    return perQueryStats.computeIfAbsent(resourceId, value -> new PerQueryStats());
  }

  public synchronized void closeQuery(QueryResourceId resourceId)
  {
    if (resourceId == null || !perQueryStats.containsKey(resourceId)) {
      return;
    }
    PerQueryStats container = perQueryStats.remove(resourceId);
    aggregateStatsContainer.addQueryStats(container);
  }

  public synchronized AggregateStats getStatsSince()
  {
    AggregateStats aggregateStats = new AggregateStats(aggregateStatsContainer);
    aggregateStatsContainer.reset();
    return aggregateStats;
  }

  public static class AggregateStats
  {
    private long mergeBufferQueries = 0;
    private long mergeBufferAcquisitionTimeNs = 0;
    private long totalMergeBufferUsedBytes = 0;
    private long maxMergeBufferAcquisitionTimeNs = 0;
    private long maxMergeBufferUsedBytes = 0;
    private long spilledQueries = 0;
    private long spilledBytes = 0;
    private long maxSpilledBytes = 0;
    private long mergeDictionarySize = 0;
    private long maxMergeDictionarySize = 0;

    public AggregateStats()
    {
    }

    public AggregateStats(AggregateStats aggregateStats)
    {
      this(
          aggregateStats.mergeBufferQueries,
          aggregateStats.mergeBufferAcquisitionTimeNs,
          aggregateStats.totalMergeBufferUsedBytes,
          aggregateStats.maxMergeBufferAcquisitionTimeNs,
          aggregateStats.maxMergeBufferUsedBytes,
          aggregateStats.spilledQueries,
          aggregateStats.spilledBytes,
          aggregateStats.maxSpilledBytes,
          aggregateStats.mergeDictionarySize,
          aggregateStats.maxMergeDictionarySize
      );
    }

    public AggregateStats(
        long mergeBufferQueries,
        long mergeBufferAcquisitionTimeNs,
        long totalMergeBufferUsedBytes,
        long maxMergeBufferAcquisitionTimeNs,
        long maxMergeBufferUsedBytes,
        long spilledQueries,
        long spilledBytes,
        long maxSpilledBytes,
        long mergeDictionarySize,
        long maxMergeDictionarySize
    )
    {
      this.mergeBufferQueries = mergeBufferQueries;
      this.mergeBufferAcquisitionTimeNs = mergeBufferAcquisitionTimeNs;
      this.totalMergeBufferUsedBytes = totalMergeBufferUsedBytes;
      this.maxMergeBufferAcquisitionTimeNs = maxMergeBufferAcquisitionTimeNs;
      this.maxMergeBufferUsedBytes = maxMergeBufferUsedBytes;
      this.spilledQueries = spilledQueries;
      this.spilledBytes = spilledBytes;
      this.maxSpilledBytes = maxSpilledBytes;
      this.mergeDictionarySize = mergeDictionarySize;
      this.maxMergeDictionarySize = maxMergeDictionarySize;
    }

    public long getMergeBufferQueries()
    {
      return mergeBufferQueries;
    }

    public long getMergeBufferAcquisitionTimeNs()
    {
      return mergeBufferAcquisitionTimeNs;
    }

    public long getMaxMergeBufferAcquisitionTimeNs()
    {
      return maxMergeBufferAcquisitionTimeNs;
    }

    public long getTotalMergeBufferUsedBytes()
    {
      return totalMergeBufferUsedBytes;
    }

    public long getMaxMergeBufferUsedBytes()
    {
      return maxMergeBufferUsedBytes;
    }

    public long getSpilledQueries()
    {
      return spilledQueries;
    }

    public long getSpilledBytes()
    {
      return spilledBytes;
    }

    public long getMaxSpilledBytes()
    {
      return maxSpilledBytes;
    }

    public long getMergeDictionarySize()
    {
      return mergeDictionarySize;
    }

    public long getMaxMergeDictionarySize()
    {
      return maxMergeDictionarySize;
    }

    public void addQueryStats(PerQueryStats perQueryStats)
    {
      if (perQueryStats.getMergeBufferAcquisitionTimeNs() > 0) {
        mergeBufferQueries++;
        mergeBufferAcquisitionTimeNs += perQueryStats.getMergeBufferAcquisitionTimeNs();
        maxMergeBufferAcquisitionTimeNs = Math.max(
            maxMergeBufferAcquisitionTimeNs,
            perQueryStats.getMergeBufferAcquisitionTimeNs()
        );
        totalMergeBufferUsedBytes += perQueryStats.getMergeBufferTotalUsedBytes();
        maxMergeBufferUsedBytes = Math.max(maxMergeBufferUsedBytes, perQueryStats.getMergeBufferTotalUsedBytes());
      }

      if (perQueryStats.getSpilledBytes() > 0) {
        spilledQueries++;
        spilledBytes += perQueryStats.getSpilledBytes();
        maxSpilledBytes = Math.max(maxSpilledBytes, perQueryStats.getSpilledBytes());
      }

      mergeDictionarySize += perQueryStats.getMergeDictionarySize();
      maxMergeDictionarySize = Math.max(maxMergeDictionarySize, perQueryStats.getMergeDictionarySize());
    }

    public void reset()
    {
      this.mergeBufferQueries = 0;
      this.mergeBufferAcquisitionTimeNs = 0;
      this.maxMergeBufferAcquisitionTimeNs = 0;
      this.totalMergeBufferUsedBytes = 0;
      this.maxMergeBufferUsedBytes = 0;
      this.spilledQueries = 0;
      this.spilledBytes = 0;
      this.maxSpilledBytes = 0;
      this.mergeDictionarySize = 0;
      this.maxMergeDictionarySize = 0;
    }
  }

  public static class PerQueryStats
  {
    private final AtomicLong mergeBufferAcquisitionTimeNs = new AtomicLong(0);
    private final AtomicLong mergeBufferTotalUsedBytes = new AtomicLong(0);
    private final AtomicLong spilledBytes = new AtomicLong(0);
    private final AtomicLong mergeDictionarySize = new AtomicLong(0);

    public void mergeBufferAcquisitionTime(long delay)
    {
      mergeBufferAcquisitionTimeNs.addAndGet(delay);
    }

    public void mergeBufferTotalUsedBytes(long bytes)
    {
      mergeBufferTotalUsedBytes.addAndGet(bytes);
    }

    public void spilledBytes(long bytes)
    {
      spilledBytes.addAndGet(bytes);
    }

    public void dictionarySize(long size)
    {
      mergeDictionarySize.addAndGet(size);
    }

    public long getMergeBufferAcquisitionTimeNs()
    {
      return mergeBufferAcquisitionTimeNs.get();
    }

    public long getMergeBufferTotalUsedBytes()
    {
      return mergeBufferTotalUsedBytes.get();
    }

    public long getSpilledBytes()
    {
      return spilledBytes.get();
    }

    public long getMergeDictionarySize()
    {
      return mergeDictionarySize.get();
    }
  }
}
