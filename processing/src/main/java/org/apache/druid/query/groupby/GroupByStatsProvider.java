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
 * Metrics collector for groupBy queries like spilled bytes, merge buffer acquistion time, dictionary size.
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
    return aggregateStatsContainer.reset();
  }

  public static class AggregateStats
  {
    private long mergeBufferQueries = 0;
    private long mergeBufferAcquisitionTimeNs = 0;
    private long spilledQueries = 0;
    private long spilledBytes = 0;
    private long mergeDictionarySize = 0;

    public AggregateStats()
    {
    }

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

    public void addQueryStats(PerQueryStats perQueryStats)
    {
      if (perQueryStats.getMergeBufferAcquisitionTimeNs() > 0) {
        mergeBufferQueries++;
        mergeBufferAcquisitionTimeNs += perQueryStats.getMergeBufferAcquisitionTimeNs();
      }

      if (perQueryStats.getSpilledBytes() > 0) {
        spilledQueries++;
        spilledBytes += perQueryStats.getSpilledBytes();
      }

      mergeDictionarySize += perQueryStats.getMergeDictionarySize();
    }

    public AggregateStats reset()
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

  public static class PerQueryStats
  {
    private final AtomicLong mergeBufferAcquisitionTimeNs = new AtomicLong(0);
    private final AtomicLong spilledBytes = new AtomicLong(0);
    private final AtomicLong mergeDictionarySize = new AtomicLong(0);

    public void mergeBufferAcquisitionTime(long delay)
    {
      mergeBufferAcquisitionTimeNs.addAndGet(delay);
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
