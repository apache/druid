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
    private long maxMergeBufferAcquisitionTimeNs = 0;
    private long totalMergeBufferUsedBytes = 0;
    private long maxMergeBufferUsedBytes = 0;
    private double maxSpillProximity = 0.0;
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
          aggregateStats.maxMergeBufferAcquisitionTimeNs,
          aggregateStats.totalMergeBufferUsedBytes,
          aggregateStats.maxMergeBufferUsedBytes,
          aggregateStats.maxSpillProximity,
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
        long maxMergeBufferAcquisitionTimeNs,
        long totalMergeBufferUsedBytes,
        long maxMergeBufferUsedBytes,
        double maxSpillProximity,
        long spilledQueries,
        long spilledBytes,
        long maxSpilledBytes,
        long mergeDictionarySize,
        long maxMergeDictionarySize
    )
    {
      this.mergeBufferQueries = mergeBufferQueries;
      this.mergeBufferAcquisitionTimeNs = mergeBufferAcquisitionTimeNs;
      this.maxMergeBufferAcquisitionTimeNs = maxMergeBufferAcquisitionTimeNs;
      this.totalMergeBufferUsedBytes = totalMergeBufferUsedBytes;
      this.maxMergeBufferUsedBytes = maxMergeBufferUsedBytes;
      this.maxSpillProximity = maxSpillProximity;
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

    public double getMaxSpillProximity()
    {
      return maxSpillProximity;
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

    /**
     * Folds a completed query's stats into the running aggregate. For merge-buffer usage:
     * <ul>
     *   <li>{@code totalMergeBufferUsedBytes} (emitted as {@code mergeBuffer/bytesUsed}) sums each query's usage
     *       across all queries, where each query's usage is itself the sum across the query's slices.</li>
     *   <li>{@code maxMergeBufferUsedBytes} (emitted as {@code mergeBuffer/maxBytesUsed}) is the max such per-query
     *       summed usage across queries.</li>
     *   <li>{@code maxSpillProximity} (emitted as {@code mergeBuffer/maxSpillProximity}) is the max per-query spill
     *       proximity across queries, where each query's value is its fullest slice's fill fraction of the spill
     *       threshold. Unlike the byte sums above, this is a per-slice MAX so it reflects the slice that drives a
     *       spill; 1.0 means at least one query reached the spill point.</li>
     * </ul>
     */
    public void addQueryStats(PerQueryStats perQueryStats)
    {
      if (perQueryStats.getMergeBufferAcquisitionTimeNs() > 0) {
        mergeBufferQueries++;
        mergeBufferAcquisitionTimeNs += perQueryStats.getMergeBufferAcquisitionTimeNs();
        maxMergeBufferAcquisitionTimeNs = Math.max(
            maxMergeBufferAcquisitionTimeNs,
            perQueryStats.getMergeBufferAcquisitionTimeNs()
        );
        totalMergeBufferUsedBytes += perQueryStats.getMergeBufferUsedBytes();
        maxMergeBufferUsedBytes = Math.max(maxMergeBufferUsedBytes, perQueryStats.getMergeBufferUsedBytes());
        maxSpillProximity = Math.max(maxSpillProximity, perQueryStats.getSpillProximity());
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
      this.maxSpillProximity = 0.0;
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
    /**
     * Sum of the peak merge-buffer usage of every grouper (slice) this query held. A
     * {@code ConcurrentGrouper} slices a single merge buffer into one slice per processing thread, and each slice
     * reports its own peak via
     * {@link #addMergeBufferUsedBytes(long)} when closed, so the per-query value is the SUM across the query's slices.
     */
    private final AtomicLong mergeBufferUsedBytes = new AtomicLong(0);
    /**
     * Peak used bytes of the single fullest slice this query held, tracked as a MAX across the query's slices. A query
     * spills as soon as one slice fills, so spill proximity is driven by the hottest slice, NOT the sum tracked by
     * {@link #mergeBufferUsedBytes}. Compared against {@link #sliceSpillThresholdBytes}.
     */
    private final AtomicLong maxSliceUsedBytes = new AtomicLong(0);
    /**
     * Per-slice spill threshold in bytes, i.e. {@code sliceSize * maxLoadFactor}. A slice's hash table spills once its
     * bucket count reaches the max load factor, so this (not the raw slice size) is the denominator that makes a
     * usage ratio of 1.0 mean "at the spill point". Tracked as a max; all slices of a query share the same value.
     */
    private final AtomicLong sliceSpillThresholdBytes = new AtomicLong(0);
    private final AtomicLong spilledBytes = new AtomicLong(0);
    private final AtomicLong mergeDictionarySize = new AtomicLong(0);

    public void mergeBufferAcquisitionTime(long delay)
    {
      mergeBufferAcquisitionTimeNs.addAndGet(delay);
    }

    /**
     * Accumulates the peak merge-buffer usage of one grouper (slice). Despite the previous "max" naming, this method
     * sums across the slices a query holds; see {@link #mergeBufferUsedBytes}.
     */
    public void addMergeBufferUsedBytes(long bytes)
    {
      mergeBufferUsedBytes.addAndGet(bytes);
    }

    /**
     * Records one slice's peak used bytes and its spill threshold ({@code sliceSize * maxLoadFactor}). Both are tracked
     * as maxima, so after all slices close the pair describes the single fullest slice — the one that drives spilling.
     * Used to compute {@code mergeBuffer/maxSpillProximity}.
     */
    public void sliceUsage(long usedBytes, long spillThresholdBytes)
    {
      maxSliceUsedBytes.accumulateAndGet(usedBytes, Math::max);
      sliceSpillThresholdBytes.accumulateAndGet(spillThresholdBytes, Math::max);
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

    public long getMergeBufferUsedBytes()
    {
      return mergeBufferUsedBytes.get();
    }

    /**
     * Spill proximity for this query in [0.0, 1.0]: the fullest slice's used bytes divided by that slice's spill
     * threshold. 1.0 means a slice reached the point at which it spills to disk. Returns 0.0 when no slice usage was
     * recorded (e.g. a grouper that never initialized).
     */
    public double getSpillProximity()
    {
      long threshold = sliceSpillThresholdBytes.get();
      if (threshold <= 0) {
        return 0.0;
      }
      return Math.min(1.0, (double) maxSliceUsedBytes.get() / threshold);
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
