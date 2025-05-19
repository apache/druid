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

package org.apache.druid.segment.realtime;

import com.google.common.annotations.VisibleForTesting;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Metrics for segment generation.
 */
public class SegmentGenerationMetrics
{
  private static final long NO_EMIT_SEGMENT_HANDOFF_TIME = -1L;

  private static final long NO_EMIT_MESSAGE_GAP = -1L;

  private final AtomicLong dedupCount = new AtomicLong(0);
  private final AtomicLong rowOutputCount = new AtomicLong(0);
  private final AtomicLong numPersists = new AtomicLong(0);
  private final AtomicLong persistTimeMillis = new AtomicLong(0);
  private final AtomicLong persistBackPressureMillis = new AtomicLong(0);
  private final AtomicLong failedPersists = new AtomicLong(0);
  private final AtomicLong failedHandoffs = new AtomicLong(0);
  // Measures the number of rows that have been merged. Segments are merged into a single file before they are pushed to deep storage.
  private final AtomicLong mergedRows = new AtomicLong(0);
  // Measures the number of rows that have been pushed to deep storage.
  private final AtomicLong pushedRows = new AtomicLong(0);
  private final AtomicLong mergeTimeMillis = new AtomicLong(0);
  private final AtomicLong mergeCpuTime = new AtomicLong(0);
  private final AtomicLong persistCpuTime = new AtomicLong(0);
  private final AtomicLong handOffCount = new AtomicLong(0);
  private final AtomicLong sinkCount = new AtomicLong(0);
  private final AtomicLong messageMaxTimestamp = new AtomicLong(0);
  private final AtomicLong messageGap = new AtomicLong(0);
  private final AtomicBoolean processingDone = new AtomicBoolean(false);

  private final AtomicLong maxSegmentHandoffTime = new AtomicLong(NO_EMIT_SEGMENT_HANDOFF_TIME);

  /**
   * {@code MessageGapStats} tracks message gap statistics and is thread-safe.
   */
  public static class MessageGapStats
  {
    private long min = Long.MAX_VALUE;
    private long max = Long.MIN_VALUE;
    private long count = 0;
    private double total = 0;

    public synchronized double avg()
    {
      return total / count;
    }

    public synchronized long min()
    {
      return min;
    }

    public synchronized long max()
    {
      return max;
    }

    public synchronized long count()
    {
      return count;
    }

    public synchronized void add(final long messageGap)
    {
      total += messageGap;
      ++count;
      if (min > messageGap) {
        min = messageGap;
      }
      if (max < messageGap) {
        max = messageGap;
      }
    }

    public MessageGapStats getAndReset()
    {
      final MessageGapStats copy = new MessageGapStats();
      synchronized (this) {
        copy.total = total;
        copy.count = count;
        copy.min = min;
        copy.max = max;

        total = 0;
        count = 0;
        min = Long.MAX_VALUE;
        max = Long.MIN_VALUE;
      }
      return copy;
    }
  }

  private final MessageGapStats messageGapStats = new MessageGapStats();

  public void incrementRowOutputCount(long numRows)
  {
    rowOutputCount.addAndGet(numRows);
  }

  public void incrementNumPersists()
  {
    numPersists.incrementAndGet();
  }

  public void incrementPersistTimeMillis(long millis)
  {
    persistTimeMillis.addAndGet(millis);
  }

  public void incrementPersistBackPressureMillis(long millis)
  {
    persistBackPressureMillis.addAndGet(millis);
  }

  public void incrementFailedPersists()
  {
    failedPersists.incrementAndGet();
  }

  public void incrementFailedHandoffs()
  {
    failedHandoffs.incrementAndGet();
  }

  public void incrementMergedRows(long rows)
  {
    mergedRows.addAndGet(rows);
  }

  public void incrementPushedRows(long rows)
  {
    pushedRows.addAndGet(rows);
  }

  public void incrementHandOffCount()
  {
    handOffCount.incrementAndGet();
  }

  public void setSinkCount(long sinkCount)
  {
    this.sinkCount.set(sinkCount);
  }

  public void reportMessageGap(final long messageGap)
  {
    messageGapStats.add(messageGap);
  }

  public void reportMessageMaxTimestamp(long messageMaxTimestamp)
  {
    if (this.messageMaxTimestamp.get() < messageMaxTimestamp) {
      this.messageMaxTimestamp.getAndAccumulate(messageMaxTimestamp, Math::max);
    }
  }

  public void reportMaxSegmentHandoffTime(long maxSegmentHandoffTime)
  {
    if (this.maxSegmentHandoffTime.get() < maxSegmentHandoffTime) {
      this.maxSegmentHandoffTime.getAndAccumulate(maxSegmentHandoffTime, Math::max);
    }
  }

  public void markProcessingDone()
  {
    this.processingDone.set(true);
  }

  @VisibleForTesting
  public boolean isProcessingDone()
  {
    return processingDone.get();
  }

  public long dedup()
  {
    return dedupCount.get();
  }

  public long rowOutput()
  {
    return rowOutputCount.get();
  }

  public long numPersists()
  {
    return numPersists.get();
  }

  public long persistTimeMillis()
  {
    return persistTimeMillis.get();
  }

  public long persistBackPressureMillis()
  {
    return persistBackPressureMillis.get();
  }

  public long failedPersists()
  {
    return failedPersists.get();
  }

  public long failedHandoffs()
  {
    return failedHandoffs.get();
  }

  public long mergedRows()
  {
    return mergedRows.get();
  }

  public long pushedRows()
  {
    return pushedRows.get();
  }

  public long mergeTimeMillis()
  {
    return mergeTimeMillis.get();
  }

  public long mergeCpuTime()
  {
    return mergeCpuTime.get();
  }

  public long persistCpuTime()
  {
    return persistCpuTime.get();
  }

  public long handOffCount()
  {
    return handOffCount.get();
  }

  public long sinkCount()
  {
    return sinkCount.get();
  }

  /**
   * See {@code MessageGapStats} for current gaurantees on thread-safety.
   */
  public MessageGapStats getMessageGapStats()
  {
    return messageGapStats;
  }

  public long messageGap()
  {
    return messageGap.get();
  }

  public long maxSegmentHandoffTime()
  {
    return maxSegmentHandoffTime.get();
  }

  public SegmentGenerationMetrics snapshot()
  {
    final SegmentGenerationMetrics retVal = new SegmentGenerationMetrics();
    retVal.dedupCount.set(dedupCount.get());
    retVal.rowOutputCount.set(rowOutputCount.get());
    retVal.numPersists.set(numPersists.get());
    retVal.persistTimeMillis.set(persistTimeMillis.get());
    retVal.persistBackPressureMillis.set(persistBackPressureMillis.get());
    retVal.failedPersists.set(failedPersists.get());
    retVal.failedHandoffs.set(failedHandoffs.get());
    retVal.mergeTimeMillis.set(mergeTimeMillis.get());
    retVal.mergeCpuTime.set(mergeCpuTime.get());
    retVal.persistCpuTime.set(persistCpuTime.get());
    retVal.handOffCount.set(handOffCount.get());
    retVal.sinkCount.set(sinkCount.get());
    retVal.maxSegmentHandoffTime.set(maxSegmentHandoffTime.get());
    retVal.mergedRows.set(mergedRows.get());
    retVal.pushedRows.set(pushedRows.get());

    long messageGapSnapshot = 0;
    final long maxTimestamp = messageMaxTimestamp.get();
    retVal.messageMaxTimestamp.set(maxTimestamp);
    if (processingDone.get()) {
      messageGapSnapshot = NO_EMIT_MESSAGE_GAP;
    } else if (maxTimestamp > 0) {
      messageGapSnapshot = System.currentTimeMillis() - maxTimestamp;
    }
    retVal.messageGap.set(messageGapSnapshot);

    final MessageGapStats messageGapStatsSnapshot = messageGapStats.getAndReset();
    retVal.messageGapStats.total = messageGapStatsSnapshot.total;
    retVal.messageGapStats.count = messageGapStatsSnapshot.count;
    retVal.messageGapStats.max = messageGapStatsSnapshot.max;
    retVal.messageGapStats.min = messageGapStatsSnapshot.min;

    reset();

    return retVal;
  }

  private void reset()
  {
    maxSegmentHandoffTime.set(NO_EMIT_SEGMENT_HANDOFF_TIME);
  }
}
