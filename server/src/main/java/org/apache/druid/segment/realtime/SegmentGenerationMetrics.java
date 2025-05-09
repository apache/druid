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
import java.util.concurrent.locks.ReentrantLock;

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
  private final AtomicLong mergedRows = new AtomicLong(0);
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

  public static class MessageGapStats
  {
    long minMessageGap = Long.MAX_VALUE;
    long maxMessageGap = Long.MIN_VALUE;
    long numMessageGap = 0;
    double totalMessageGap = 0;

    public double avgMessageGap()
    {
      return totalMessageGap / numMessageGap;
    }

    public long getMinMessageGap()
    {
      return minMessageGap;
    }

    public long getMaxMessageGap()
    {
      return maxMessageGap;
    }

    public long getNumMessageGap()
    {
      return numMessageGap;
    }

    public MessageGapStats copyOf()
    {
      final MessageGapStats copy = new MessageGapStats();
      copy.minMessageGap = minMessageGap;
      copy.maxMessageGap = maxMessageGap;
      copy.numMessageGap = numMessageGap;
      copy.totalMessageGap = totalMessageGap;
      return copy;
    }
  }

  private final MessageGapStats messageGapStats = new MessageGapStats();
  private final ReentrantLock lock = new ReentrantLock();

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
    lock.lock();
    try {
      messageGapStats.totalMessageGap += messageGap;
      ++messageGapStats.numMessageGap;
      if (messageGapStats.minMessageGap > messageGap) {
        messageGapStats.minMessageGap = messageGap;
      }
      if (messageGapStats.maxMessageGap < messageGap) {
        messageGapStats.maxMessageGap = messageGap;
      }
    }
    finally {
      lock.unlock();
    }
  }

  public void reportMessageMaxTimestamp(long messageMaxTimestamp)
  {
    this.messageMaxTimestamp.getAndAccumulate(messageMaxTimestamp, Math::max);
  }

  public void reportMaxSegmentHandoffTime(long maxSegmentHandoffTime)
  {
    this.maxSegmentHandoffTime.getAndAccumulate(maxSegmentHandoffTime, Math::max);
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

  public MessageGapStats getMessageGapStats()
  {
    lock.lock();
    try {
      return messageGapStats.copyOf();
    }
    finally {
      lock.unlock();
    }
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

    lock.lock();
    try {
      retVal.messageGapStats.totalMessageGap = messageGapStats.totalMessageGap;
      retVal.messageGapStats.numMessageGap = messageGapStats.numMessageGap;
      retVal.messageGapStats.minMessageGap = messageGapStats.minMessageGap;
      retVal.messageGapStats.maxMessageGap = messageGapStats.maxMessageGap;

      messageGapStats.totalMessageGap = 0;
      messageGapStats.numMessageGap = 0;
      messageGapStats.minMessageGap = Long.MAX_VALUE;
      messageGapStats.maxMessageGap = Long.MIN_VALUE;
    }
    finally {
      lock.unlock();
    }

    reset();

    return retVal;
  }

  private void reset()
  {
    maxSegmentHandoffTime.set(NO_EMIT_SEGMENT_HANDOFF_TIME);
  }
}
