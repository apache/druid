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
 */
public class FireDepartmentMetrics
{
  private static final long NO_EMIT_SEGMENT_HANDOFF_TIME = -1L;

  private static final long NO_EMIT_MESSAGE_GAP = -1L;

  private final AtomicLong processedCount = new AtomicLong(0);
  private final AtomicLong processedWithErrorsCount = new AtomicLong(0);
  private final AtomicLong thrownAwayCount = new AtomicLong(0);
  private final AtomicLong unparseableCount = new AtomicLong(0);
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

  public void incrementProcessed()
  {
    processedCount.incrementAndGet();
  }

  public void incrementProcessedWithErrors()
  {
    processedWithErrorsCount.incrementAndGet();
  }

  public void incrementThrownAway()
  {
    thrownAwayCount.incrementAndGet();
  }

  public void incrementDedup()
  {
    dedupCount.incrementAndGet();
  }

  public void incrementUnparseable()
  {
    unparseableCount.incrementAndGet();
  }

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

  public void incrementMergeTimeMillis(long millis)
  {
    mergeTimeMillis.addAndGet(millis);
  }

  public void incrementMergedRows(long rows)
  {
    mergedRows.addAndGet(rows);
  }

  public void incrementPushedRows(long rows)
  {
    pushedRows.addAndGet(rows);
  }

  public void incrementMergeCpuTime(long mergeTime)
  {
    mergeCpuTime.addAndGet(mergeTime);
  }

  public void incrementPersistCpuTime(long persistTime)
  {
    persistCpuTime.addAndGet(persistTime);
  }

  public void incrementHandOffCount()
  {
    handOffCount.incrementAndGet();
  }

  public void setSinkCount(long sinkCount)
  {
    this.sinkCount.set(sinkCount);
  }

  public void reportMessageMaxTimestamp(long messageMaxTimestamp)
  {
    this.messageMaxTimestamp.set(Math.max(messageMaxTimestamp, this.messageMaxTimestamp.get()));
  }

  public void reportMaxSegmentHandoffTime(long maxSegmentHandoffTime)
  {
    this.maxSegmentHandoffTime.set(Math.max(maxSegmentHandoffTime, this.maxSegmentHandoffTime.get()));
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

  public long processed()
  {
    return processedCount.get();
  }

  public long processedWithErrors()
  {
    return processedWithErrorsCount.get();
  }

  public long thrownAway()
  {
    return thrownAwayCount.get();
  }

  public long unparseable()
  {
    return unparseableCount.get();
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

  public long messageGap()
  {
    return messageGap.get();
  }

  public long maxSegmentHandoffTime()
  {
    return maxSegmentHandoffTime.get();
  }

  public FireDepartmentMetrics snapshot()
  {
    final FireDepartmentMetrics retVal = new FireDepartmentMetrics();
    retVal.processedCount.set(processedCount.get());
    retVal.processedWithErrorsCount.set(processedWithErrorsCount.get());
    retVal.thrownAwayCount.set(thrownAwayCount.get());
    retVal.unparseableCount.set(unparseableCount.get());
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
    retVal.messageMaxTimestamp.set(messageMaxTimestamp.get());
    retVal.maxSegmentHandoffTime.set(maxSegmentHandoffTime.get());
    retVal.mergedRows.set(mergedRows.get());
    retVal.pushedRows.set(pushedRows.get());

    long messageGapSnapshot = 0;
    final long maxTimestamp = retVal.messageMaxTimestamp.get();
    if (processingDone.get()) {
      messageGapSnapshot = NO_EMIT_MESSAGE_GAP;
    } else if (maxTimestamp > 0) {
      messageGapSnapshot = System.currentTimeMillis() - maxTimestamp;
    }
    retVal.messageGap.set(messageGapSnapshot);

    reset();

    return retVal;
  }

  private void reset()
  {
    maxSegmentHandoffTime.set(NO_EMIT_SEGMENT_HANDOFF_TIME);
  }
}
