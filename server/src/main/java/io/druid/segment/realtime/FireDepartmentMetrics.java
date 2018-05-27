/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.realtime;

import com.google.common.base.Preconditions;

import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class FireDepartmentMetrics
{
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
  private final AtomicLong mergeTimeMillis = new AtomicLong(0);
  private final AtomicLong mergeCpuTime = new AtomicLong(0);
  private final AtomicLong persistCpuTime = new AtomicLong(0);
  private final AtomicLong handOffCount = new AtomicLong(0);
  private final AtomicLong sinkCount = new AtomicLong(0);
  private final AtomicLong messageMaxTimestamp = new AtomicLong(0);
  private final AtomicLong messageGap = new AtomicLong(0);

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

  public long messageMaxTimestamp()
  {
    return messageMaxTimestamp.get();
  }

  public long messageGap()
  {
    return messageGap.get();
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
    retVal.messageGap.set(System.currentTimeMillis() - messageMaxTimestamp.get());
    return retVal;
  }

  /**
   * merge other FireDepartmentMetrics, will modify this object's data
   *
   * @return this object
   */
  public FireDepartmentMetrics merge(FireDepartmentMetrics other)
  {
    Preconditions.checkNotNull(other, "Cannot merge a null FireDepartmentMetrics");
    FireDepartmentMetrics otherSnapshot = other.snapshot();
    processedCount.addAndGet(otherSnapshot.processed());
    processedWithErrorsCount.addAndGet(otherSnapshot.processedWithErrors());
    thrownAwayCount.addAndGet(otherSnapshot.thrownAway());
    rowOutputCount.addAndGet(otherSnapshot.rowOutput());
    unparseableCount.addAndGet(otherSnapshot.unparseable());
    dedupCount.addAndGet(otherSnapshot.dedup());
    numPersists.addAndGet(otherSnapshot.numPersists());
    persistTimeMillis.addAndGet(otherSnapshot.persistTimeMillis());
    persistBackPressureMillis.addAndGet(otherSnapshot.persistBackPressureMillis());
    failedPersists.addAndGet(otherSnapshot.failedPersists());
    failedHandoffs.addAndGet(otherSnapshot.failedHandoffs());
    mergeTimeMillis.addAndGet(otherSnapshot.mergeTimeMillis());
    mergeCpuTime.addAndGet(otherSnapshot.mergeCpuTime());
    persistCpuTime.addAndGet(otherSnapshot.persistCpuTime());
    handOffCount.addAndGet(otherSnapshot.handOffCount());
    sinkCount.addAndGet(otherSnapshot.sinkCount());
    messageMaxTimestamp.set(Math.max(messageMaxTimestamp(), otherSnapshot.messageMaxTimestamp()));
    messageGap.set(Math.max(messageGap(), otherSnapshot.messageGap()));
    return this;
  }
}
