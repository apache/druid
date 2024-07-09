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

package org.apache.druid.server.coordinator.loading;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.EvictingQueue;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Stopwatch;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks the current segment loading rate for a single server.
 * <p>
 * The loading rate is computed as a moving average of the last
 * {@link #MOVING_AVERAGE_WINDOW_SIZE} progress updates (or more if any of the
 * updates were smaller than {@link #MIN_ENTRY_SIZE_BYTES}).
 * <p>
 * This class is currently not required to be thread-safe as the caller
 * {@link HttpLoadQueuePeon} itself ensures that the write methods of this class
 * are only accessed by one thread at a time.
 */
@NotThreadSafe
public class LoadingRateTracker
{
  public static final int MOVING_AVERAGE_WINDOW_SIZE = 10;
  public static final long MIN_ENTRY_SIZE_BYTES = 1_000_000_000;

  private final EvictingQueue<Entry> window = EvictingQueue.create(MOVING_AVERAGE_WINDOW_SIZE);

  /**
   * Total stats for the whole window. This includes the total from the current batch as well.
   */
  private final AtomicReference<Entry> windowTotal = new AtomicReference<>(null);

  private final AtomicReference<Entry> currentBatchTotal = new AtomicReference<>(null);

  private Entry currentTail;

  private final Stopwatch currentBatchDuration = Stopwatch.createUnstarted();

  public void startBatch()
  {
    if (isTrackingBatch()) {
      // Do nothing
    } else {
      currentBatchDuration.restart();
      currentBatchTotal.set(null);
    }
  }

  public boolean isTrackingBatch()
  {
    return currentBatchDuration.isRunning();
  }

  public void updateBatchProgress(long bytes)
  {
    updateBatchProgress(bytes, currentBatchDuration.millisElapsed());
  }

  @VisibleForTesting
  void updateBatchProgress(final long bytes, final long batchDurationMillis)
  {
    if (!isTrackingBatch()) {
      throw DruidException.defensive("startBatch() must be called before tracking load progress.");
    }

    final Entry oldbatchTotal = currentBatchTotal.get();

    // Update the batch total. Increment the bytes and update the batch duration.
    final Entry updatedBatchTotal = new Entry();
    updatedBatchTotal.bytes = bytes + (oldbatchTotal == null ? 0 : oldbatchTotal.bytes);
    updatedBatchTotal.millisElapsed = batchDurationMillis;
    currentBatchTotal.set(updatedBatchTotal);

    // Update the overall window total. Subtract the old batch total, add the new batch total
    final Entry updatedWindowTotal = new Entry();
    updatedWindowTotal.incrementBy(windowTotal.get());
    updatedWindowTotal.incrementBy(updatedBatchTotal);

    if (oldbatchTotal != null) {
      updatedWindowTotal.bytes -= oldbatchTotal.bytes;
      updatedWindowTotal.millisElapsed -= oldbatchTotal.millisElapsed;
    }

    windowTotal.set(updatedWindowTotal);
  }

  public void completeBatch()
  {
    if (isTrackingBatch()) {
      currentBatchDuration.reset();
      addCurrentBatchToWindow();
    }
  }

  public void reset()
  {
    window.clear();
    windowTotal.set(null);
    currentBatchTotal.set(null);
  }

  /**
   * Moving average load rate in kbps (1000 bits per second).
   */
  public long getMovingAverageLoadRateKbps()
  {
    final Entry overallTotal = windowTotal.get();
    if (overallTotal == null || overallTotal.millisElapsed <= 0) {
      return 0;
    } else {
      return (8 * overallTotal.bytes) / overallTotal.millisElapsed;
    }
  }

  private void addCurrentBatchToWindow()
  {
    final Entry updatedWindowTotal = new Entry();
    updatedWindowTotal.incrementBy(windowTotal.get());

    final Entry evictedHead = addToTail(currentBatchTotal.get());
    if (evictedHead != null) {
      updatedWindowTotal.bytes -= evictedHead.bytes;
      updatedWindowTotal.millisElapsed -= evictedHead.millisElapsed;
    }

    windowTotal.set(updatedWindowTotal);
    currentBatchTotal.set(null);
  }

  /**
   * Adds the given value at the tail of the queue.
   *
   * @return Old head of the queue if it was evicted, null otherwise.
   */
  private Entry addToTail(Entry value)
  {
    final Entry oldHead = window.peek();

    if (currentTail == null) {
      currentTail = new Entry();
      window.add(currentTail);
    }

    currentTail.incrementBy(value);
    if (currentTail.bytes >= MIN_ENTRY_SIZE_BYTES) {
      currentTail = null;
    }

    // Compare if the oldHead and the newHead are the same object (not equals)
    final Entry newHead = window.peek();
    return newHead == oldHead ? null : oldHead;
  }

  private static class Entry
  {
    long bytes;
    long millisElapsed;

    void incrementBy(Entry other)
    {
      if (other != null) {
        this.bytes += other.bytes;
        this.millisElapsed += other.millisElapsed;
      }
    }
  }
}
