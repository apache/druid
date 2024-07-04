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

import com.google.common.collect.EvictingQueue;

import javax.annotation.concurrent.ThreadSafe;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks the current segment loading rate for a single server.
 * <p>
 * The loading rate is computed as a moving average of the last
 * {@link #MOVING_AVERAGE_WINDOW_SIZE} progress updates (or more if any of the
 * updates was smaller than {@link #MIN_ENTRY_SIZE_BYTES}).
 */
@ThreadSafe
public class LoadingRateTracker
{
  public static final int MOVING_AVERAGE_WINDOW_SIZE = 10;
  public static final long MIN_ENTRY_SIZE_BYTES = 1_000_000_000;

  private final EvictingQueue<Entry> latestEntries = EvictingQueue.create(MOVING_AVERAGE_WINDOW_SIZE);

  private final Entry windowTotal = new Entry();
  private final AtomicReference<Entry> movingAverage = new AtomicReference<>();

  private Entry currentTail;

  public synchronized void updateProgress(long bytes, long millisElapsed)
  {
    if (bytes >= 0 && millisElapsed > 0) {
      windowTotal.increment(bytes, millisElapsed);

      final Entry evictedHead = addToTail(bytes, millisElapsed);
      if (evictedHead != null) {
        windowTotal.increment(-evictedHead.bytes, -evictedHead.millisElapsed);
      }

      if (windowTotal.bytes > 0 && windowTotal.millisElapsed > 0) {
        final Entry updatedMovingAverage = new Entry();
        updatedMovingAverage.increment(windowTotal.bytes, windowTotal.millisElapsed);
        movingAverage.set(updatedMovingAverage);
      }
    }
  }

  public synchronized void reset()
  {
    latestEntries.clear();
    movingAverage.set(null);
  }

  /**
   * Moving average load rate in kbps (kilobits per second).
   */
  public long getMovingAverageLoadRateKbps()
  {
    final Entry movingAverage = this.movingAverage.get();
    return movingAverage == null || movingAverage.millisElapsed <= 0
           ? 0
           : (8 * movingAverage.bytes) / movingAverage.millisElapsed;
  }

  /**
   * Moving average load rate in kBps (kilobytes per second).
   */
  public long getMovingAverageLoadRateKilobytesPerSecond()
  {
    final Entry movingAverage = this.movingAverage.get();
    return movingAverage == null || movingAverage.millisElapsed <= 0
           ? 0
           : movingAverage.bytes / movingAverage.millisElapsed;
  }

  /**
   * Adds the given value at the tail of the queue.
   *
   * @return Old head of the queue if it was evicted, null otherwise.
   */
  private synchronized Entry addToTail(long bytes, long millisElapsed)
  {
    final Entry oldHead = latestEntries.peek();

    if (currentTail == null) {
      currentTail = new Entry();
      latestEntries.add(currentTail);
    }

    currentTail.increment(bytes, millisElapsed);
    if (currentTail.bytes >= MIN_ENTRY_SIZE_BYTES) {
      currentTail = null;
    }

    // Compare if the oldHead and the newHead are the same object (not equals)
    final Entry newHead = latestEntries.peek();
    return newHead == oldHead ? null : oldHead;
  }

  private static class Entry
  {
    long bytes;
    long millisElapsed;

    void increment(long bytes, long millisElapsed)
    {
      this.bytes += bytes;
      this.millisElapsed += millisElapsed;
    }
  }
}
