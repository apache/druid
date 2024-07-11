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

import javax.annotation.concurrent.NotThreadSafe;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Tracks the current segment loading rate for a single server.
 * <p>
 * The loading rate is computed as a moving average of the last
 * {@link #MOVING_AVERAGE_WINDOW_SIZE} successful segment loads (or more if any
 * of the segments were smaller than {@link #MIN_ENTRY_SIZE_BYTES}). Since the
 * milliseconds taken to load individual segments are simply added up, the computed
 * rate might be incorrect if the server uses multiple loading threads.
 * <p>
 * This class is currently not required to be thread-safe as the caller
 * {@link HttpLoadQueuePeon} itself ensures that the write methods of this class
 * are only accessed by one thread at a time.
 */
@NotThreadSafe
public class LoadingRateTracker
{
  public static final int MOVING_AVERAGE_WINDOW_SIZE = 10;
  public static final long MIN_ENTRY_SIZE_BYTES = 1 << 30;

  private final EvictingQueue<Entry> window = EvictingQueue.create(MOVING_AVERAGE_WINDOW_SIZE);
  private final AtomicReference<Entry> windowTotal = new AtomicReference<>(null);

  private Entry currentTail;

  /**
   * Adds the given number of bytes to the moving average window.
   */
  public void add(final long bytes, final long loadTimeMillis)
  {
    // Add a fresh entry at the tail if it is full
    final Entry evictedHead = addNewEntryIfTailIsFull();

    final Entry delta = new Entry();
    delta.bytes = bytes;
    delta.millisElapsed = loadTimeMillis;

    currentTail.incrementBy(delta);

    // Update the window total
    if (evictedHead != null) {
      delta.bytes -= evictedHead.bytes;
      delta.millisElapsed -= evictedHead.millisElapsed;
    }
    windowTotal.updateAndGet(delta::incrementBy);
  }

  public void reset()
  {
    window.clear();
    windowTotal.set(null);
    currentTail = null;
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

  /**
   * Adds a fresh entry to the queue if the current tail entry is already full.
   *
   * @return Old head of the queue if it was evicted, null otherwise.
   */
  private Entry addNewEntryIfTailIsFull()
  {
    final Entry oldHead = window.peek();

    if (currentTail == null || currentTail.bytes >= MIN_ENTRY_SIZE_BYTES) {
      currentTail = new Entry();
      window.add(currentTail);
    }

    // Compare if the oldHead and the newHead are the same object (not equals)
    final Entry newHead = window.peek();
    return newHead == oldHead ? null : oldHead;
  }

  private static class Entry
  {
    long bytes;
    long millisElapsed;

    Entry incrementBy(Entry delta)
    {
      if (delta != null) {
        this.bytes += delta.bytes;
        this.millisElapsed += delta.millisElapsed;
      }
      return this;
    }
  }
}
