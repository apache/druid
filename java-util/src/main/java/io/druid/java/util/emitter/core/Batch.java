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

package io.druid.java.util.emitter.core;

import com.google.common.base.Preconditions;
import io.druid.java.util.common.logger.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

/**
 * Buffer for batched data + synchronization state.
 * <p>
 * The state structure ({@link AbstractQueuedLongSynchronizer#state}):
 * Bits 0-30 - bufferWatermark
 * Bit 31 - always 0
 * Bits 32-62 - "parties" (the number of concurrent writers)
 * Bit 63 - sealed flag
 * <p>
 * Writer threads (callers of {@link HttpPostEmitter#emit(Event)}) are eligible to come, increment bufferWatermark and
 * write data into the buffer, as long as sealed flag is false.
 * <p>
 * {@link HttpPostEmitter#emittingThread} is eligible to emit the buffer, when sealed flag=true and parties=0 (all
 * writes are completed). See {@link #isEmittingAllowed(long)}.
 * <p>
 * In this class, "lock" means "increment number of parties by 1", i. e. lock the emitter thread from emitting this
 * batch. "Unlock" means "decrement number of parties by 1".
 */
class Batch extends AbstractQueuedLongSynchronizer
{
  private static final Logger log = new Logger(Batch.class);

  private static final long PARTY = 1L << 32;
  private static final long SEAL_BIT = 1L << 63;

  private static int bufferWatermark(long state)
  {
    return (int) state;
  }

  private static int parties(long state)
  {
    return ((int) (state >>> 32)) & Integer.MAX_VALUE;
  }

  private static boolean isSealed(long state)
  {
    // The highest bit is 1.
    return state < 0;
  }

  private static boolean isEmittingAllowed(long state)
  {
    return isSealed(state) && parties(state) == 0;
  }

  /**
   * Tags (arg values) to transmit request from {@link #releaseShared(long)} to {@link #tryReleaseShared(long)}.
   */
  private static final long UNLOCK_TAG = 0;
  private static final long UNLOCK_AND_SEAL_TAG = 1;
  private static final long SEAL_TAG = 2;

  /**
   * The emitter this batch belongs to.
   */
  private final HttpPostEmitter emitter;

  /**
   * The data buffer of the batch.
   */
  final byte[] buffer;

  /**
   * Ordering number of this batch, as they filled & emitted in {@link HttpPostEmitter} serially, starting from 0.
   * It's a boxed Long rather than primitive long, because we want to minimize the number of allocations done in
   * {@link HttpPostEmitter#onSealExclusive} and so the probability of {@link OutOfMemoryError}.
   * @see HttpPostEmitter#onSealExclusive
   * @see HttpPostEmitter#concurrentBatch
   */
  final Long batchNumber;

  /**
   * The number of events in this batch, needed for event count-based batch emitting.
   */
  final AtomicInteger eventCount = new AtomicInteger(0);

  /**
   * The time when the first event was written into this batch, needed for timeout-based batch emitting.
   */
  private long firstEventTimestamp = -1;

  Batch(HttpPostEmitter emitter, byte[] buffer, long batchNumber)
  {
    this.emitter = emitter;
    this.buffer = buffer;
    this.batchNumber = batchNumber;
  }

  int getSealedBufferWatermark()
  {
    long state = getState();
    Preconditions.checkState(isSealed(state));
    return bufferWatermark(state);
  }

  /**
   * Tries to add (write) event to the batch, returns true, if successful. If fails, no subsequent attempts to add event
   * to this batch will succeed, the next batch should be taken.
   */
  boolean tryAddEvent(byte[] event)
  {
    while (true) {
      long state = getState();
      if (isSealed(state)) {
        return false;
      }
      int bufferWatermark = bufferWatermark(state);
      if (bufferWatermark == 0) {
        if (tryAddFirstEvent(event)) {
          return true;
        }
      } else if (newBufferWatermark(bufferWatermark, event) <= emitter.maxBufferWatermark) {
        if (tryAddNonFirstEvent(state, event)) {
          return true;
        }
      } else {
        seal();
        return false;
      }
    }
  }

  private boolean tryAddFirstEvent(byte[] event)
  {
    if (!tryReserveFirstEventSizeAndLock(event)) {
      return false;
    }
    try {
      int bufferOffset = emitter.batchingStrategy.writeBatchStart(buffer);
      writeEvent(event, bufferOffset);
      eventCount.incrementAndGet();
      firstEventTimestamp = System.currentTimeMillis();
      return true;
    }
    finally {
      unlock();
    }
  }

  private boolean tryReserveFirstEventSizeAndLock(byte[] event)
  {
    return compareAndSetState(0, emitter.batchingStrategy.batchStartLength() + event.length + PARTY);
  }

  private int newBufferWatermark(int bufferWatermark, byte[] eventBytes)
  {
    return bufferWatermark + emitter.batchingStrategy.separatorLength() + eventBytes.length;
  }

  private boolean tryAddNonFirstEvent(long state, byte[] event)
  {
    int bufferOffset = tryReserveEventSizeAndLock(state, emitter.batchingStrategy.separatorLength() + event.length);
    if (bufferOffset < 0) {
      return false;
    }
    try {
      bufferOffset = emitter.batchingStrategy.writeMessageSeparator(buffer, bufferOffset);
      writeEvent(event, bufferOffset);
      return true;
    }
    finally {
      unlockAndSealIfNeeded();
    }
  }

  /**
   * Returns the buffer offset at which the caller has reserved the ability to write `size` bytes exclusively,
   * or negative number, if the reservation attempt failed.
   */
  private int tryReserveEventSizeAndLock(long state, int size)
  {
    Preconditions.checkArgument(size > 0);
    int bufferWatermark = bufferWatermark(state);
    while (true) {
      if (compareAndSetState(state, state + size + PARTY)) {
        return bufferWatermark;
      }
      state = getState();
      if (isSealed(state)) {
        return -1;
      }
      bufferWatermark = bufferWatermark(state);
      int newBufferWatermark = bufferWatermark + size;
      Preconditions.checkState(newBufferWatermark > 0);
      if (newBufferWatermark > emitter.maxBufferWatermark) {
        return -1;
      }
    }
  }

  private void unlockAndSealIfNeeded()
  {
    if (eventCount.incrementAndGet() >= emitter.config.getFlushCount()) {
      unlockAndSeal();
    } else {
      long timeSinceFirstEvent = System.currentTimeMillis() - firstEventTimestamp;
      if (firstEventTimestamp > 0 && timeSinceFirstEvent > emitter.config.getFlushMillis()) {
        unlockAndSeal();
      } else {
        unlock();
      }
    }
  }

  void sealIfFlushNeeded()
  {
    long timeSinceFirstEvent = System.currentTimeMillis() - firstEventTimestamp;
    if (firstEventTimestamp > 0 && timeSinceFirstEvent > emitter.config.getFlushMillis()) {
      seal();
    }
  }

  private void writeEvent(byte[] event, int bufferOffset)
  {
    System.arraycopy(event, 0, buffer, bufferOffset, event.length);
  }


  private void unlock()
  {
    releaseShared(UNLOCK_TAG);
  }

  private void unlockAndSeal()
  {
    releaseShared(UNLOCK_AND_SEAL_TAG);
  }

  void seal()
  {
    releaseShared(SEAL_TAG);
  }

  @Override
  protected boolean tryReleaseShared(long tag)
  {
    if (tag == UNLOCK_TAG) {
      while (true) {
        long state = getState();
        int parties = parties(state);
        if (parties == 0) {
          throw new IllegalMonitorStateException();
        }
        long newState = state - PARTY;
        if (compareAndSetState(state, newState)) {
          return isEmittingAllowed(newState);
        }
      }
    } else if (tag == UNLOCK_AND_SEAL_TAG) {
      while (true) {
        long state = getState();
        int parties = parties(state);
        if (parties == 0) {
          throw new IllegalMonitorStateException();
        }
        long newState = (state - PARTY) | SEAL_BIT;
        if (compareAndSetState(state, newState)) {
          // Ensures only one thread calls emitter.onSealExclusive() for each batch.
          if (!isSealed(state)) {
            log.debug("Unlocked and sealed batch [%d]", batchNumber);
            debugLogState("old state", state);
            debugLogState("new state", newState);
            emitter.onSealExclusive(
                this,
                firstEventTimestamp > 0 ? System.currentTimeMillis() - firstEventTimestamp : -1
            );
          }
          return isEmittingAllowed(newState);
        }
      }
    } else if (tag == SEAL_TAG) {
      while (true) {
        long state = getState();
        if (isSealed(state)) {
          // Returning false, despite acquisition could be possible now, because this thread actually didn't update the
          // state, i. e. didn't "release" in AbstractQueuedLongSynchronizer's terms.
          return false;
        }
        long newState = state | SEAL_BIT;
        if (compareAndSetState(state, newState)) {
          log.debug("Sealed batch [%d]", batchNumber);
          debugLogState("old state", state);
          debugLogState("new state", newState);
          emitter.onSealExclusive(
              this,
              firstEventTimestamp > 0 ? System.currentTimeMillis() - firstEventTimestamp : -1
          );
          return isEmittingAllowed(newState);
        }
      }
    } else {
      throw new IllegalStateException("Unknown tag: " + tag);
    }
  }

  void awaitEmittingAllowed()
  {
    acquireShared(1);
  }

  @Override
  protected long tryAcquireShared(long ignored)
  {
    return isEmittingAllowed(getState()) ? 1 : -1;
  }

  @Override
  public String toString()
  {
    long state = getState();
    return "Batch{" +
           "batchNumber=" + batchNumber +
           ", bufferWatermark=" + bufferWatermark(state) +
           ", parties=" + parties(state) +
           ", isSealed=" + isSealed(state) +
           "}";
  }

  private static void debugLogState(String name, long state)
  {
    if (log.isDebugEnabled()) {
      log.debug(
          "%s[bufferWatermark=%d, parties=%d, isSealed=%s]",
          name,
          bufferWatermark(state),
          parties(state),
          isSealed(state)
      );
    }
  }
}
