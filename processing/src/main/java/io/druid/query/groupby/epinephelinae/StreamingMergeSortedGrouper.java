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

package io.druid.query.groupby.epinephelinae;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.CloseableIterator;
import io.druid.query.QueryContexts;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A streaming grouper which can aggregate sorted inputs.  This grouper can aggregate while its iterator is being
 * consumed.  Also, the aggregation thread and iterating thread can be different.
 *
 * This grouper is backed by a circular array off-heap buffer.  Reading iterator is able to read from an array slot only
 * if the write for that slot is finished.
 */
public class StreamingMergeSortedGrouper<KeyType> implements Grouper<KeyType>
{
  private static final Logger LOG = new Logger(StreamingMergeSortedGrouper.class);
  private static final long DEFAULT_TIMEOUT_NS = 5_000_000_000L; // 5 seconds

  // Threashold time for spin locks in increaseWriteIndex() and increaseReadIndex(). Thread.yield() is called for the
  // waiting thread after this threadhold time.
  private static final long SPIN_FOR_TIMEOUT_THRESHOLD_NS = 1000L;

  private final Supplier<ByteBuffer> bufferSupplier;
  private final KeySerde<KeyType> keySerde;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int keySize;
  private final int recordSize; // size of (key + all aggregates)

  // Timeout for waiting for a slot to be available for read/write. This is required to prevent the processing
  // thread from being blocked if the iterator of this grouper is not consumed due to some failures.
  private final long queryTimeoutAtNs;

  // Below variables are initialized when init() is called
  private ByteBuffer buffer;
  private int maxNumSlots;
  private boolean initialized;

  /**
   * Indicate that this grouper consumed the last input or not.  Always set by the writing thread and read by the
   * reading thread.
   */
  private volatile boolean finished;

  /**
   * Current write position.  This is always moved ahead of nextReadIndex.
   * Also, it is always incremented by the writing thread and read by both the writing and the reading threads.
   */
  private volatile int curWriteIndex;

  /**
   * Next read position.  This can be moved to a position only when write for the position is finished.
   * Also, it is always incremented by the reading thread and read by both the writing and the reading threads.
   */
  private volatile int nextReadIndex;

  /**
   * Returns the minimum buffer capacity required to use this grouper.  This grouper keeps track read/write indexes
   * and they cannot point the same position at the same time.  Since the read/write indexes circularly, the required
   * minimum buffer capacity is 3 * record size.
   *
   * @return required minimum buffer capacity
   */
  public static <KeyType> int requiredBufferCapacity(
      KeySerde<KeyType> keySerde,
      AggregatorFactory[] aggregatorFactories
  )
  {
    int recordSize = keySerde.keySize();
    for (AggregatorFactory aggregatorFactory : aggregatorFactories) {
      recordSize += aggregatorFactory.getMaxIntermediateSize();
    }
    return recordSize * 3;
  }

  StreamingMergeSortedGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final long queryTimeoutAtMs
  )
  {
    this.bufferSupplier = bufferSupplier;
    this.keySerde = keySerde;
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];

    this.keySize = keySerde.keySize();
    int offset = keySize;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }
    this.recordSize = offset;
    final long timeoutNs = queryTimeoutAtMs == QueryContexts.NO_TIMEOUT ?
                           DEFAULT_TIMEOUT_NS :
                           TimeUnit.MILLISECONDS.toNanos(queryTimeoutAtMs - System.currentTimeMillis());

    this.queryTimeoutAtNs = System.nanoTime() + timeoutNs;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      buffer = bufferSupplier.get();
      maxNumSlots = buffer.capacity() / recordSize;
      Preconditions.checkState(
          maxNumSlots > 2,
          "Buffer[%s] should be large enough to store at least three records[%s]",
          buffer.capacity(),
          recordSize
      );

      reset();
      initialized = true;
    }
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public AggregateResult aggregate(KeyType key, int notUsed)
  {
    return aggregate(key);
  }

  @Override
  public AggregateResult aggregate(KeyType key)
  {
    try {
      final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);

      if (keyBuffer.remaining() != keySize) {
        throw new IAE(
            "keySerde.toByteBuffer(key).remaining[%s] != keySerde.keySize[%s], buffer was the wrong size?!",
            keyBuffer.remaining(),
            keySize
        );
      }

      final int prevRecordOffset = curWriteIndex * recordSize;
      if (curWriteIndex == -1 || !keyEquals(keyBuffer, buffer, prevRecordOffset)) {
        initNewSlot(keyBuffer);
      }

      final int curRecordOffset = curWriteIndex * recordSize;
      for (int i = 0; i < aggregatorOffsets.length; i++) {
        aggregators[i].aggregate(buffer, curRecordOffset + aggregatorOffsets[i]);
      }

      return AggregateResult.ok();
    }
    catch (RuntimeException e) {
      finished = true;
      throw e;
    }
  }

  private boolean keyEquals(ByteBuffer curKeyBuffer, ByteBuffer buffer, int bufferOffset)
  {
    int i = 0;
    for (; i + Long.BYTES <= keySize; i += Long.BYTES) {
      if (curKeyBuffer.getLong(i) != buffer.getLong(bufferOffset + i)) {
        return false;
      }
    }

    if (i + Integer.BYTES <= keySize) {
      if (curKeyBuffer.getInt(i) != buffer.getInt(bufferOffset + i)) {
        return false;
      }
      i += Integer.BYTES;
    }

    for (; i < keySize; i++) {
      if (curKeyBuffer.get(i) != buffer.get(bufferOffset + i)) {
        return false;
      }
    }

    return true;
  }

  private void initNewSlot(ByteBuffer newKey)
  {
    increaseWriteIndex();

    final int recordOffset = recordSize * curWriteIndex;
    buffer.position(recordOffset);
    buffer.put(newKey);

    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].init(buffer, recordOffset + aggregatorOffsets[i]);
    }
  }

  /**
   * Wait for {@link #nextReadIndex} to be moved if necessary and move {@link #curWriteIndex}.  {@link #nextReadIndex}
   * is checked in while loops instead of waiting using a lock to avoid frequent thread park.
   */
  private void increaseWriteIndex()
  {
    final long startAt = System.nanoTime();
    final long spinTimeoutAt = startAt + SPIN_FOR_TIMEOUT_THRESHOLD_NS;
    long now = startAt;

    if (curWriteIndex == maxNumSlots - 1) {
      // Should wait for the reading thread to start reading only if the writing thread should overwrite the first slot.
      while ((nextReadIndex == -1 || nextReadIndex == 0) && !Thread.currentThread().isInterrupted()) {
        if (now >= queryTimeoutAtNs) {
          throw new RuntimeException(new TimeoutException());
        }
        if (now >= spinTimeoutAt) {
          Thread.yield();
        }
        now = System.nanoTime();
      }
      curWriteIndex = 0;
    } else {
      final int nextWriteIndex = curWriteIndex + 1;
      while ((nextWriteIndex == nextReadIndex) && !Thread.currentThread().isInterrupted()) {
        if (now >= queryTimeoutAtNs) {
          throw new RuntimeException(new TimeoutException());
        }
        if (now >= spinTimeoutAt) {
          Thread.yield();
        }
        now = System.nanoTime();
      }
      curWriteIndex = nextWriteIndex;
    }
  }

  @Override
  public void reset()
  {
    curWriteIndex = -1;
    nextReadIndex = -1;
    finished = false;
  }

  @Override
  public void close()
  {
    for (BufferAggregator aggregator : aggregators) {
      try {
        aggregator.close();
      }
      catch (Exception e) {
        LOG.warn(e, "Could not close aggregator [%s], skipping.", aggregator);
      }
    }
  }

  /**
   * Signal that no more inputs are added.  Must be called after {@link #aggregate(Object)} is called for the last input.
   */
  public void finish()
  {
    increaseWriteIndex();
    finished = true;
  }

  /**
   * Return a sorted iterator.  This method can be called safely while writing and iterating thread and writing thread
   * can be different.  The result iterator always returns sorted results.  This method should be called only one time
   * per grouper.
   *
   * @return a sorted iterator
   */
  public CloseableIterator<Entry<KeyType>> iterator()
  {
    if (!initialized) {
      throw new ISE("Grouper should be initialized first");
    }

    return new CloseableIterator<Entry<KeyType>>()
    {
      {
        // Waits for some data to be ready
        increaseReadIndexTo(0);
      }

      @Override
      public boolean hasNext()
      {
        return !finished || remaining() > 0;
      }

      private int remaining()
      {
        if (curWriteIndex >= nextReadIndex) {
          return curWriteIndex - nextReadIndex;
        } else {
          return (maxNumSlots - nextReadIndex) + curWriteIndex;
        }
      }

      @Override
      public Entry<KeyType> next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        final int recordOffset = recordSize * nextReadIndex;
        final KeyType key = keySerde.fromByteBuffer(buffer, recordOffset);

        final Object[] values = new Object[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
          values[i] = aggregators[i].get(buffer, recordOffset + aggregatorOffsets[i]);
        }

        final int increaseTo = nextReadIndex == maxNumSlots - 1 ? 0 : nextReadIndex + 1;
        increaseReadIndexTo(increaseTo);

        return new Entry<>(key, values);
      }

      private void increaseReadIndexTo(int target)
      {
        final long startAt = System.nanoTime();
        final long spinTimeoutAt = startAt + SPIN_FOR_TIMEOUT_THRESHOLD_NS;
        long now = startAt;

        while ((!isReady() || target == curWriteIndex) && !finished && !Thread.currentThread().isInterrupted()) {
          if (now >= queryTimeoutAtNs) {
            throw new RuntimeException(new TimeoutException());
          }
          if (now >= spinTimeoutAt) {
            Thread.yield();
          }
          now = System.nanoTime();
        }

        nextReadIndex = target;
      }

      private boolean isReady()
      {
        return curWriteIndex != -1;
      }

      @Override
      public void close() throws IOException
      {
        // do nothing
      }
    };
  }

  /**
   * Return a sorted iterator.  This method can be called safely while writing and iterating thread and writing thread
   * can be different.  The result iterator always returns sorted results.  This method should be called only one time
   * per grouper.
   *
   * @param sorted not used
   *
   * @return a sorted iterator
   */
  @Override
  public CloseableIterator<Entry<KeyType>> iterator(boolean sorted)
  {
    return iterator();
  }
}
