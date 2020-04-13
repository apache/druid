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

package org.apache.druid.query.groupby.epinephelinae;

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.NoSuchElementException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A streaming grouper which can aggregate sorted inputs.  This grouper can aggregate while its iterator is being
 * consumed.  The aggregation thread and the iterating thread can be different.
 *
 * This grouper is backed by an off-heap circular array.  The reading thread is able to read data from an array slot
 * only when aggregation for the grouping key correspoing to that slot is finished.  Since the reading and writing
 * threads cannot access the same array slot at the same time, they can read/write data without contention.
 *
 * This class uses the spinlock for waiting for at least one slot to become available when the array is empty or full.
 * If the array is empty, the reading thread waits for the aggregation for an array slot is finished.  If the array is
 * full, the writing thread waits for the reading thread to read at least one aggregate from the array.
 */
public class StreamingMergeSortedGrouper<KeyType> implements Grouper<KeyType>
{
  private static final Logger LOG = new Logger(StreamingMergeSortedGrouper.class);
  private static final long DEFAULT_TIMEOUT_NS = TimeUnit.SECONDS.toNanos(5); // default timeout for spinlock

  // Threashold time for spinlocks in increaseWriteIndex() and increaseReadIndex(). The waiting thread calls
  // Thread.yield() after this threadhold time elapses.
  private static final long SPIN_FOR_TIMEOUT_THRESHOLD_NS = 1000L;

  private final Supplier<ByteBuffer> bufferSupplier;
  private final KeySerde<KeyType> keySerde;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int keySize;
  private final int recordSize; // size of (key + all aggregates)

  // Timeout for the current query.
  // The query must fail with a timeout exception if System.nanoTime() >= queryTimeoutAtNs. This is used in the
  // spinlocks to prevent the writing thread from being blocked if the iterator of this grouper is not consumed due to
  // some failures which potentially makes the whole system being paused.
  private final long queryTimeoutAtNs;
  private final boolean hasQueryTimeout;

  // Below variables are initialized when init() is called.
  private ByteBuffer buffer;
  private int maxNumSlots;
  private boolean initialized;

  /**
   * Indicate that this grouper consumed the last input or not.  The writing thread must set this value to true by
   * calling {@link #finish()} when it's done.  This variable is always set by the writing thread and read by the
   * reading thread.
   */
  private volatile boolean finished;

  /**
   * Current write index of the array.  This points to the array slot where the aggregation is currently performed.  Its
   * initial value is -1 which means any data are not written yet.  Since it's assumed that the input is sorted by the
   * grouping key, this variable is moved to the next slot whenever a new grouping key is found.  Once it reaches the
   * last slot of the array, it moves to the first slot.
   *
   * This is always moved ahead of {@link #nextReadIndex}.  If the array is full, this variable
   * cannot be moved until {@link #nextReadIndex} is moved.  See {@link #increaseWriteIndex()} for more details. This
   * variable is always incremented by the writing thread and read by both the writing and the reading threads.
   */
  private volatile int curWriteIndex;

  /**
   * Next read index of the array.  This points to the array slot which the reading thread will read next.  Its initial
   * value is -1 which means any data are not read yet.  This variable can point an array slot only when the aggregation
   * for that slot is finished.  Once it reaches the last slot of the array, it moves to the first slot.
   *
   * This always follows {@link #curWriteIndex}.  If the array is empty, this variable cannot be moved until the
   * aggregation for at least one grouping key is finished which in turn {@link #curWriteIndex} is moved.  See
   * {@link #iterator()} for more details.  This variable is always incremented by the reading thread and read by both
   * the writing and the reading threads.
   */
  private volatile int nextReadIndex;

  /**
   * Returns the minimum buffer capacity required for this grouper.  This grouper keeps track read/write indexes
   * and they cannot point the same array slot at the same time.  Since the read/write indexes move circularly, one
   * extra slot is needed in addition to the read/write slots.  Finally, the required minimum buffer capacity is
   * 3 * record size.
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
      recordSize += aggregatorFactory.getMaxIntermediateSizeWithNulls();
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
      offset += aggregatorFactories[i].getMaxIntermediateSizeWithNulls();
    }
    this.recordSize = offset;

    // queryTimeoutAtMs comes from System.currentTimeMillis(), but we should use System.nanoTime() to check timeout in
    // this class. See increaseWriteIndex() and increaseReadIndex().
    this.hasQueryTimeout = queryTimeoutAtMs != QueryContexts.NO_TIMEOUT;
    final long timeoutNs = hasQueryTimeout ?
                           TimeUnit.MILLISECONDS.toNanos(queryTimeoutAtMs - System.currentTimeMillis()) :
                           QueryContexts.NO_TIMEOUT;

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
        // Initialize a new slot for the new key. This may be potentially blocked if the array is full until at least
        // one slot becomes available.
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

  /**
   * Checks two keys contained in the given buffers are same.
   *
   * @param curKeyBuffer the buffer for the given key from {@link #aggregate(Object)}
   * @param buffer       the whole array buffer
   * @param bufferOffset the key offset of the buffer
   *
   * @return true if the two buffers are same.
   */
  private boolean keyEquals(ByteBuffer curKeyBuffer, ByteBuffer buffer, int bufferOffset)
  {
    // Since this method is frequently called per each input row, the compare performance matters.
    int i = 0;
    for (; i + Long.BYTES <= keySize; i += Long.BYTES) {
      if (curKeyBuffer.getLong(i) != buffer.getLong(bufferOffset + i)) {
        return false;
      }
    }

    if (i + Integer.BYTES <= keySize) {
      // This can be called at most once because we already compared using getLong() in the above.
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

  /**
   * Initialize a new slot for a new grouping key.  This may be potentially blocked if the array is full until at least
   * one slot becomes available.
   */
  private void initNewSlot(ByteBuffer newKey)
  {
    // Wait if the array is full and increase curWriteIndex
    increaseWriteIndex();

    final int recordOffset = recordSize * curWriteIndex;
    buffer.position(recordOffset);
    buffer.put(newKey);

    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].init(buffer, recordOffset + aggregatorOffsets[i]);
    }
  }

  /**
   * Wait for {@link #nextReadIndex} to be moved if necessary and move {@link #curWriteIndex}.
   */
  private void increaseWriteIndex()
  {
    final long startAtNs = System.nanoTime();
    final long queryTimeoutAtNs = getQueryTimeoutAtNs(startAtNs);
    final long spinTimeoutAtNs = startAtNs + SPIN_FOR_TIMEOUT_THRESHOLD_NS;
    long timeoutNs = queryTimeoutAtNs - startAtNs;
    long spinTimeoutNs = SPIN_FOR_TIMEOUT_THRESHOLD_NS;

    // In the below, we check that the array is full and wait for at least one slot to become available.
    //
    // nextReadIndex is a volatile variable and the changes on it are continuously checked until they are seen in
    // the while loop. See the following links.
    // * http://docs.oracle.com/javase/specs/jls/se7/html/jls-8.html#jls-8.3.1.4
    // * http://docs.oracle.com/javase/specs/jls/se7/html/jls-17.html#jls-17.4.5
    // * https://stackoverflow.com/questions/11761552/detailed-semantics-of-volatile-regarding-timeliness-of-visibility

    if (curWriteIndex == maxNumSlots - 1) {
      // We additionally check that nextReadIndex is -1 here because the writing thread should wait for the reading
      // thread to start reading only when the writing thread tries to overwrite the first slot for the first time.

      // The below condition is checked in a while loop instead of using a lock to avoid frequent thread park.
      while ((nextReadIndex == -1 || nextReadIndex == 0) && !Thread.currentThread().isInterrupted()) {
        if (timeoutNs <= 0L) {
          throw new RuntimeException(new TimeoutException());
        }
        // Thread.yield() should not be called from the very beginning
        if (spinTimeoutNs <= 0L) {
          Thread.yield();
        }
        long now = System.nanoTime();
        timeoutNs = queryTimeoutAtNs - now;
        spinTimeoutNs = spinTimeoutAtNs - now;
      }

      // Changes on nextReadIndex happens-before changing curWriteIndex.
      curWriteIndex = 0;
    } else {
      final int nextWriteIndex = curWriteIndex + 1;

      // The below condition is checked in a while loop instead of using a lock to avoid frequent thread park.
      while ((nextWriteIndex == nextReadIndex) && !Thread.currentThread().isInterrupted()) {
        if (timeoutNs <= 0L) {
          throw new RuntimeException(new TimeoutException());
        }
        // Thread.yield() should not be called from the very beginning
        if (spinTimeoutNs <= 0L) {
          Thread.yield();
        }
        long now = System.nanoTime();
        timeoutNs = queryTimeoutAtNs - now;
        spinTimeoutNs = spinTimeoutAtNs - now;
      }

      // Changes on nextReadIndex happens-before changing curWriteIndex.
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
    // Once finished is set, curWriteIndex must not be changed. This guarantees that the remaining number of items in
    // the array is always decreased as the reading thread proceeds. See hasNext() and remaining() below.
    finished = true;
  }

  /**
   * Return a sorted iterator.  This method can be called safely while writing, and the iterating thread and the writing
   * thread can be different.  The result iterator always returns sorted results.  This method should be called only one
   * time per grouper.
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
        // Wait for some data to be ready and initialize nextReadIndex.
        increaseReadIndexTo(0);
      }

      @Override
      public boolean hasNext()
      {
        // If setting finished happens-before the below check, curWriteIndex isn't changed anymore and thus remainig()
        // can be computed safely because nextReadIndex is changed only by the reading thread.
        // Otherwise, hasNext() always returns true.
        //
        // The below line can be executed between increasing curWriteIndex and setting finished in
        // StreamingMergeSortedGrouper.finish(), but it is also a valid case because there should be at least one slot
        // which is not read yet before finished is set.
        return !finished || remaining() > 0;
      }

      /**
       * Calculate the number of remaining items in the array.  Must be called only when
       * {@link StreamingMergeSortedGrouper#finished} is true.
       *
       * @return the number of remaining items
       */
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

        // Here, nextReadIndex should be valid which means:
        // - a valid array index which should be >= 0 and < maxNumSlots
        // - an index of the array slot where the aggregation for the corresponding grouping key is done
        // - an index of the array slot which is not read yet
        final int recordOffset = recordSize * nextReadIndex;
        final KeyType key = keySerde.fromByteBuffer(buffer, recordOffset);

        final Object[] values = new Object[aggregators.length];
        for (int i = 0; i < aggregators.length; i++) {
          values[i] = aggregators[i].get(buffer, recordOffset + aggregatorOffsets[i]);
        }

        final int targetIndex = nextReadIndex == maxNumSlots - 1 ? 0 : nextReadIndex + 1;
        // Wait if the array is empty until at least one slot becomes available for read, and then increase
        // nextReadIndex.
        increaseReadIndexTo(targetIndex);

        return new Entry<>(key, values);
      }

      /**
       * Wait for {@link StreamingMergeSortedGrouper#curWriteIndex} to be moved if necessary and move
       * {@link StreamingMergeSortedGrouper#nextReadIndex}.
       *
       * @param target the target index {@link StreamingMergeSortedGrouper#nextReadIndex} will move to
       */
      private void increaseReadIndexTo(int target)
      {
        // Check that the array is empty and wait for at least one slot to become available.
        //
        // curWriteIndex is a volatile variable and the changes on it are continuously checked until they are seen in
        // the while loop. See the following links.
        // * http://docs.oracle.com/javase/specs/jls/se7/html/jls-8.html#jls-8.3.1.4
        // * http://docs.oracle.com/javase/specs/jls/se7/html/jls-17.html#jls-17.4.5
        // * https://stackoverflow.com/questions/11761552/detailed-semantics-of-volatile-regarding-timeliness-of-visibility

        final long startAtNs = System.nanoTime();
        final long queryTimeoutAtNs = getQueryTimeoutAtNs(startAtNs);
        final long spinTimeoutAtNs = startAtNs + SPIN_FOR_TIMEOUT_THRESHOLD_NS;
        long timeoutNs = queryTimeoutAtNs - startAtNs;
        long spinTimeoutNs = SPIN_FOR_TIMEOUT_THRESHOLD_NS;

        // The below condition is checked in a while loop instead of using a lock to avoid frequent thread park.
        while ((curWriteIndex == -1 || target == curWriteIndex) &&
               !finished && !Thread.currentThread().isInterrupted()) {
          if (timeoutNs <= 0L) {
            throw new RuntimeException(new TimeoutException());
          }
          // Thread.yield() should not be called from the very beginning
          if (spinTimeoutNs <= 0L) {
            Thread.yield();
          }
          long now = System.nanoTime();
          timeoutNs = queryTimeoutAtNs - now;
          spinTimeoutNs = spinTimeoutAtNs - now;
        }

        // Changes on curWriteIndex happens-before changing nextReadIndex.
        nextReadIndex = target;
      }

      @Override
      public void close()
      {
        // do nothing
      }
    };
  }

  private long getQueryTimeoutAtNs(long startAtNs)
  {
    return hasQueryTimeout ? queryTimeoutAtNs : startAtNs + DEFAULT_TIMEOUT_NS;
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
