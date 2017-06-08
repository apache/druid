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

import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class BufferGrouper<KeyType> extends AbstractBufferGrouper<KeyType>
{
  private static final Logger log = new Logger(BufferGrouper.class);
  private static final int MIN_INITIAL_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.7f;

  private ByteBuffer buffer;
  private boolean initialized = false;

  // Track the offsets of used buckets using this list.
  // When a new bucket is initialized by initializeNewBucketKey(), an offset is added to this list.
  // When expanding the table, the list is reset() and filled with the new offsets of the copied buckets.
  private ByteBuffer offsetListBuffer;
  private ByteBufferIntList offsetList;

  public BufferGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize,
      final float maxLoadFactor,
      final int initialBuckets
  )
  {
    super(bufferSupplier, keySerde, aggregatorFactories, bufferGrouperMaxSize);

    this.maxLoadFactor = maxLoadFactor > 0 ? maxLoadFactor : DEFAULT_MAX_LOAD_FACTOR;
    this.initialBuckets = initialBuckets > 0 ? Math.max(MIN_INITIAL_BUCKETS, initialBuckets) : DEFAULT_INITIAL_BUCKETS;

    if (this.maxLoadFactor >= 1.0f) {
      throw new IAE("Invalid maxLoadFactor[%f], must be < 1.0", maxLoadFactor);
    }

    int offset = HASH_SIZE + keySize;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }

    this.bucketSize = offset;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      this.buffer = bufferSupplier.get();

      int hashTableSize = ByteBufferHashTable.calculateTableArenaSizeWithPerBucketAdditionalSize(
          buffer.capacity(),
          bucketSize,
          Ints.BYTES
      );

      hashTableBuffer = buffer.duplicate();
      hashTableBuffer.position(0);
      hashTableBuffer.limit(hashTableSize);
      hashTableBuffer = hashTableBuffer.slice();

      offsetListBuffer = buffer.duplicate();
      offsetListBuffer.position(hashTableSize);
      offsetListBuffer.limit(buffer.capacity());
      offsetListBuffer = offsetListBuffer.slice();

      this.offsetList = new ByteBufferIntList(
          offsetListBuffer,
          offsetListBuffer.capacity() / Ints.BYTES
      );

      this.hashTable = new ByteBufferHashTable(
          maxLoadFactor,
          initialBuckets,
          bucketSize,
          hashTableBuffer,
          keySize,
          bufferGrouperMaxSize,
          new BufferGrouperBucketUpdateHandler()
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
  public void newBucketHook(int bucketOffset)
  {
  }

  @Override
  public boolean canSkipAggregate(boolean bucketWasUsed, int bucketOffset)
  {
    return false;
  }

  @Override
  public void afterAggregateHook(int bucketOffset)
  {

  }

  @Override
  public void reset()
  {
    offsetList.reset();
    hashTable.reset();
    keySerde.reset();
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(boolean sorted)
  {
    if (!initialized) {
      // it's possible for iterator() to be called before initialization when
      // a nested groupBy's subquery has an empty result set (see testEmptySubquery() in GroupByQueryRunnerTest)
      return Iterators.<Entry<KeyType>>emptyIterator();
    }

    if (sorted) {
      final List<Integer> wrappedOffsets = new AbstractList<Integer>()
      {
        @Override
        public Integer get(int index)
        {
          return offsetList.get(index);
        }

        @Override
        public Integer set(int index, Integer element)
        {
          final Integer oldValue = get(index);
          offsetList.set(index, element);
          return oldValue;
        }

        @Override
        public int size()
        {
          return hashTable.getSize();
        }
      };

      final BufferComparator comparator = keySerde.bufferComparator();

      // Sort offsets in-place.
      Collections.sort(
          wrappedOffsets,
          new Comparator<Integer>()
          {
            @Override
            public int compare(Integer lhs, Integer rhs)
            {
              final ByteBuffer tableBuffer = hashTable.getTableBuffer();
              return comparator.compare(
                  tableBuffer,
                  tableBuffer,
                  lhs + HASH_SIZE,
                  rhs + HASH_SIZE
              );
            }
          }
      );

      return new Iterator<Entry<KeyType>>()
      {
        int curr = 0;
        final int size = getSize();

        @Override
        public boolean hasNext()
        {
          return curr < size;
        }

        @Override
        public Entry<KeyType> next()
        {
          if (curr >= size) {
            throw new NoSuchElementException();
          }
          return bucketEntryForOffset(wrappedOffsets.get(curr++));
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException();
        }
      };
    } else {
      // Unsorted iterator
      return new Iterator<Entry<KeyType>>()
      {
        int curr = 0;
        final int size = getSize();

        @Override
        public boolean hasNext()
        {
          return curr < size;
        }

        @Override
        public Entry<KeyType> next()
        {
          if (curr >= size) {
            throw new NoSuchElementException();
          }
          final int offset = offsetList.get(curr);
          final Entry<KeyType> entry = bucketEntryForOffset(offset);
          curr++;

          return entry;
        }

        @Override
        public void remove()
        {
          throw new UnsupportedOperationException();
        }
      };
    }
  }

  private class BufferGrouperBucketUpdateHandler implements ByteBufferHashTable.BucketUpdateHandler
  {
    @Override
    public void handleNewBucket(int bucketOffset)
    {
      offsetList.add(bucketOffset);
    }

    @Override
    public void handlePreTableSwap()
    {
      offsetList.reset();
    }

    @Override
    public void handleBucketMove(
        int oldBucketOffset, int newBucketOffset, ByteBuffer oldBuffer, ByteBuffer newBuffer
    )
    {
      // relocate aggregators (see https://github.com/druid-io/druid/pull/4071)
      for (int i = 0; i < aggregators.length; i++) {
        aggregators[i].relocate(
            oldBucketOffset + aggregatorOffsets[i],
            newBucketOffset + aggregatorOffsets[i],
            oldBuffer,
            newBuffer
        );
      }

      offsetList.add(newBucketOffset);
    }
  }
}
