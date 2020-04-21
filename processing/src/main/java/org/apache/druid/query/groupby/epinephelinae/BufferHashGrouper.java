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

import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.CloseableIterators;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.ToIntFunction;

public class BufferHashGrouper<KeyType> extends AbstractBufferHashGrouper<KeyType>
{
  private static final int MIN_INITIAL_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.7f;

  private boolean initialized = false;

  // The BufferHashGrouper normally sorts by all fields of the grouping key with lexicographic ascending order.
  // However, when a query will have the limit push down optimization applied (see LimitedBufferHashGrouper),
  // the optimization may not be applied on some nodes because of buffer capacity limits. In this case,
  // those nodes will use BufferHashGrouper instead of LimitedBufferHashGrouper. In this mixed use case,
  // nodes using BufferHashGrouper need to use the same sorting order as nodes using LimitedBufferHashGrouper, so that
  // results are merged properly. When useDefaultSorting is false, we call keySerde.bufferComparatorWithAggregators()
  // to get a comparator that uses the ordering defined by the OrderByColumnSpec of a query.
  private final boolean useDefaultSorting;

  @Nullable
  private ByteBufferIntList offsetList;

  public BufferHashGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final AggregatorAdapters aggregators,
      final int bufferGrouperMaxSize,
      final float maxLoadFactor,
      final int initialBuckets,
      final boolean useDefaultSorting
  )
  {
    super(bufferSupplier, keySerde, aggregators, HASH_SIZE + keySerde.keySize(), bufferGrouperMaxSize);

    this.maxLoadFactor = maxLoadFactor > 0 ? maxLoadFactor : DEFAULT_MAX_LOAD_FACTOR;
    this.initialBuckets = initialBuckets > 0 ? Math.max(MIN_INITIAL_BUCKETS, initialBuckets) : DEFAULT_INITIAL_BUCKETS;

    if (this.maxLoadFactor >= 1.0f) {
      throw new IAE("Invalid maxLoadFactor[%f], must be < 1.0", maxLoadFactor);
    }

    this.bucketSize = HASH_SIZE + keySerde.keySize() + aggregators.spaceNeeded();
    this.useDefaultSorting = useDefaultSorting;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      ByteBuffer buffer = bufferSupplier.get();

      int hashTableSize = ByteBufferHashTable.calculateTableArenaSizeWithPerBucketAdditionalSize(
          buffer.capacity(),
          bucketSize,
          Integer.BYTES
      );

      hashTableBuffer = buffer.duplicate();
      hashTableBuffer.position(0);
      hashTableBuffer.limit(hashTableSize);
      hashTableBuffer = hashTableBuffer.slice();

      // Track the offsets of used buckets using this list.
      // When a new bucket is initialized by initializeNewBucketKey(), an offset is added to this list.
      // When expanding the table, the list is reset() and filled with the new offsets of the copied buckets.
      ByteBuffer offsetListBuffer = buffer.duplicate();
      offsetListBuffer.position(hashTableSize);
      offsetListBuffer.limit(buffer.capacity());
      offsetListBuffer = offsetListBuffer.slice();

      this.offsetList = new ByteBufferIntList(
          offsetListBuffer,
          offsetListBuffer.capacity() / Integer.BYTES
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
  public ToIntFunction<KeyType> hashFunction()
  {
    return Groupers::hashObject;
  }

  @Override
  public void newBucketHook(int bucketOffset)
  {
    // Nothing needed.
  }

  @Override
  public boolean canSkipAggregate(int bucketOffset)
  {
    return false;
  }

  @Override
  public void afterAggregateHook(int bucketOffset)
  {
    // Nothing needed.
  }

  @Override
  public void reset()
  {
    offsetList.reset();
    hashTable.reset();
    keySerde.reset();
  }

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(boolean sorted)
  {
    if (!initialized) {
      // it's possible for iterator() to be called before initialization when
      // a nested groupBy's subquery has an empty result set (see testEmptySubquery() in GroupByQueryRunnerTest)
      return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
    }

    if (sorted) {
      @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
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

      final BufferComparator comparator;
      if (useDefaultSorting) {
        comparator = keySerde.bufferComparator();
      } else {
        comparator = keySerde.bufferComparatorWithAggregators(
            aggregators.factories().toArray(new AggregatorFactory[0]),
            aggregators.aggregatorPositions()
        );
      }

      // Sort offsets in-place.
      Collections.sort(
          wrappedOffsets,
          (lhs, rhs) -> {
            final ByteBuffer tableBuffer = hashTable.getTableBuffer();
            return comparator.compare(
                tableBuffer,
                tableBuffer,
                lhs + HASH_SIZE,
                rhs + HASH_SIZE
            );
          }
      );

      return new CloseableIterator<Entry<KeyType>>()
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

        @Override
        public void close()
        {
          // do nothing
        }
      };
    } else {
      // Unsorted iterator
      return new CloseableIterator<Entry<KeyType>>()
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

        @Override
        public void close()
        {
          // do nothing
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
    public void handleBucketMove(int oldBucketOffset, int newBucketOffset, ByteBuffer oldBuffer, ByteBuffer newBuffer)
    {
      // relocate aggregators (see https://github.com/apache/druid/pull/4071)
      aggregators.relocate(
          oldBucketOffset + baseAggregatorOffset,
          newBucketOffset + baseAggregatorOffset,
          oldBuffer,
          newBuffer
      );

      offsetList.add(newBucketOffset);
    }
  }
}
