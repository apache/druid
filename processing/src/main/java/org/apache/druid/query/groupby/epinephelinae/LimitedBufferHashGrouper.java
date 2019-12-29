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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

public class LimitedBufferHashGrouper<KeyType> extends AbstractBufferHashGrouper<KeyType>
{
  private static final int MIN_INITIAL_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.7f;

  // Limit to apply to results.
  private int limit;

  // Indicates if the sorting order has fields not in the grouping key, used when pushing down limit/sorting.
  // In this case, grouping key comparisons need to also compare on aggregators.
  // Additionally, results must be resorted by grouping key to allow results to merge correctly.
  private boolean sortHasNonGroupingFields;

  // Min-max heap, used for storing offsets when applying limits/sorting in the BufferHashGrouper
  private ByteBufferMinMaxOffsetHeap offsetHeap;

  // ByteBuffer slices used by the grouper
  private ByteBuffer totalBuffer;
  private ByteBuffer hashTableBuffer;
  private ByteBuffer offsetHeapBuffer;

  // Updates the heap index field for buckets, created passed to the heap when
  // pushing down limit and the sort order includes aggregators
  private BufferGrouperOffsetHeapIndexUpdater heapIndexUpdater;
  private boolean initialized = false;

  public LimitedBufferHashGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final Grouper.KeySerde<KeyType> keySerde,
      final AggregatorAdapters aggregators,
      final int bufferGrouperMaxSize,
      final float maxLoadFactor,
      final int initialBuckets,
      final int limit,
      final boolean sortHasNonGroupingFields
  )
  {
    super(bufferSupplier, keySerde, aggregators, HASH_SIZE + keySerde.keySize(), bufferGrouperMaxSize);
    this.maxLoadFactor = maxLoadFactor > 0 ? maxLoadFactor : DEFAULT_MAX_LOAD_FACTOR;
    this.initialBuckets = initialBuckets > 0 ? Math.max(MIN_INITIAL_BUCKETS, initialBuckets) : DEFAULT_INITIAL_BUCKETS;
    this.limit = limit;
    this.sortHasNonGroupingFields = sortHasNonGroupingFields;

    if (this.maxLoadFactor >= 1.0f) {
      throw new IAE("Invalid maxLoadFactor[%f], must be < 1.0", maxLoadFactor);
    }

    // For each bucket, store an extra field indicating the bucket's current index within the heap when
    // pushing down limits (size Integer.BYTES).
    this.bucketSize = HASH_SIZE + keySerde.keySize() + Integer.BYTES + aggregators.spaceNeeded();
  }

  @Override
  public void init()
  {
    if (initialized) {
      return;
    }
    this.totalBuffer = bufferSupplier.get();

    // We check this already in SpillingGrouper to ensure that LimitedBufferHashGrouper is only used when there is
    // sufficient buffer capacity. If this error occurs, something went very wrong.
    if (!validateBufferCapacity(totalBuffer.capacity())) {
      throw new IAE("WTF? Using LimitedBufferHashGrouper with insufficient buffer capacity.");
    }

    //only store offsets up to `limit` + 1 instead of up to # of buckets, we only keep the top results
    int heapByteSize = (limit + 1) * Integer.BYTES;

    int hashTableSize = ByteBufferHashTable.calculateTableArenaSizeWithFixedAdditionalSize(
        totalBuffer.capacity(),
        bucketSize,
        heapByteSize
    );

    hashTableBuffer = totalBuffer.duplicate();
    hashTableBuffer.position(0);
    hashTableBuffer.limit(hashTableSize);
    hashTableBuffer = hashTableBuffer.slice();

    offsetHeapBuffer = totalBuffer.duplicate();
    offsetHeapBuffer.position(hashTableSize);
    offsetHeapBuffer = offsetHeapBuffer.slice();
    offsetHeapBuffer.limit(totalBuffer.capacity() - hashTableSize);

    this.hashTable = new AlternatingByteBufferHashTable(
        maxLoadFactor,
        initialBuckets,
        bucketSize,
        hashTableBuffer,
        keySize,
        bufferGrouperMaxSize
    );
    this.heapIndexUpdater = new BufferGrouperOffsetHeapIndexUpdater(totalBuffer, bucketSize - Integer.BYTES);
    this.offsetHeap = new ByteBufferMinMaxOffsetHeap(offsetHeapBuffer, limit, makeHeapComparator(), heapIndexUpdater);

    reset();

    initialized = true;
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public void newBucketHook(int bucketOffset)
  {
    heapIndexUpdater.updateHeapIndexForOffset(bucketOffset, -1);
    if (!sortHasNonGroupingFields) {
      offsetHeap.addOffset(bucketOffset);
    }
  }

  @Override
  public boolean canSkipAggregate(int bucketOffset)
  {
    return !sortHasNonGroupingFields && heapIndexUpdater.getHeapIndexForOffset(bucketOffset) < 0;
  }

  @Override
  public void afterAggregateHook(int bucketOffset)
  {
    if (sortHasNonGroupingFields) {
      int heapIndex = heapIndexUpdater.getHeapIndexForOffset(bucketOffset);

      if (heapIndex < 0) {
        offsetHeap.addOffset(bucketOffset);
      } else {
        // Since the sorting columns contain at least one aggregator, we need to remove and reinsert
        // the entries after aggregating to maintain proper ordering
        offsetHeap.removeAt(heapIndex);
        offsetHeap.addOffset(bucketOffset);
      }
    }
  }

  @Override
  public void reset()
  {
    hashTable.reset();
    keySerde.reset();
    offsetHeap.reset();
    heapIndexUpdater.setHashTableBuffer(hashTable.getTableBuffer());
  }

  @Override
  public CloseableIterator<Entry<KeyType>> iterator(boolean sorted)
  {
    if (!initialized) {
      // it's possible for iterator() to be called before initialization when
      // a nested groupBy's subquery has an empty result set (see testEmptySubqueryWithLimitPushDown()
      // in GroupByQueryRunnerTest)
      return CloseableIterators.withEmptyBaggage(Collections.emptyIterator());
    }

    if (sortHasNonGroupingFields) {
      // re-sort the heap in place, it's also an array of offsets in the totalBuffer
      return makeDefaultOrderingIterator();
    } else {
      return makeHeapIterator();
    }
  }

  public int getLimit()
  {
    return limit;
  }

  public static class BufferGrouperOffsetHeapIndexUpdater
  {
    private ByteBuffer hashTableBuffer;
    private final int indexPosition;

    public BufferGrouperOffsetHeapIndexUpdater(
        ByteBuffer hashTableBuffer,
        int indexPosition
    )
    {
      this.hashTableBuffer = hashTableBuffer;
      this.indexPosition = indexPosition;
    }

    public void setHashTableBuffer(ByteBuffer newTableBuffer)
    {
      hashTableBuffer = newTableBuffer;
    }

    public void updateHeapIndexForOffset(int bucketOffset, int newHeapIndex)
    {
      hashTableBuffer.putInt(bucketOffset + indexPosition, newHeapIndex);
    }

    public int getHeapIndexForOffset(int bucketOffset)
    {
      return hashTableBuffer.getInt(bucketOffset + indexPosition);
    }
  }

  private CloseableIterator<Entry<KeyType>> makeDefaultOrderingIterator()
  {
    final int size = offsetHeap.getHeapSize();

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    final List<Integer> wrappedOffsets = new AbstractList<Integer>()
    {
      @Override
      public Integer get(int index)
      {
        return offsetHeap.getAt(index);
      }

      @Override
      public Integer set(int index, Integer element)
      {
        final Integer oldValue = get(index);
        offsetHeap.setAt(index, element);
        return oldValue;
      }

      @Override
      public int size()
      {
        return size;
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
            final ByteBuffer curHashTableBuffer = hashTable.getTableBuffer();
            return comparator.compare(
                curHashTableBuffer,
                curHashTableBuffer,
                lhs + HASH_SIZE,
                rhs + HASH_SIZE
            );
          }
        }
    );

    return new CloseableIterator<Entry<KeyType>>()
    {
      int curr = 0;

      @Override
      public boolean hasNext()
      {
        return curr < size;
      }

      @Override
      public Grouper.Entry<KeyType> next()
      {
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
  }

  private CloseableIterator<Entry<KeyType>> makeHeapIterator()
  {
    final int initialHeapSize = offsetHeap.getHeapSize();
    return new CloseableIterator<Entry<KeyType>>()
    {
      int curr = 0;

      @Override
      public boolean hasNext()
      {
        return curr < initialHeapSize;
      }

      @Override
      public Grouper.Entry<KeyType> next()
      {
        if (curr >= initialHeapSize) {
          throw new NoSuchElementException();
        }
        final int offset = offsetHeap.removeMin();
        final Grouper.Entry<KeyType> entry = bucketEntryForOffset(offset);
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

  private Comparator<Integer> makeHeapComparator()
  {
    return new Comparator<Integer>()
    {
      final BufferComparator bufferComparator = keySerde.bufferComparatorWithAggregators(
          aggregators.factories().toArray(new AggregatorFactory[0]),
          aggregators.aggregatorPositions()
      );

      @Override
      public int compare(Integer o1, Integer o2)
      {
        final ByteBuffer tableBuffer = hashTable.getTableBuffer();
        return bufferComparator.compare(tableBuffer, tableBuffer, o1 + HASH_SIZE, o2 + HASH_SIZE);
      }
    };
  }

  public boolean validateBufferCapacity(int bufferCapacity)
  {
    int numBucketsNeeded = (int) Math.ceil((limit + 1) / maxLoadFactor);
    int targetTableArenaSize = numBucketsNeeded * bucketSize * 2;
    int heapSize = (limit + 1) * (Integer.BYTES);
    int requiredSize = targetTableArenaSize + heapSize;

    if (bufferCapacity < requiredSize) {
      log.debug(
          "Buffer capacity [%,d] is too small for limit[%d] with load factor[%f], " +
          "minimum bytes needed: [%,d], not applying limit push down optimization.",
          bufferCapacity,
          limit,
          maxLoadFactor,
          requiredSize
      );
      return false;
    } else {
      return true;
    }
  }

  private class AlternatingByteBufferHashTable extends ByteBufferHashTable
  {
    // The base buffer is split into two alternating halves, with one sub-buffer in use at a given time.
    // When the current sub-buffer fills, the used bits of the other sub-buffer are cleared, entries up to the limit
    // are copied from the current full sub-buffer to the new buffer, and the active buffer (referenced by tableBuffer)
    // is swapped to the new buffer.
    private ByteBuffer[] subHashTableBuffers;

    public AlternatingByteBufferHashTable(
        float maxLoadFactor,
        int initialBuckets,
        int bucketSizeWithHash,
        ByteBuffer totalHashTableBuffer,
        int keySize,
        int maxSizeForTesting
    )
    {
      super(
          maxLoadFactor,
          initialBuckets,
          bucketSizeWithHash,
          totalHashTableBuffer,
          keySize,
          maxSizeForTesting,
          null
      );

      this.growthCount = 0;

      int subHashTableSize = tableArenaSize / 2;
      maxBuckets = subHashTableSize / bucketSizeWithHash;
      regrowthThreshold = maxSizeForBuckets(maxBuckets);

      // split the hashtable into 2 sub tables that we rotate between
      ByteBuffer subHashTable1Buffer = totalHashTableBuffer.duplicate();
      subHashTable1Buffer.position(0);
      subHashTable1Buffer.limit(subHashTableSize);
      subHashTable1Buffer = subHashTable1Buffer.slice();

      ByteBuffer subHashTable2Buffer = totalHashTableBuffer.duplicate();
      subHashTable2Buffer.position(subHashTableSize);
      subHashTable2Buffer.limit(tableArenaSize);
      subHashTable2Buffer = subHashTable2Buffer.slice();

      subHashTableBuffers = new ByteBuffer[]{subHashTable1Buffer, subHashTable2Buffer};
    }

    @Override
    public void reset()
    {
      size = 0;
      growthCount = 0;
      // clear the used bits of the first buffer
      for (int i = 0; i < maxBuckets; i++) {
        subHashTableBuffers[0].put(i * bucketSizeWithHash, (byte) 0);
      }
      tableBuffer = subHashTableBuffers[0];
    }

    @Override
    public void adjustTableWhenFull()
    {
      int newTableIdx = (growthCount % 2 == 0) ? 1 : 0;
      ByteBuffer newTableBuffer = subHashTableBuffers[newTableIdx];

      // clear the used bits of the buffer we're swapping to
      for (int i = 0; i < maxBuckets; i++) {
        newTableBuffer.put(i * bucketSizeWithHash, (byte) 0);
      }

      // Get the offsets of the top N buckets from the heap and copy the buckets to new table
      final ByteBuffer entryBuffer = tableBuffer.duplicate();
      final ByteBuffer keyBuffer = tableBuffer.duplicate();

      int numCopied = 0;
      for (int i = 0; i < offsetHeap.getHeapSize(); i++) {
        final int oldBucketOffset = offsetHeap.getAt(i);

        if (isOffsetUsed(oldBucketOffset)) {
          // Read the entry from the old hash table
          entryBuffer.limit(oldBucketOffset + bucketSizeWithHash);
          entryBuffer.position(oldBucketOffset);
          keyBuffer.limit(entryBuffer.position() + HASH_SIZE + keySize);
          keyBuffer.position(entryBuffer.position() + HASH_SIZE);

          // Put the entry in the new hash table
          final int keyHash = entryBuffer.getInt(entryBuffer.position()) & 0x7fffffff;
          final int newBucket = findBucket(true, maxBuckets, newTableBuffer, keyBuffer, keyHash);

          if (newBucket < 0) {
            throw new ISE("WTF?! Couldn't find a bucket while resizing?!");
          }

          final int newBucketOffset = newBucket * bucketSizeWithHash;
          newTableBuffer.position(newBucketOffset);
          newTableBuffer.put(entryBuffer);
          numCopied++;

          // Update the heap with the copied bucket's new offset in the new table
          offsetHeap.setAt(i, newBucketOffset);

          // relocate aggregators (see https://github.com/apache/druid/pull/4071)
          aggregators.relocate(
              oldBucketOffset + baseAggregatorOffset,
              newBucketOffset + baseAggregatorOffset,
              tableBuffer,
              newTableBuffer
          );
        }
      }

      size = numCopied;
      tableBuffer = newTableBuffer;
      growthCount++;
    }
  }
}
