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
import io.druid.java.util.common.ISE;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class LimitedBufferGrouper<KeyType> extends AbstractBufferGrouper<KeyType>
{
  private static final int MIN_INITIAL_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.7f;

  private final AggregatorFactory[] aggregatorFactories;

  // Limit to apply to results.
  // If limit > 0, track hash table entries in a binary heap with size of limit.
  // If -1, no limit is applied, hash table entry offsets are tracked with an unordered list with no limit.
  private int limit;

  // Indicates if the sorting order has fields not in the grouping key, used when pushing down limit/sorting.
  // In this case, grouping key comparisons need to also compare on aggregators.
  // Additionally, results must be resorted by grouping key to allow results to merge correctly.
  private boolean sortHasNonGroupingFields;

  // Min-max heap, used for storing offsets when applying limits/sorting in the BufferGrouper
  private ByteBufferMinMaxOffsetHeap offsetHeap;

  // ByteBuffer slices used by the grouper
  private ByteBuffer totalBuffer;
  private ByteBuffer hashTableBuffer;
  private ByteBuffer offsetHeapBuffer;

  // Updates the heap index field for buckets, created passed to the heap when
  // pushing down limit and the sort order includes aggregators
  private BufferGrouperOffsetHeapIndexUpdater heapIndexUpdater;
  private boolean initialized = false;

  public LimitedBufferGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final Grouper.KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize,
      final float maxLoadFactor,
      final int initialBuckets,
      final int limit,
      final boolean sortHasNonGroupingFields
  )
  {
    super(bufferSupplier, keySerde, aggregatorFactories, bufferGrouperMaxSize);
    this.maxLoadFactor = maxLoadFactor > 0 ? maxLoadFactor : DEFAULT_MAX_LOAD_FACTOR;
    this.initialBuckets = initialBuckets > 0 ? Math.max(MIN_INITIAL_BUCKETS, initialBuckets) : DEFAULT_INITIAL_BUCKETS;
    this.limit = limit;
    this.sortHasNonGroupingFields = sortHasNonGroupingFields;

    if (this.maxLoadFactor >= 1.0f) {
      throw new IAE("Invalid maxLoadFactor[%f], must be < 1.0", maxLoadFactor);
    }

    int offset = HASH_SIZE + keySize;
    this.aggregatorFactories = aggregatorFactories;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }

    // For each bucket, store an extra field indicating the bucket's current index within the heap when
    // pushing down limits
    offset += Ints.BYTES;
    this.bucketSize = offset;
  }

  @Override
  public void init()
  {
    if (initialized) {
      return;
    }
    this.totalBuffer = bufferSupplier.get();

    LimitedBufferGrouper.validateBufferCapacity(
        limit,
        maxLoadFactor,
        totalBuffer,
        bucketSize
    );

    //only store offsets up to `limit` + 1 instead of up to # of buckets, we only keep the top results
    int heapByteSize = (limit + 1) * Ints.BYTES;

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
    this.heapIndexUpdater = new BufferGrouperOffsetHeapIndexUpdater(totalBuffer, bucketSize - Ints.BYTES);
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
  }

  @Override
  public boolean canSkipAggregate(boolean bucketWasUsed, int bucketOffset)
  {
    if (bucketWasUsed) {
      if (!sortHasNonGroupingFields) {
        if (heapIndexUpdater.getHeapIndexForOffset(bucketOffset) < 0) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public void afterAggregateHook(int bucketOffset)
  {
    int heapIndex = heapIndexUpdater.getHeapIndexForOffset(bucketOffset);
    if (heapIndex < 0) {
      // not in the heap, add it
      offsetHeap.addOffset(bucketOffset);
    } else if (sortHasNonGroupingFields) {
      // Since the sorting columns contain at least one aggregator, we need to remove and reinsert
      // the entries after aggregating to maintain proper ordering
      offsetHeap.removeAt(heapIndex);
      offsetHeap.addOffset(bucketOffset);
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
  public Iterator<Entry<KeyType>> iterator(boolean sorted)
  {
    if (!initialized) {
      // it's possible for iterator() to be called before initialization when
      // a nested groupBy's subquery has an empty result set (see testEmptySubqueryWithLimitPushDown()
      // in GroupByQueryRunnerTest)
      return Iterators.<Entry<KeyType>>emptyIterator();
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

  public class BufferGrouperOffsetHeapIndexUpdater
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

    public void setHashTableBuffer(ByteBuffer newTableBuffer) {
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

  private Iterator<Grouper.Entry<KeyType>> makeDefaultOrderingIterator()
  {
    final int size = offsetHeap.getHeapSize();

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

    final Grouper.KeyComparator comparator = keySerde.bufferComparator();

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

    return new Iterator<Grouper.Entry<KeyType>>()
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
    };
  }

  private Iterator<Grouper.Entry<KeyType>> makeHeapIterator()
  {
    final int initialHeapSize = offsetHeap.getHeapSize();
    return new Iterator<Grouper.Entry<KeyType>>()
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
    };
  }

  private Comparator<Integer> makeHeapComparator()
  {
    return new Comparator<Integer>()
    {
      final Grouper.KeyComparator keyComparator = keySerde.bufferComparatorWithAggregators(
          aggregatorFactories,
          aggregatorOffsets
      );
      @Override
      public int compare(Integer o1, Integer o2)
      {
        final ByteBuffer tableBuffer = hashTable.getTableBuffer();
        return keyComparator.compare(tableBuffer, tableBuffer, o1 + HASH_SIZE, o2 + HASH_SIZE);
      }
    };
  }


  public static void validateBufferCapacity(
      int limit,
      float maxLoadFactor,
      ByteBuffer buffer,
      int bucketSize
  )
  {
    int numBucketsNeeded = (int) Math.ceil((limit + 1) / maxLoadFactor);
    int targetTableArenaSize = numBucketsNeeded * bucketSize * 2;
    int heapSize = (limit + 1) * (Ints.BYTES);
    int requiredSize = targetTableArenaSize + heapSize;

    if (buffer.capacity() < requiredSize) {
      throw new IAE(
          "Buffer capacity [%d] is too small for limit[%d] with load factor[%f], minimum bytes needed: [%d]",
          buffer.capacity(),
          limit,
          maxLoadFactor,
          requiredSize
      );
    }
  }

  private class AlternatingByteBufferHashTable extends ByteBufferHashTable
  {
    private ByteBuffer totalHashTableBuffer;
    private ByteBuffer[] subHashTableBuffers;
    private ByteBufferHashTable[] subHashTables;
    private ByteBufferHashTable activeHashTable;

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
      this.totalHashTableBuffer = totalHashTableBuffer;

      int subHashTableSize = tableArenaSize / 2;
      buckets = subHashTableSize / bucketSize;
      maxSize = maxSizeForBuckets(buckets);

      // split the hashtable into 2 sub tables that we rotate between
      ByteBuffer subHashTable1Buffer = totalHashTableBuffer.duplicate();
      subHashTable1Buffer.position(0);
      subHashTable1Buffer.limit(subHashTableSize);
      subHashTable1Buffer = subHashTable1Buffer.slice();

      ByteBuffer subHashTable2Buffer = totalHashTableBuffer.duplicate();
      subHashTable2Buffer.position(subHashTableSize);
      subHashTable2Buffer.limit(tableArenaSize);
      subHashTable2Buffer = subHashTable2Buffer.slice();

      subHashTableBuffers = new ByteBuffer[] {subHashTable1Buffer, subHashTable2Buffer};

      subHashTables = new ByteBufferHashTable[2];
      subHashTables[0] = new ByteBufferHashTable(
          maxLoadFactor,
          buckets,
          bucketSizeWithHash,
          subHashTableBuffers[0],
          keySize,
          maxSizeForTesting,
          null
      );
      subHashTables[1] = new ByteBufferHashTable(
          maxLoadFactor,
          buckets,
          bucketSizeWithHash,
          subHashTableBuffers[1],
          keySize,
          maxSizeForTesting,
          null
      );
    }

    @Override
    public void reset()
    {
      size = 0;
      growthCount = 0;
      subHashTables[0].reset();
      subHashTables[1].reset();
      tableBuffer = subHashTableBuffers[0];
      activeHashTable = subHashTables[0];
    }

    @Override
    public void adjustTableWhenFull()
    {
      int tableIdx = (growthCount % 2 == 0) ? 1 : 0;
      ByteBuffer newTableBuffer = subHashTableBuffers[tableIdx];
      ByteBufferHashTable newHashTable = subHashTables[tableIdx];
      newHashTable.reset();

      // Get the offsets of the top N buckets from the heap and copy the buckets to new table
      final ByteBuffer entryBuffer = tableBuffer.duplicate();
      final ByteBuffer keyBuffer = tableBuffer.duplicate();

      int numCopied = 0;
      for (int i = 0; i < offsetHeap.getHeapSize(); i++) {
        final int oldBucketOffset = offsetHeap.getAt(i);

        if (activeHashTable.isOffsetUsed(oldBucketOffset)) {
          // Read the entry from the old hash table
          entryBuffer.limit(oldBucketOffset + bucketSize);
          entryBuffer.position(oldBucketOffset);
          keyBuffer.limit(entryBuffer.position() + HASH_SIZE + keySize);
          keyBuffer.position(entryBuffer.position() + HASH_SIZE);

          // Put the entry in the new hash table
          final int keyHash = entryBuffer.getInt(entryBuffer.position()) & 0x7fffffff;
          final int newBucket =  newHashTable.findBucket(true, buckets, newTableBuffer, keyBuffer, keyHash);

          if (newBucket < 0) {
            throw new ISE("WTF?! Couldn't find a bucket while resizing?!");
          }

          final int newOffset = newBucket * bucketSize;
          newTableBuffer.position(newOffset);
          newTableBuffer.put(entryBuffer);
          numCopied++;

          // Update the heap with the copied bucket's new offset in the new table
          offsetHeap.setAt(i, newOffset);
        }
      }

      size = numCopied;
      tableBuffer = newTableBuffer;
      activeHashTable = newHashTable;
      growthCount++;
    }
  }
}
