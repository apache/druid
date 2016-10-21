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
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * Grouper based around a hash table and companion array in a single ByteBuffer. Not thread-safe.
 *
 * The buffer has two parts: a table arena (offset 0 to tableArenaSize) and an array containing pointers objects in
 * the table (tableArenaSize until the end of the buffer).
 *
 * The table uses open addressing with linear probing on collisions. Each bucket contains the key hash (with the high
 * bit set to signify the bucket is used), the serialized key (which are a fixed size) and scratch space for
 * BufferAggregators (which is also fixed size). The actual table is represented by "tableBuffer", which points to the
 * same memory as positions "tableStart" through "tableStart + buckets * bucketSize" of "buffer". Everything else in
 * the table arena is potentially junk.
 *
 * The array of pointers starts out ordered by insertion order, but might be sorted on calls to
 * {@link #iterator(boolean)}. This sorting is done in-place to avoid materializing the full array of pointers. The
 * first "size" pointers in the array of pointers are valid; everything else is potentially junk.
 *
 * The table is periodically grown to accommodate more keys. Even though starting small is not necessary to control
 * memory use (we already have the entire buffer allocated) or iteration speed (iteration is fast due to the array
 * of pointers) it still helps significantly on initialization times. Otherwise, we'd need to clear the used bits of
 * each bucket in the entire buffer, which is a lot of writes if the buckets are small.
 */
public class BufferGrouper<KeyType extends Comparable<KeyType>> implements Grouper<KeyType>
{
  private static final Logger log = new Logger(BufferGrouper.class);

  private static final int MIN_INITIAL_BUCKETS = 4;
  private static final int DEFAULT_INITIAL_BUCKETS = 1024;
  private static final float DEFAULT_MAX_LOAD_FACTOR = 0.7f;
  private static final int HASH_SIZE = Ints.BYTES;

  private final ByteBuffer buffer;
  private final KeySerde<KeyType> keySerde;
  private final int keySize;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int initialBuckets;
  private final int bucketSize;
  private final int tableArenaSize;
  private final int bufferGrouperMaxSize; // Integer.MAX_VALUE in production, only used for unit tests
  private final float maxLoadFactor;

  // Buffer pointing to the current table (it moves around as the table grows)
  private ByteBuffer tableBuffer;

  // Offset of tableBuffer within the larger buffer
  private int tableStart;

  // Current number of buckets in the table
  private int buckets;

  // Number of elements in the table right now
  private int size;

  // Maximum number of elements in the table before it must be resized
  private int maxSize;

  public BufferGrouper(
      final ByteBuffer buffer,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize,
      final float maxLoadFactor,
      final int initialBuckets
  )
  {
    this.buffer = buffer;
    this.keySerde = keySerde;
    this.keySize = keySerde.keySize();
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];
    this.bufferGrouperMaxSize = bufferGrouperMaxSize;
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
    this.tableArenaSize = (buffer.capacity() / (bucketSize + Ints.BYTES)) * bucketSize;

    reset();
  }

  @Override
  public boolean aggregate(KeyType key, int keyHash)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    if (keyBuffer == null) {
      return false;
    }

    Preconditions.checkArgument(
        keyBuffer.remaining() == keySize,
        "keySerde.toByteBuffer(key).remaining[%s] != keySerde.keySize[%s], buffer was the wrong size?!",
        keyBuffer.remaining(),
        keySize
    );

    int bucket = findBucket(
        tableBuffer,
        buckets,
        bucketSize,
        size < Math.min(maxSize, bufferGrouperMaxSize),
        keyBuffer,
        keySize,
        keyHash
    );

    if (bucket < 0) {
      if (size < bufferGrouperMaxSize) {
        growIfPossible();
        bucket = findBucket(tableBuffer, buckets, bucketSize, size < maxSize, keyBuffer, keySize, keyHash);
      }

      if (bucket < 0) {
        return false;
      }
    }

    final int offset = bucket * bucketSize;

    // Set up key if this is a new bucket.
    if (!isUsed(bucket)) {
      tableBuffer.position(offset);
      tableBuffer.putInt(keyHash | 0x80000000);
      tableBuffer.put(keyBuffer);

      for (int i = 0; i < aggregators.length; i++) {
        aggregators[i].init(tableBuffer, offset + aggregatorOffsets[i]);
      }

      buffer.putInt(tableArenaSize + size * Ints.BYTES, offset);
      size++;
    }

    // Aggregate the current row.
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].aggregate(tableBuffer, offset + aggregatorOffsets[i]);
    }

    return true;
  }

  @Override
  public boolean aggregate(final KeyType key)
  {
    return aggregate(key, Groupers.hash(key));
  }

  @Override
  public void reset()
  {
    size = 0;
    buckets = Math.min(tableArenaSize / bucketSize, initialBuckets);
    maxSize = maxSizeForBuckets(buckets);

    if (buckets < 1) {
      throw new IAE(
          "Not enough capacity for even one row! Need[%,d] but have[%,d].",
          bucketSize + Ints.BYTES,
          buffer.capacity()
      );
    }

    // Start table part-way through the buffer so the last growth can start from zero and thereby use more space.
    tableStart = tableArenaSize - buckets * bucketSize;
    int nextBuckets = buckets * 2;
    while (true) {
      final int nextTableStart = tableStart - nextBuckets * bucketSize;
      if (nextTableStart > tableArenaSize / 2) {
        tableStart = nextTableStart;
        nextBuckets = nextBuckets * 2;
      } else {
        break;
      }
    }

    if (tableStart < tableArenaSize / 2) {
      tableStart = 0;
    }

    final ByteBuffer bufferDup = buffer.duplicate();
    bufferDup.position(tableStart);
    bufferDup.limit(tableStart + buckets * bucketSize);
    tableBuffer = bufferDup.slice();

    // Clear used bits of new table
    for (int i = 0; i < buckets; i++) {
      tableBuffer.put(i * bucketSize, (byte) 0);
    }

    keySerde.reset();
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(final boolean sorted)
  {
    if (sorted) {
      final List<Integer> wrappedOffsets = new AbstractList<Integer>()
      {
        @Override
        public Integer get(int index)
        {
          return buffer.getInt(tableArenaSize + index * Ints.BYTES);
        }

        @Override
        public Integer set(int index, Integer element)
        {
          final Integer oldValue = get(index);
          buffer.putInt(tableArenaSize + index * Ints.BYTES, element);
          return oldValue;
        }

        @Override
        public int size()
        {
          return size;
        }
      };

      final KeyComparator comparator = keySerde.comparator();

      // Sort offsets in-place.
      Collections.sort(
          wrappedOffsets,
          new Comparator<Integer>()
          {
            @Override
            public int compare(Integer lhs, Integer rhs)
            {
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

        @Override
        public boolean hasNext()
        {
          return curr < size;
        }

        @Override
        public Entry<KeyType> next()
        {
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

        @Override
        public boolean hasNext()
        {
          return curr < size;
        }

        @Override
        public Entry<KeyType> next()
        {
          final int offset = buffer.getInt(tableArenaSize + curr * Ints.BYTES);
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

  @Override
  public void close()
  {
    for (BufferAggregator aggregator : aggregators) {
      try {
        aggregator.close();
      }
      catch (Exception e) {
        log.warn(e, "Could not close aggregator, skipping.", aggregator);
      }
    }
  }

  private boolean isUsed(final int bucket)
  {
    return (tableBuffer.get(bucket * bucketSize) & 0x80) == 0x80;
  }

  private Entry<KeyType> bucketEntryForOffset(final int bucketOffset)
  {
    final KeyType key = keySerde.fromByteBuffer(tableBuffer, bucketOffset + HASH_SIZE);
    final Object[] values = new Object[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      values[i] = aggregators[i].get(tableBuffer, bucketOffset + aggregatorOffsets[i]);
    }

    return new Entry<>(key, values);
  }

  private void growIfPossible()
  {
    if (tableStart == 0) {
      // tableStart = 0 is the last growth; no further growing is possible.
      return;
    }

    final int newBuckets;
    final int newMaxSize;
    final int newTableStart;

    if ((tableStart + buckets * 3 * bucketSize) > tableArenaSize) {
      // Not enough space to grow upwards, start back from zero
      newTableStart = 0;
      newBuckets = tableStart / bucketSize;
      newMaxSize = maxSizeForBuckets(newBuckets);
    } else {
      newTableStart = tableStart + tableBuffer.limit();
      newBuckets = buckets * 2;
      newMaxSize = maxSizeForBuckets(newBuckets);
    }

    if (newBuckets < buckets) {
      throw new ISE("WTF?! newBuckets[%,d] < buckets[%,d]", newBuckets, buckets);
    }

    ByteBuffer newTableBuffer = buffer.duplicate();
    newTableBuffer.position(newTableStart);
    newTableBuffer.limit(newTableStart + newBuckets * bucketSize);
    newTableBuffer = newTableBuffer.slice();

    int newSize = 0;

    // Clear used bits of new table
    for (int i = 0; i < newBuckets; i++) {
      newTableBuffer.put(i * bucketSize, (byte) 0);
    }

    // Loop over old buckets and copy to new table
    final ByteBuffer entryBuffer = tableBuffer.duplicate();
    final ByteBuffer keyBuffer = tableBuffer.duplicate();

    for (int oldBucket = 0; oldBucket < buckets; oldBucket++) {
      if (isUsed(oldBucket)) {
        entryBuffer.limit((oldBucket + 1) * bucketSize);
        entryBuffer.position(oldBucket * bucketSize);
        keyBuffer.limit(entryBuffer.position() + HASH_SIZE + keySize);
        keyBuffer.position(entryBuffer.position() + HASH_SIZE);

        final int keyHash = entryBuffer.getInt(entryBuffer.position()) & 0x7fffffff;
        final int newBucket = findBucket(newTableBuffer, newBuckets, bucketSize, true, keyBuffer, keySize, keyHash);

        if (newBucket < 0) {
          throw new ISE("WTF?! Couldn't find a bucket while resizing?!");
        }

        newTableBuffer.position(newBucket * bucketSize);
        newTableBuffer.put(entryBuffer);

        buffer.putInt(tableArenaSize + newSize * Ints.BYTES, newBucket * bucketSize);
        newSize++;
      }
    }

    buckets = newBuckets;
    maxSize = newMaxSize;
    tableBuffer = newTableBuffer;
    tableStart = newTableStart;

    if (size != newSize) {
      throw new ISE("WTF?! size[%,d] != newSize[%,d] after resizing?!", size, maxSize);
    }
  }

  private int maxSizeForBuckets(int buckets)
  {
    return Math.max(1, (int) (buckets * maxLoadFactor));
  }

  /**
   * Finds the bucket into which we should insert a key.
   *
   * @param keyBuffer key, must have exactly keySize bytes remaining. Will not be modified.
   *
   * @return bucket index for this key, or -1 if no bucket is available due to being full
   */
  private static int findBucket(
      final ByteBuffer tableBuffer,
      final int buckets,
      final int bucketSize,
      final boolean allowNewBucket,
      final ByteBuffer keyBuffer,
      final int keySize,
      final int keyHash
  )
  {
    // startBucket will never be negative since keyHash is always positive (see Groupers.hash)
    final int startBucket = keyHash % buckets;
    int bucket = startBucket;

outer:
    while (true) {
      final int bucketOffset = bucket * bucketSize;

      if ((tableBuffer.get(bucketOffset) & 0x80) == 0) {
        // Found unused bucket before finding our key
        return allowNewBucket ? bucket : -1;
      }

      for (int i = bucketOffset + HASH_SIZE, j = keyBuffer.position(); j < keyBuffer.position() + keySize; i++, j++) {
        if (tableBuffer.get(i) != keyBuffer.get(j)) {
          bucket += 1;
          if (bucket == buckets) {
            bucket = 0;
          }

          if (bucket == startBucket) {
            // Came back around to the start without finding a free slot, that was a long trip!
            // Should never happen unless buckets == maxSize.
            return -1;
          }

          continue outer;
        }
      }

      // Found our key in a used bucket
      return bucket;
    }
  }
}
