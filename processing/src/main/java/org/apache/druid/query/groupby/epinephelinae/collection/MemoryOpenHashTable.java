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

package org.apache.druid.query.groupby.epinephelinae.collection;

import it.unimi.dsi.fastutil.ints.IntIterator;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.groupby.epinephelinae.Groupers;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.NoSuchElementException;

/**
 * An open-addressed hash table with linear probing backed by {@link WritableMemory}. Does not offer a similar
 * interface to {@link java.util.Map} because this is meant to be useful to lower-level, high-performance callers.
 * There is no copying or serde of keys and values: callers access the backing memory of the table directly.
 *
 * This table will not grow itself. Callers must handle growing if required; the {@link #copyTo} method is provided
 * to assist.
 */
public class MemoryOpenHashTable
{
  private static final byte USED_BYTE = 1;
  private static final int USED_BYTE_SIZE = Byte.BYTES;

  private final WritableMemory tableMemory;
  private final int keySize;
  private final int valueSize;
  private final int bucketSize;

  // Maximum number of elements in the table (based on numBuckets and maxLoadFactor).
  private final int maxSize;

  // Number of available/used buckets in the table. Always a power of two.
  private final int numBuckets;

  // Mask that clips a number to [0, numBuckets). Used when searching through buckets.
  private final int bucketMask;

  // Number of elements in the table right now.
  private int size;

  /**
   * Create a new table.
   *
   * @param tableMemory backing memory for the table; must be exactly large enough to hold "numBuckets"
   * @param numBuckets  number of buckets for the table
   * @param maxSize     maximum number of elements for the table; must be less than numBuckets
   * @param keySize     key size in bytes
   * @param valueSize   value size in bytes
   */
  public MemoryOpenHashTable(
      final WritableMemory tableMemory,
      final int numBuckets,
      final int maxSize,
      final int keySize,
      final int valueSize
  )
  {
    this.tableMemory = tableMemory;
    this.numBuckets = numBuckets;
    this.bucketMask = numBuckets - 1;
    this.maxSize = maxSize;
    this.keySize = keySize;
    this.valueSize = valueSize;
    this.bucketSize = bucketSize(keySize, valueSize);

    // Our main intended users (VectorGrouper implementations) need the tableMemory to be backed by a big-endian
    // ByteBuffer that is coterminous with the tableMemory, since it's going to feed that buffer into VectorAggregators
    // instead of interacting with our WritableMemory directly. Nothing about this class actually requires that the
    // Memory be backed by a ByteBuffer, but we'll check it here anyway for the benefit of our biggest customer.
    verifyMemoryIsByteBuffer(tableMemory);

    if (!tableMemory.getTypeByteOrder().equals(ByteOrder.nativeOrder())) {
      throw new ISE("tableMemory must be native byte order");
    }

    if (tableMemory.getCapacity() != memoryNeeded(numBuckets, bucketSize)) {
      throw new ISE(
          "tableMemory must be size[%,d] but was[%,d]",
          memoryNeeded(numBuckets, bucketSize),
          tableMemory.getCapacity()
      );
    }

    if (maxSize >= numBuckets) {
      throw new ISE("maxSize must be less than numBuckets");
    }

    if (Integer.bitCount(numBuckets) != 1) {
      throw new ISE("numBuckets must be a power of two but was[%,d]", numBuckets);
    }

    clear();
  }

  /**
   * Returns the amount of memory needed for a table.
   *
   * This is just a multiplication, which is easy enough to do on your own, but sometimes it's nice for clarity's sake
   * to call a function with a name that indicates why the multiplication is happening.
   *
   * @param numBuckets number of buckets
   * @param bucketSize size per bucket (in bytes)
   *
   * @return size of table (in bytes)
   */
  public static int memoryNeeded(final int numBuckets, final int bucketSize)
  {
    return numBuckets * bucketSize;
  }

  /**
   * Returns the size of each bucket in a table.
   *
   * @param keySize   size of keys (in bytes)
   * @param valueSize size of values (in bytes)
   *
   * @return size of buckets (in bytes)
   */
  public static int bucketSize(final int keySize, final int valueSize)
  {
    return USED_BYTE_SIZE + keySize + valueSize;
  }

  /**
   * Clear the table, resetting size to zero.
   */
  public void clear()
  {
    size = 0;

    // Clear used flags.
    for (int bucket = 0; bucket < numBuckets; bucket++) {
      tableMemory.putByte((long) bucket * bucketSize, (byte) 0);
    }
  }

  /**
   * Copy this table into another one. The other table must be large enough to hold all the copied buckets. The other
   * table will be cleared before the copy takes place.
   *
   * @param other       the other table
   * @param copyHandler a callback that is notified for each copied bucket
   */
  public void copyTo(final MemoryOpenHashTable other, @Nullable final BucketCopyHandler copyHandler)
  {
    if (other.size() > 0) {
      other.clear();
    }

    for (int bucket = 0; bucket < numBuckets; bucket++) {
      final int bucketOffset = bucket * bucketSize;
      if (isOffsetUsed(bucketOffset)) {
        final int keyPosition = bucketOffset + USED_BYTE_SIZE;
        final int keyHash = Groupers.smear(HashTableUtils.hashMemory(tableMemory, keyPosition, keySize));
        final int newBucket = other.findBucket(keyHash, tableMemory, keyPosition);

        if (newBucket >= 0) {
          // Not expected to happen, since we cleared the other table first.
          throw new ISE("Found already-used bucket while copying");
        }

        if (!other.canInsertNewBucket()) {
          throw new ISE("Unable to copy bucket to new table, size[%,d]", other.size());
        }

        final int newBucketOffset = -(newBucket + 1) * bucketSize;
        assert !other.isOffsetUsed(newBucketOffset);
        tableMemory.copyTo(bucketOffset, other.tableMemory, newBucketOffset, bucketSize);
        other.size++;

        if (copyHandler != null) {
          copyHandler.bucketCopied(bucket, -(newBucket + 1), this, other);
        }
      }
    }

    // Sanity check.
    if (other.size() != size) {
      throw new ISE("New table size[%,d] != old table size[%,d] after copying", other.size(), size);
    }
  }

  /**
   * Finds the bucket for a particular key.
   *
   * @param keyHash          result of calling {@link HashTableUtils#hashMemory} on this key
   * @param keySpace         memory containing the key
   * @param keySpacePosition position of the key within keySpace
   *
   * @return bucket number if currently occupied, or {@code -bucket - 1} if not occupied (yet)
   */
  public int findBucket(final int keyHash, final Memory keySpace, final int keySpacePosition)
  {
    int bucket = keyHash & bucketMask;

    while (true) {
      final int bucketOffset = bucket * bucketSize;

      if (tableMemory.getByte(bucketOffset) == 0) {
        // Found unused bucket before finding our key.
        return -bucket - 1;
      }

      final boolean keyFound = HashTableUtils.memoryEquals(
          tableMemory,
          bucketOffset + USED_BYTE_SIZE,
          keySpace,
          keySpacePosition,
          keySize
      );

      if (keyFound) {
        return bucket;
      }

      bucket = (bucket + 1) & bucketMask;
    }
  }

  /**
   * Returns whether this table can accept a new bucket.
   */
  public boolean canInsertNewBucket()
  {
    return size < maxSize;
  }

  /**
   * Initialize a bucket with a particular key.
   *
   * Do not call this method unless the bucket is currently unused and {@link #canInsertNewBucket()} returns true.
   *
   * @param bucket           bucket number
   * @param keySpace         memory containing the key
   * @param keySpacePosition position of the key within keySpace
   */
  public void initBucket(final int bucket, final Memory keySpace, final int keySpacePosition)
  {
    final int bucketOffset = bucket * bucketSize;

    // Method preconditions.
    assert canInsertNewBucket() && !isOffsetUsed(bucketOffset);

    // Mark the bucket used and write in the key.
    tableMemory.putByte(bucketOffset, USED_BYTE);
    keySpace.copyTo(keySpacePosition, tableMemory, bucketOffset + USED_BYTE_SIZE, keySize);
    size++;
  }

  /**
   * Returns the number of elements currently in the table.
   */
  public int size()
  {
    return size;
  }

  /**
   * Returns the number of buckets in this table. Note that not all of these can actually be used. The amount that
   * can be used depends on the "maxSize" parameter provided during construction.
   */
  public int numBuckets()
  {
    return numBuckets;
  }

  /**
   * Returns the size of keys, in bytes.
   */
  public int keySize()
  {
    return keySize;
  }

  /**
   * Returns the size of values, in bytes.
   */
  public int valueSize()
  {
    return valueSize;
  }

  /**
   * Returns the offset within each bucket where the key starts.
   */
  public int bucketKeyOffset()
  {
    return USED_BYTE_SIZE;
  }

  /**
   * Returns the offset within each bucket where the value starts.
   */
  public int bucketValueOffset()
  {
    return USED_BYTE_SIZE + keySize;
  }

  /**
   * Returns the size in bytes of each bucket.
   */
  public int bucketSize()
  {
    return bucketSize;
  }

  /**
   * Returns the position within {@link #memory()} where a particular bucket starts.
   */
  public int bucketMemoryPosition(final int bucket)
  {
    return bucket * bucketSize;
  }

  /**
   * Returns the memory backing this table.
   */
  public WritableMemory memory()
  {
    return tableMemory;
  }

  /**
   * Iterates over all used buckets, returning bucket numbers for each one.
   *
   * The intent is that callers will pass the bucket numbers to {@link #bucketMemoryPosition} and then use
   * {@link #bucketKeyOffset()} and {@link #bucketValueOffset()} to extract keys and values from the buckets as needed.
   */
  public IntIterator bucketIterator()
  {
    return new IntIterator()
    {
      private int curr = 0;
      private int currBucket = -1;

      @Override
      public boolean hasNext()
      {
        return curr < size;
      }

      @Override
      public int nextInt()
      {
        if (curr >= size) {
          throw new NoSuchElementException();
        }

        currBucket++;

        while (!isOffsetUsed(currBucket * bucketSize)) {
          currBucket++;
        }

        curr++;
        return currBucket;
      }
    };
  }

  /**
   * Returns whether the bucket at position "bucketOffset" is used or not. Note that this is a bucket position (in
   * bytes), not a bucket number.
   */
  private boolean isOffsetUsed(final int bucketOffset)
  {
    return tableMemory.getByte(bucketOffset) == USED_BYTE;
  }

  /**
   * Validates that some Memory is coterminous with a backing big-endian ByteBuffer. Returns quietly if so, throws an
   * exception otherwise.
   */
  private static void verifyMemoryIsByteBuffer(final Memory memory)
  {
    final ByteBuffer buffer = memory.getByteBuffer();

    if (buffer == null) {
      throw new ISE("tableMemory must be ByteBuffer-backed");
    }

    if (!buffer.order().equals(ByteOrder.BIG_ENDIAN)) {
      throw new ISE("tableMemory's ByteBuffer must be in big-endian order");
    }

    if (buffer.capacity() != memory.getCapacity() || buffer.remaining() != buffer.capacity()) {
      throw new ISE("tableMemory's ByteBuffer must be coterminous");
    }
  }

  /**
   * Callback used by {@link #copyTo}.
   */
  public interface BucketCopyHandler
  {
    /**
     * Indicates that "oldBucket" in "oldTable" was copied to "newBucket" in "newTable".
     *
     * @param oldBucket old bucket number
     * @param newBucket new bucket number
     * @param oldTable  old table
     * @param newTable  new table
     */
    void bucketCopied(
        int oldBucket,
        int newBucket,
        MemoryOpenHashTable oldTable,
        MemoryOpenHashTable newTable
    );
  }
}
