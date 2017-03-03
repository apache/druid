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

import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;

import java.nio.ByteBuffer;

public class ByteBufferHashTable
{
  public static int calculateTableArenaSizeWithPerBucketAdditionalSize(
      int bufferCapacity,
      int bucketSize,
      int perBucketAdditionalSize
  )
  {
    return (bufferCapacity / (bucketSize + perBucketAdditionalSize)) * bucketSize;
  }

  public static int calculateTableArenaSizeWithFixedAdditionalSize(
      int bufferCapacity,
      int bucketSize,
      int fixedAdditionalSize
  )
  {
    return ((bufferCapacity - fixedAdditionalSize) / bucketSize) * bucketSize;
  }

  protected final int maxSizeForTesting; // Integer.MAX_VALUE in production, only used for unit tests

  protected static final int HASH_SIZE = Ints.BYTES;

  protected final float maxLoadFactor;
  protected final int initialBuckets;
  protected ByteBuffer buffer;
  protected int bucketSizeWithHash;
  protected int tableArenaSize;
  protected int keySize;

  protected int tableStart;

  // Buffer pointing to the current table (it moves around as the table grows)
  protected ByteBuffer tableBuffer;

  // Number of elements in the table right now
  protected int size;

  // Maximum number of elements in the table before it must be resized
  protected int maxSize;

  // current number of available/used buckets in the table
  protected int buckets;

  // how many times the table buffer has filled/readjusted (through adjustTableWhenFull())
  protected int growthCount;

  // If not null, track the offsets of used buckets using this list.
  // When a new bucket is initialized by initializeNewBucketKey(), an offset is added to this list.
  // When expanding the table, the list is reset() and filled with the new offsets of the copied buckets.
  protected ByteBufferIntList bucketOffsetList;

  public ByteBufferHashTable(
      float maxLoadFactor,
      int initialBuckets,
      int bucketSizeWithHash,
      ByteBuffer buffer,
      int keySize,
      int maxSizeForTesting,
      ByteBufferIntList bucketOffsetList
  )
  {
    this.maxLoadFactor = maxLoadFactor;
    this.initialBuckets = initialBuckets;
    this.bucketSizeWithHash = bucketSizeWithHash;
    this.buffer = buffer;
    this.keySize = keySize;
    this.maxSizeForTesting = maxSizeForTesting;
    this.tableArenaSize = buffer.capacity();
    this.bucketOffsetList = bucketOffsetList;
  }

  public void reset()
  {
    size = 0;

    buckets = Math.min(tableArenaSize / bucketSizeWithHash, initialBuckets);
    maxSize = maxSizeForBuckets(buckets);

    if (buckets < 1) {
      throw new IAE(
          "Not enough capacity for even one row! Need[%,d] but have[%,d].",
          bucketSizeWithHash + Ints.BYTES,
          buffer.capacity()
      );
    }

    // Start table part-way through the buffer so the last growth can start from zero and thereby use more space.
    tableStart = tableArenaSize - buckets * bucketSizeWithHash;
    int nextBuckets = buckets * 2;
    while (true) {
      final int nextTableStart = tableStart - nextBuckets * bucketSizeWithHash;
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
    bufferDup.limit(tableStart + buckets * bucketSizeWithHash);
    tableBuffer = bufferDup.slice();

    // Clear used bits of new table
    for (int i = 0; i < buckets; i++) {
      tableBuffer.put(i * bucketSizeWithHash, (byte) 0);
    }
  }

  public void adjustTableWhenFull()
  {
    if (tableStart == 0) {
      // tableStart = 0 is the last growth; no further growing is possible.
      return;
    }

    final int newBuckets;
    final int newMaxSize;
    final int newTableStart;

    if ((tableStart + buckets * 3 * bucketSizeWithHash) > tableArenaSize) {
      // Not enough space to grow upwards, start back from zero
      newTableStart = 0;
      newBuckets = tableStart / bucketSizeWithHash;
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
    newTableBuffer.limit(newTableStart + newBuckets * bucketSizeWithHash);
    newTableBuffer = newTableBuffer.slice();

    int newSize = 0;

    // Clear used bits of new table
    for (int i = 0; i < newBuckets; i++) {
      newTableBuffer.put(i * bucketSizeWithHash, (byte) 0);
    }

    // Loop over old buckets and copy to new table
    final ByteBuffer entryBuffer = tableBuffer.duplicate();
    final ByteBuffer keyBuffer = tableBuffer.duplicate();

    int oldBuckets = buckets;

    if (bucketOffsetList != null) {
      bucketOffsetList.reset();
    }

    for (int oldBucket = 0; oldBucket < oldBuckets; oldBucket++) {
      if (isBucketUsed(oldBucket)) {
        entryBuffer.limit((oldBucket + 1) * bucketSizeWithHash);
        entryBuffer.position(oldBucket * bucketSizeWithHash);
        keyBuffer.limit(entryBuffer.position() + HASH_SIZE + keySize);
        keyBuffer.position(entryBuffer.position() + HASH_SIZE);

        final int keyHash = entryBuffer.getInt(entryBuffer.position()) & 0x7fffffff;
        final int newBucket = findBucket(true, newBuckets, newTableBuffer, keyBuffer, keyHash);

        if (newBucket < 0) {
          throw new ISE("WTF?! Couldn't find a bucket while resizing?!");
        }

        final int newOffset = newBucket * bucketSizeWithHash;

        newTableBuffer.position(newOffset);
        newTableBuffer.put(entryBuffer);

        newSize++;

        if (bucketOffsetList != null) {
          bucketOffsetList.add(newOffset);
        }
      }
    }

    buckets = newBuckets;
    maxSize = newMaxSize;
    tableBuffer = newTableBuffer;
    tableStart = newTableStart;

    growthCount++;

    if (size != newSize) {
      throw new ISE("WTF?! size[%,d] != newSize[%,d] after resizing?!", size, newSize);
    }
  }

  protected void initializeNewBucketKey(
      final int bucket,
      final ByteBuffer keyBuffer,
      final int keyHash
  )
  {
    int offset = bucket * bucketSizeWithHash;
    tableBuffer.position(offset);
    tableBuffer.putInt(keyHash | 0x80000000);
    tableBuffer.put(keyBuffer);
    size++;

    if (bucketOffsetList != null) {
      bucketOffsetList.add(bucket * bucketSizeWithHash);
    }
  }

  /**
   * Find a bucket for a key, attempting to resize the table with adjustTableWhenFull() if possible.
   *
   * @param keyBuffer buffer containing the key
   * @param keyHash hash of the key
   * @return bucket number of the found bucket or -1 if a bucket could not be allocated after resizing.
   */
  protected int findBucketWithAutoGrowth(
      final ByteBuffer keyBuffer,
      final int keyHash
  )
  {
    int bucket = findBucket(canAllowNewBucket(), buckets, tableBuffer, keyBuffer, keyHash);

    if (bucket < 0) {
      if (size < maxSizeForTesting) {
        adjustTableWhenFull();
        bucket = findBucket(size < maxSize, buckets, tableBuffer, keyBuffer, keyHash);
      }
    }

    return bucket;
  }

  /**
   * Finds the bucket into which we should insert a key.
   *
   * @param keyBuffer key, must have exactly keySize bytes remaining. Will not be modified.
   * @param targetTableBuffer Need selectable buffer, since when resizing hash table,
   *                          findBucket() is used on the newly allocated table buffer
   *
   * @return bucket index for this key, or -1 if no bucket is available due to being full
   */
  protected int findBucket(
      final boolean allowNewBucket,
      final int buckets,
      final ByteBuffer targetTableBuffer,
      final ByteBuffer keyBuffer,
      final int keyHash
  )
  {
    // startBucket will never be negative since keyHash is always positive (see Groupers.hash)
    final int startBucket = keyHash % buckets;
    int bucket = startBucket;

outer:
    while (true) {
      final int bucketOffset = bucket * bucketSizeWithHash;

      if ((targetTableBuffer.get(bucketOffset) & 0x80) == 0) {
        // Found unused bucket before finding our key
        return allowNewBucket ? bucket : -1;
      }

      for (int i = bucketOffset + HASH_SIZE, j = keyBuffer.position(); j < keyBuffer.position() + keySize; i++, j++) {
        if (targetTableBuffer.get(i) != keyBuffer.get(j)) {
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

  protected boolean canAllowNewBucket()
  {
    return size < Math.min(maxSize, maxSizeForTesting);
  }

  protected int getOffsetForBucket(int bucket)
  {
    return bucket * bucketSizeWithHash;
  }

  protected int maxSizeForBuckets(int buckets)
  {
    return Math.max(1, (int) (buckets * maxLoadFactor));
  }

  protected boolean isBucketUsed(final int bucket)
  {
    return (tableBuffer.get(bucket * bucketSizeWithHash) & 0x80) == 0x80;
  }

  protected boolean isOffsetUsed(final int bucketOffset)
  {
    return (tableBuffer.get(bucketOffset) & 0x80) == 0x80;
  }

  public ByteBuffer getTableBuffer()
  {
    return tableBuffer;
  }

  public int getSize()
  {
    return size;
  }

  public int getMaxSize()
  {
    return maxSize;
  }

  public int getBuckets()
  {
    return buckets;
  }

  public int getGrowthCount()
  {
    return growthCount;
  }
}
