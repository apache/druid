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
  protected final ByteBuffer buffer;
  protected final int bucketSizeWithHash;
  protected final int tableArenaSize;
  protected final int keySize;

  protected int tableStart;

  // Buffer pointing to the current table (it moves around as the table grows)
  protected ByteBuffer tableBuffer;

  // Number of elements in the table right now
  protected int size;

  // Maximum number of elements in the table before it must be resized
  // This value changes when the table is resized.
  protected int regrowthThreshold;

  // current number of available/used buckets in the table
  // This value changes when the table is resized.
  protected int maxBuckets;

  // how many times the table buffer has filled/readjusted (through adjustTableWhenFull())
  protected int growthCount;



  protected BucketUpdateHandler bucketUpdateHandler;

  public ByteBufferHashTable(
      float maxLoadFactor,
      int initialBuckets,
      int bucketSizeWithHash,
      ByteBuffer buffer,
      int keySize,
      int maxSizeForTesting,
      BucketUpdateHandler bucketUpdateHandler
  )
  {
    this.maxLoadFactor = maxLoadFactor;
    this.initialBuckets = initialBuckets;
    this.bucketSizeWithHash = bucketSizeWithHash;
    this.buffer = buffer;
    this.keySize = keySize;
    this.maxSizeForTesting = maxSizeForTesting;
    this.tableArenaSize = buffer.capacity();
    this.bucketUpdateHandler = bucketUpdateHandler;
  }

  public void reset()
  {
    size = 0;

    maxBuckets = Math.min(tableArenaSize / bucketSizeWithHash, initialBuckets);
    regrowthThreshold = maxSizeForBuckets(maxBuckets);

    if (maxBuckets < 1) {
      throw new IAE(
          "Not enough capacity for even one row! Need[%,d] but have[%,d].",
          bucketSizeWithHash + Ints.BYTES,
          buffer.capacity()
      );
    }

    // Start table part-way through the buffer so the last growth can start from zero and thereby use more space.
    tableStart = tableArenaSize - maxBuckets * bucketSizeWithHash;
    int nextBuckets = maxBuckets * 2;
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
    bufferDup.limit(tableStart + maxBuckets * bucketSizeWithHash);
    tableBuffer = bufferDup.slice();

    // Clear used bits of new table
    for (int i = 0; i < maxBuckets; i++) {
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

    if (((long) maxBuckets * 3 * bucketSizeWithHash) > (long) tableArenaSize - tableStart) {
      // Not enough space to grow upwards, start back from zero
      newTableStart = 0;
      newBuckets = tableStart / bucketSizeWithHash;
      newMaxSize = maxSizeForBuckets(newBuckets);
    } else {
      newTableStart = tableStart + tableBuffer.limit();
      newBuckets = maxBuckets * 2;
      newMaxSize = maxSizeForBuckets(newBuckets);
    }

    if (newBuckets < maxBuckets) {
      throw new ISE("WTF?! newBuckets[%,d] < maxBuckets[%,d]", newBuckets, maxBuckets);
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

    int oldBuckets = maxBuckets;

    if (bucketUpdateHandler != null) {
      bucketUpdateHandler.handlePreTableSwap();
    }

    for (int oldBucket = 0; oldBucket < oldBuckets; oldBucket++) {
      if (isBucketUsed(oldBucket)) {
        int oldBucketOffset = oldBucket * bucketSizeWithHash;
        entryBuffer.limit((oldBucket + 1) * bucketSizeWithHash);
        entryBuffer.position(oldBucketOffset);
        keyBuffer.limit(entryBuffer.position() + HASH_SIZE + keySize);
        keyBuffer.position(entryBuffer.position() + HASH_SIZE);

        final int keyHash = entryBuffer.getInt(entryBuffer.position()) & 0x7fffffff;
        final int newBucket = findBucket(true, newBuckets, newTableBuffer, keyBuffer, keyHash);

        if (newBucket < 0) {
          throw new ISE("WTF?! Couldn't find a bucket while resizing?!");
        }

        final int newBucketOffset = newBucket * bucketSizeWithHash;

        newTableBuffer.position(newBucketOffset);
        newTableBuffer.put(entryBuffer);

        newSize++;

        if (bucketUpdateHandler != null) {
          bucketUpdateHandler.handleBucketMove(oldBucketOffset, newBucketOffset, tableBuffer, newTableBuffer);
        }
      }
    }

    maxBuckets = newBuckets;
    regrowthThreshold = newMaxSize;
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

    if (bucketUpdateHandler != null) {
      bucketUpdateHandler.handleNewBucket(offset);
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
    int bucket = findBucket(canAllowNewBucket(), maxBuckets, tableBuffer, keyBuffer, keyHash);

    if (bucket < 0) {
      if (size < maxSizeForTesting) {
        adjustTableWhenFull();
        bucket = findBucket(size < regrowthThreshold, maxBuckets, tableBuffer, keyBuffer, keyHash);
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
            // Should never happen unless buckets == regrowthThreshold.
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
    return size < Math.min(regrowthThreshold, maxSizeForTesting);
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

  public int getRegrowthThreshold()
  {
    return regrowthThreshold;
  }

  public int getMaxBuckets()
  {
    return maxBuckets;
  }

  public int getGrowthCount()
  {
    return growthCount;
  }

  public interface BucketUpdateHandler
  {
    void handleNewBucket(int bucketOffset);
    void handlePreTableSwap();
    void handleBucketMove(int oldBucketOffset, int newBucketOffset, ByteBuffer oldBuffer, ByteBuffer newBuffer);
  }
}
