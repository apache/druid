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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;

import java.nio.ByteBuffer;

/**
 * A fixed-width, open-addressing hash table that lives inside a caller-provided byte buffer.
 * <p>
 * The table uses a contiguous slice of the input {@link ByteBuffer} as its backing store. Each bucket holds
 * at most one entry, and occupies {@code bucketSizeWithHash} number of bytes. Collisions are resolved by continuously
 * probing the next bucket to find an empty bucket to slot the new entry. The current table view {@code tableBuffer}
 * is maintained as a {@link ByteBuffer} slice that moves and grows within the arena as the table expands.
 */
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

  protected static final int HASH_SIZE = Integer.BYTES;

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

  @Nullable
  protected BucketUpdateHandler bucketUpdateHandler;

  // Tracks maximum bytes used for the entire lifecycle of this hash table.
  protected long maxMergeBufferUsedBytes;

  // Peak {@code size / terminalRegrowthThreshold} over this table's lifetime, in [0.0, 1.0], where
  // {@code terminalRegrowthThreshold} is the FIXED bucket-count ceiling of the last growth level (see
  // {@link #getSpillRegrowthThreshold()}). Because the denominator is fixed and {@code size} only grows (and is
  // preserved across rehash), this rises smoothly from 0 toward 1 as the table fills, landing on exactly 1.0 at the
  // real spill trigger — unlike dividing by the CURRENT level's threshold, which resets to ~0.5 on every doubling and
  // pins the lifetime max near {@code (N-1)/N} regardless of how much headroom remains. Preserved across
  // {@link #reset()} so a grouper that spilled retains the 1.0 peak even after {@code size} returns to 0.
  protected double maxSpillProximity;

  // Cached denominator for {@link #maxSpillProximity}: the regrowthThreshold at the terminal (final) growth level.
  // Zero means "not yet computed"; {@link #maxSizeForBuckets} is always >= 1 so zero is a safe sentinel. Depends only
  // on final geometry ({@link #tableArenaSize}, {@link #bucketSizeWithHash}, {@link #initialBuckets},
  // {@link #maxLoadFactor}), so it is computed once and reused.
  private int spillRegrowthThreshold;

  public ByteBufferHashTable(
      float maxLoadFactor,
      int initialBuckets,
      int bucketSizeWithHash,
      ByteBuffer buffer,
      int keySize,
      int maxSizeForTesting,
      @Nullable BucketUpdateHandler bucketUpdateHandler
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
    this.maxMergeBufferUsedBytes = 0;
    this.maxSpillProximity = 0.0;
    this.spillRegrowthThreshold = 0;
  }

  public void reset()
  {
    size = 0;

    maxBuckets = Math.min(tableArenaSize / bucketSizeWithHash, initialBuckets);
    regrowthThreshold = maxSizeForBuckets(maxBuckets);

    if (maxBuckets < 1) {
      throw new IAE(
          "Not enough capacity for even one row! Need[%,d] but have[%,d].",
          bucketSizeWithHash + Integer.BYTES,
          buffer.capacity()
      );
    }

    // Start table part-way through the buffer so the last growth can start from zero and thereby use more space.
    tableStart = tableArenaSize - maxBuckets * bucketSizeWithHash;
    int nextBuckets = maxBuckets * 2;
    while (true) {
      long nextBucketsSize = (long) nextBuckets * bucketSizeWithHash;
      if (nextBucketsSize > Integer.MAX_VALUE) {
        break;
      }
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
    updateMaxMergeBufferUsedBytes();

    // Clear used bits of new table
    for (int i = 0; i < maxBuckets; i++) {
      tableBuffer.putInt(i * bucketSizeWithHash, 0);
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
      throw new ISE("newBuckets[%,d] < maxBuckets[%,d]", newBuckets, maxBuckets);
    }

    ByteBuffer newTableBuffer = buffer.duplicate();
    newTableBuffer.position(newTableStart);
    newTableBuffer.limit(newTableStart + newBuckets * bucketSizeWithHash);
    newTableBuffer = newTableBuffer.slice();

    int newSize = 0;

    // Clear used bits of new table
    for (int i = 0; i < newBuckets; i++) {
      newTableBuffer.putInt(i * bucketSizeWithHash, 0);
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

        final int keyHash = entryBuffer.getInt(entryBuffer.position()) & Groupers.USED_FLAG_MASK;
        final int newBucket = findBucket(true, newBuckets, newTableBuffer, keyBuffer, keyHash);

        if (newBucket < 0) {
          throw new ISE("Couldn't find a bucket while resizing");
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
      throw new ISE("size[%,d] != newSize[%,d] after resizing", size, newSize);
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
    tableBuffer.putInt(Groupers.getUsedFlag(keyHash));
    tableBuffer.put(keyBuffer);
    size++;
    updateMaxMergeBufferUsedBytes();

    if (bucketUpdateHandler != null) {
      bucketUpdateHandler.handleNewBucket(offset);
    }
  }

  /**
   * Find a bucket for a key, attempting to grow the table with adjustTableWhenFull() if possible.
   *
   * @param keyBuffer              buffer containing the key
   * @param keyHash                hash of the key
   * @param preTableGrowthRunnable runnable that executes before the table grows
   *
   * @return bucket number of the found bucket or -1 if a bucket could not be allocated after resizing.
   */
  protected int findBucketWithAutoGrowth(
      final ByteBuffer keyBuffer,
      final int keyHash,
      final Runnable preTableGrowthRunnable
  )
  {
    int bucket = findBucket(canAllowNewBucket(), maxBuckets, tableBuffer, keyBuffer, keyHash);

    if (bucket < 0) {
      if (size < maxSizeForTesting) {
        preTableGrowthRunnable.run();
        adjustTableWhenFull();
        bucket = findBucket(size < regrowthThreshold, maxBuckets, tableBuffer, keyBuffer, keyHash);
      }
    }

    if (bucket < 0) {
      // This is the caller's spill trigger: no bucket could be allocated even after attempting to grow. Pin the
      // proximity peak to exactly 1.0 here so callers see "at the spill point" even in the rare case a rejection
      // happens before {@code size} reaches the terminal threshold (e.g. a full-probe wraparound). See
      // {@link #maxSpillProximity}.
      maxSpillProximity = 1.0;
    }

    return bucket;
  }

  /**
   * Finds the bucket into which we should insert a key.
   *
   * @param keyBuffer         key, must have exactly keySize bytes remaining. Will not be modified.
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

    // Pre-compute hash with used flag for comparison.
    final int keyHashWithUsedFlag = Groupers.getUsedFlag(keyHash);
    final int keyBufferPosition = keyBuffer.position();

    while (true) {
      final int bucketOffset = bucket * bucketSizeWithHash;
      final int storedHashWithUsedFlag = targetTableBuffer.getInt(bucketOffset);

      if ((storedHashWithUsedFlag & Groupers.USED_FLAG_BIT) == 0) {
        // Found unused bucket before finding our key
        return allowNewBucket ? bucket : -1;
      }

      if (storedHashWithUsedFlag == keyHashWithUsedFlag &&
          keysEqual(targetTableBuffer, bucketOffset + HASH_SIZE, keyBuffer, keyBufferPosition, keySize)) {
        // Found our key in a used bucket
        return bucket;
      }

      // Move to next bucket (linear probing)
      bucket += 1;
      if (bucket == buckets) {
        bucket = 0;
      }

      if (bucket == startBucket) {
        // Came back around to the start without finding a free slot, that was a long trip!
        // Should never happen unless buckets == regrowthThreshold.
        return -1;
      }
    }
  }

  /**
   * Compare keys using long/int comparisons for better performance than byte-by-byte.
   */
  private static boolean keysEqual(
      final ByteBuffer tableBuffer,
      int tableOffset,
      final ByteBuffer keyBuffer,
      int keyOffset,
      int length
  )
  {
    // Compare 8 bytes at a time
    while (length >= Long.BYTES) {
      if (tableBuffer.getLong(tableOffset) != keyBuffer.getLong(keyOffset)) {
        return false;
      }
      tableOffset += Long.BYTES;
      keyOffset += Long.BYTES;
      length -= Long.BYTES;
    }

    // Compare 4 bytes if remaining
    if (length >= Integer.BYTES) {
      if (tableBuffer.getInt(tableOffset) != keyBuffer.getInt(keyOffset)) {
        return false;
      }
      tableOffset += Integer.BYTES;
      keyOffset += Integer.BYTES;
      length -= Integer.BYTES;
    }

    // Compare remaining 1-3 bytes
    while (length > 0) {
      if (tableBuffer.get(tableOffset) != keyBuffer.get(keyOffset)) {
        return false;
      }
      tableOffset++;
      keyOffset++;
      length--;
    }

    return true;
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

  /**
   * To maintain an accurate tracking of the maximum bytes used per query, this function is to be called immediately
   * whenever either of {@link #size} or {@link #bucketSizeWithHash} is changed. Also updates {@link #maxSpillProximity}
   * on every size mutation, so the peak {@code size / terminalRegrowthThreshold} ratio is tracked without the caller
   * having to remember to observe it. Preserving the peak across resets is what lets a grouper that already spilled
   * report proximity 1.0 even though {@code size} is currently 0.
   *
   * <p>The denominator is the TERMINAL-level regrowth threshold ({@link #getSpillRegrowthThreshold()}), not the
   * current growth level's. Dividing by the current level's threshold produces a sawtooth: it resets to ~0.5 on every
   * doubling and climbs back to ~1.0 before the next, so the lifetime max pins near {@code (N-1)/N} for the highest
   * level reached — regardless of how many doublings of headroom remain before the real spill. Because
   * {@code maxSizeForBuckets} is linear in bucket count and {@code size} only grows (preserved across rehash), a fixed
   * terminal denominator makes the ratio monotonic in {@code size}: it rises smoothly 0→1 and lands on exactly 1.0 at
   * the true spill trigger.</p>
   *
   * <p>The ratio is captured strictly while {@code size < regrowthThreshold} (the current level's). At intermediate
   * growth boundaries {@code size} hits {@code regrowthThreshold} transiently and is skipped here; the arena still has
   * room to grow, so the next mutation triggers {@link #adjustTableWhenFull()} and recording resumes against the fixed
   * terminal denominator. At the TERMINAL growth level, guarded by {@link #isTerminalTableLevel()}, no further growth
   * is possible, so parking at {@code size == regrowthThreshold} IS the spill point and 1.0 is recorded (this equals
   * {@code size / terminalRegrowthThreshold} there, since the two thresholds coincide). The other definitive spill
   * trigger, where 1.0 is also pinned, is in {@link #findBucketWithAutoGrowth} when a bucket rejection actually
   * occurs.</p>
   */
  protected void updateMaxMergeBufferUsedBytes()
  {
    maxMergeBufferUsedBytes = Math.max(maxMergeBufferUsedBytes, (long) size * bucketSizeWithHash);
    if (!recordsFillProximity()) {
      // This table trims-and-swaps to stay in memory rather than spilling to disk (the alternating limit-pushdown
      // table). Its active sub-buffer fills to regrowthThreshold on every swap, so the fill ratio would saturate near
      // 1.0 without the table ever approaching a spill. Suppress fill-proximity recording entirely; the only genuine
      // spill signal for such a table is an explicit bucket rejection in findBucketWithAutoGrowth, which pins 1.0 there
      // unconditionally.
      return;
    }
    final int denominator = getSpillRegrowthThreshold();
    if (denominator <= 0) {
      return;
    }
    if (size < regrowthThreshold) {
      // Clamped defensively; size < regrowthThreshold <= terminalRegrowthThreshold keeps this strictly below 1.0.
      final double ratio = Math.min(1.0, (double) size / denominator);
      if (ratio > maxSpillProximity) {
        maxSpillProximity = ratio;
      }
    } else if (isTerminalTableLevel()) {
      // Table is at the load-factor limit and no further growth is possible: functionally the spill point.
      maxSpillProximity = 1.0;
    }
  }

  /**
   * The denominator for {@link #maxSpillProximity}: the {@code regrowthThreshold} at the terminal growth level — the
   * bucket-count ceiling at which {@link #findBucketWithAutoGrowth} can no longer allocate a new bucket and the caller
   * spills. Computed once from the table's fixed geometry (see {@link #computeSpillRegrowthThreshold()}) and cached.
   */
  protected final int getSpillRegrowthThreshold()
  {
    if (spillRegrowthThreshold == 0) {
      spillRegrowthThreshold = computeSpillRegrowthThreshold();
    }
    return spillRegrowthThreshold;
  }

  /**
   * Computes the terminal-level {@code regrowthThreshold} by replaying the arena geometry of {@link #reset()} and
   * {@link #adjustTableWhenFull()} — without allocating any buffers. The table starts at some bucket count and grows
   * by doubling upward through the arena; when there is no more room upward it wraps to {@code tableStart == 0} and
   * consumes the whole lower region as its final level. This method walks that same sequence to the terminal level
   * and returns {@link #maxSizeForBuckets} of the terminal bucket count. Overridden by fixed-size variants (e.g. the
   * alternating heap-trim table) whose regrowth threshold never changes.
   */
  protected int computeSpillRegrowthThreshold()
  {
    int buckets = Math.min(tableArenaSize / bucketSizeWithHash, initialBuckets);
    int start = tableArenaSize - buckets * bucketSizeWithHash;

    // Replay reset()'s initial placement: walk the smallest table down so successive doublings stack above it.
    int nextBuckets = buckets * 2;
    while (true) {
      final long nextBucketsSize = (long) nextBuckets * bucketSizeWithHash;
      if (nextBucketsSize > Integer.MAX_VALUE) {
        break;
      }
      final int nextTableStart = start - nextBuckets * bucketSizeWithHash;
      if (nextTableStart > tableArenaSize / 2) {
        start = nextTableStart;
        nextBuckets = nextBuckets * 2;
      } else {
        break;
      }
    }
    if (start < tableArenaSize / 2) {
      start = 0;
    }

    // Replay adjustTableWhenFull()'s growth until the terminal level (start == 0) is reached.
    while (start != 0) {
      if (((long) buckets * 3 * bucketSizeWithHash) > (long) tableArenaSize - start) {
        // Not enough room to grow upward: wrap to zero and consume the whole lower region.
        buckets = start / bucketSizeWithHash;
        start = 0;
      } else {
        // tableBuffer.limit() == buckets * bucketSizeWithHash for the current level.
        start = start + buckets * bucketSizeWithHash;
        buckets = buckets * 2;
      }
    }
    return maxSizeForBuckets(buckets);
  }

  /**
   * True when the current table level cannot be enlarged any further. Base implementation: {@link #tableStart} has
   * reached the front of the arena, matching {@link #adjustTableWhenFull()}'s early-return. Overridden by
   * hash-table variants (e.g. the alternating heap-trim variant in {@code LimitedBufferHashGrouper}) whose
   * "table full" event is NOT a spill trigger — those return false so proximity is only pinned via the explicit
   * spill path in {@link #findBucketWithAutoGrowth}.
   */
  protected boolean isTerminalTableLevel()
  {
    return tableStart == 0;
  }

  /**
   * Whether this table's fill ratio ({@code size / terminalRegrowthThreshold}) is a meaningful proximity-to-spill
   * signal. True for the base grow-by-doubling table, which spills to disk once it can no longer grow. Overridden to
   * false by variants that trim-and-swap to stay in memory (the alternating limit-pushdown table): their active
   * sub-buffer fills to {@code regrowthThreshold} on every swap, so the ratio would saturate near 1.0 for any
   * high-cardinality limit-pushdown query even though no spill is approaching. For those tables only an explicit bucket
   * rejection in {@link #findBucketWithAutoGrowth} — the real, and only, spill trigger — sets proximity to 1.0.
   */
  protected boolean recordsFillProximity()
  {
    return true;
  }

  public long getMaxMergeBufferUsedBytes()
  {
    return maxMergeBufferUsedBytes;
  }

  /**
   * Peak {@code size / terminalRegrowthThreshold} observed over this table's lifetime, in [0.0, 1.0], where the
   * denominator is the FIXED terminal-level bucket-count ceiling (see {@link #getSpillRegrowthThreshold()}). This means
   * the value is the fraction of the way to the real spill trigger: it rises monotonically with {@code size} and equals
   * 1.0 exactly at the spill point (either {@code size} reaching the terminal threshold, or a new-bucket rejection in
   * {@link #findBucketWithAutoGrowth}). It is bucket-count based, so independent of bucket width, offset-list overhead,
   * and integer truncation in the arena-size calculation. Preserved across {@link #reset()} so a grouper that already
   * spilled retains the 1.0 peak even after {@code size} returns to 0. Ordinary table growth (when
   * {@link #adjustTableWhenFull()} enlarges {@code regrowthThreshold} instead of triggering a spill) does not push this
   * to 1.0.
   */
  public double getMaxSpillProximity()
  {
    return maxSpillProximity;
  }

  public interface BucketUpdateHandler
  {
    void handleNewBucket(int bucketOffset);

    void handlePreTableSwap();

    void handleBucketMove(int oldBucketOffset, int newBucketOffset, ByteBuffer oldBuffer, ByteBuffer newBuffer);
  }
}
