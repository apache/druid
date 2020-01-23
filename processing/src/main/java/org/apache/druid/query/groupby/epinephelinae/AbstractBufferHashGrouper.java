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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.aggregation.AggregatorAdapters;

import java.nio.ByteBuffer;

public abstract class AbstractBufferHashGrouper<KeyType> implements Grouper<KeyType>
{
  protected static final int HASH_SIZE = Integer.BYTES;
  protected static final Logger log = new Logger(AbstractBufferHashGrouper.class);

  protected final Supplier<ByteBuffer> bufferSupplier;
  protected final KeySerde<KeyType> keySerde;
  protected final int keySize;
  protected final AggregatorAdapters aggregators;
  protected final int baseAggregatorOffset;
  protected final int bufferGrouperMaxSize; // Integer.MAX_VALUE in production, only used for unit tests

  // The load factor and bucket configurations are not final, to allow subclasses to set their own values
  protected float maxLoadFactor;
  protected int initialBuckets;
  protected int bucketSize;

  // The hashTable and its buffer are not final, these are set during init() for buffer management purposes
  // See PR 3863 for details: https://github.com/apache/druid/pull/3863
  protected ByteBufferHashTable hashTable;
  protected ByteBuffer hashTableBuffer; // buffer for the entire hash table (total space, not individual growth)

  public AbstractBufferHashGrouper(
      // the buffer returned from the below supplier can have dirty bits and should be cleared during initialization
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final AggregatorAdapters aggregators,
      final int baseAggregatorOffset,
      final int bufferGrouperMaxSize
  )
  {
    this.bufferSupplier = bufferSupplier;
    this.keySerde = keySerde;
    this.keySize = keySerde.keySize();
    this.aggregators = aggregators;
    this.baseAggregatorOffset = baseAggregatorOffset;
    this.bufferGrouperMaxSize = bufferGrouperMaxSize;
  }

  /**
   * Called when a new bucket is used for an entry in the hash table. An implementing BufferHashGrouper class
   * can use this to update its own state, e.g. tracking bucket offsets in a structure outside of the hash table.
   *
   * @param bucketOffset offset of the new bucket, within the buffer returned by hashTable.getTableBuffer()
   */
  public abstract void newBucketHook(int bucketOffset);

  /**
   * Called to check if it's possible to skip aggregation for a row.
   *
   * @param bucketOffset  Offset of the bucket containing this row's entry in the hash table,
   *                      within the buffer returned by hashTable.getTableBuffer()
   *
   * @return true if aggregation can be skipped, false otherwise.
   */
  public abstract boolean canSkipAggregate(int bucketOffset);

  /**
   * Called after a row is aggregated. An implementing BufferHashGrouper class can use this to update
   * its own state, e.g. reading the new aggregated values for the row's key and acting on that information.
   *
   * @param bucketOffset Offset of the bucket containing the row that was aggregated,
   *                     within the buffer returned by hashTable.getTableBuffer()
   */
  public abstract void afterAggregateHook(int bucketOffset);

  // how many times the hash table's buffer has filled/readjusted (through adjustTableWhenFull())
  public int getGrowthCount()
  {
    return hashTable.getGrowthCount();
  }

  // Number of elements in the table right now
  public int getSize()
  {
    return hashTable.getSize();
  }

  // Current number of available/used buckets in the table
  public int getBuckets()
  {
    return hashTable.getMaxBuckets();
  }

  // Maximum number of elements in the table before it must be resized
  public int getMaxSize()
  {
    return hashTable.getRegrowthThreshold();
  }

  @Override
  public AggregateResult aggregate(KeyType key, int keyHash)
  {
    final ByteBuffer keyBuffer = keySerde.toByteBuffer(key);
    if (keyBuffer == null) {
      // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message will
      // be correct.
      return Groupers.dictionaryFull(0);
    }

    if (keyBuffer.remaining() != keySize) {
      throw new IAE(
          "keySerde.toByteBuffer(key).remaining[%s] != keySerde.keySize[%s], buffer was the wrong size?!",
          keyBuffer.remaining(),
          keySize
      );
    }

    // find and try to expand if table is full and find again
    int bucket = hashTable.findBucketWithAutoGrowth(keyBuffer, keyHash, () -> {});
    if (bucket < 0) {
      // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message will
      // be correct.
      return Groupers.hashTableFull(0);
    }

    final int bucketStartOffset = hashTable.getOffsetForBucket(bucket);
    final boolean bucketWasUsed = hashTable.isBucketUsed(bucket);
    final ByteBuffer tableBuffer = hashTable.getTableBuffer();

    // Set up key and initialize the aggs if this is a new bucket.
    if (!bucketWasUsed) {
      hashTable.initializeNewBucketKey(bucket, keyBuffer, keyHash);
      aggregators.init(tableBuffer, bucketStartOffset + baseAggregatorOffset);
      newBucketHook(bucketStartOffset);
    }

    if (canSkipAggregate(bucketStartOffset)) {
      return AggregateResult.ok();
    }

    // Aggregate the current row.
    aggregators.aggregateBuffered(tableBuffer, bucketStartOffset + baseAggregatorOffset);

    afterAggregateHook(bucketStartOffset);

    return AggregateResult.ok();
  }

  @Override
  public void close()
  {
    aggregators.close();
  }

  protected Entry<KeyType> bucketEntryForOffset(final int bucketOffset)
  {
    final ByteBuffer tableBuffer = hashTable.getTableBuffer();
    final KeyType key = keySerde.fromByteBuffer(tableBuffer, bucketOffset + HASH_SIZE);
    final Object[] values = new Object[aggregators.size()];
    for (int i = 0; i < aggregators.size(); i++) {
      values[i] = aggregators.get(tableBuffer, bucketOffset + baseAggregatorOffset, i);
    }

    return new Entry<>(key, values);
  }
}
