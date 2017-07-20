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
import com.google.common.primitives.Ints;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;

import java.nio.ByteBuffer;

public abstract class AbstractBufferGrouper<KeyType> implements Grouper<KeyType>
{
  private static final AggregateResult DICTIONARY_FULL = AggregateResult.failure(
      "Not enough dictionary space to execute this query. Try increasing "
      + "druid.query.groupBy.maxMergingDictionarySize or enable disk spilling by setting "
      + "druid.query.groupBy.maxOnDiskStorage to a positive number."
  );
  private static final AggregateResult HASHTABLE_FULL = AggregateResult.failure(
      "Not enough aggregation table space to execute this query. Try increasing "
      + "druid.processing.buffer.sizeBytes or enable disk spilling by setting "
      + "druid.query.groupBy.maxOnDiskStorage to a positive number."
  );

  protected static final int HASH_SIZE = Ints.BYTES;
  protected static final Logger log = new Logger(AbstractBufferGrouper.class);

  protected final Supplier<ByteBuffer> bufferSupplier;
  protected final KeySerde<KeyType> keySerde;
  protected final int keySize;
  protected final BufferAggregator[] aggregators;
  protected final int[] aggregatorOffsets;
  protected final int bufferGrouperMaxSize; // Integer.MAX_VALUE in production, only used for unit tests

  // The load factor and bucket configurations are not final, to allow subclasses to set their own values
  protected float maxLoadFactor;
  protected int initialBuckets;
  protected int bucketSize;

  // The hashTable and its buffer are not final, these are set during init() for buffer management purposes
  // See PR 3863 for details: https://github.com/druid-io/druid/pull/3863
  protected ByteBufferHashTable hashTable;
  protected ByteBuffer hashTableBuffer; // buffer for the entire hash table (total space, not individual growth)

  public AbstractBufferGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final AggregatorFactory[] aggregatorFactories,
      final int bufferGrouperMaxSize
  )
  {
    this.bufferSupplier = bufferSupplier;
    this.keySerde = keySerde;
    this.keySize = keySerde.keySize();
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];
    this.bufferGrouperMaxSize = bufferGrouperMaxSize;
  }

  /**
   * Called when a new bucket is used for an entry in the hash table. An implementing BufferGrouper class
   * can use this to update its own state, e.g. tracking bucket offsets in a structure outside of the hash table.
   *
   * @param bucketOffset offset of the new bucket, within the buffer returned by hashTable.getTableBuffer()
   */
  public abstract void newBucketHook(int bucketOffset);

  /**
   * Called to check if it's possible to skip aggregation for a row.
   *
   * @param bucketWasUsed Was the row a new entry in the hash table?
   * @param bucketOffset Offset of the bucket containing this row's entry in the hash table,
   *                     within the buffer returned by hashTable.getTableBuffer()
   * @return true if aggregation can be skipped, false otherwise.
   */
  public abstract boolean canSkipAggregate(boolean bucketWasUsed, int bucketOffset);

  /**
   * Called after a row is aggregated. An implementing BufferGrouper class can use this to update
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
      return DICTIONARY_FULL;
    }

    if (keyBuffer.remaining() != keySize) {
      throw new IAE(
          "keySerde.toByteBuffer(key).remaining[%s] != keySerde.keySize[%s], buffer was the wrong size?!",
          keyBuffer.remaining(),
          keySize
      );
    }

    // find and try to expand if table is full and find again
    int bucket = hashTable.findBucketWithAutoGrowth(keyBuffer, keyHash);
    if (bucket < 0) {
      // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message will
      // be correct.
      return HASHTABLE_FULL;
    }

    final int bucketStartOffset = hashTable.getOffsetForBucket(bucket);
    final boolean bucketWasUsed = hashTable.isBucketUsed(bucket);
    final ByteBuffer tableBuffer = hashTable.getTableBuffer();

    // Set up key and initialize the aggs if this is a new bucket.
    if (!bucketWasUsed) {
      hashTable.initializeNewBucketKey(bucket, keyBuffer, keyHash);
      for (int i = 0; i < aggregators.length; i++) {
        aggregators[i].init(tableBuffer, bucketStartOffset + aggregatorOffsets[i]);
      }

      newBucketHook(bucketStartOffset);
    }

    if (canSkipAggregate(bucketWasUsed, bucketStartOffset)) {
      return AggregateResult.ok();
    }

    // Aggregate the current row.
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].aggregate(tableBuffer, bucketStartOffset + aggregatorOffsets[i]);
    }

    afterAggregateHook(bucketStartOffset);

    return AggregateResult.ok();
  }

  @Override
  public AggregateResult aggregate(final KeyType key)
  {
    return aggregate(key, Groupers.hash(key));
  }

  @Override
  public void close()
  {
    for (BufferAggregator aggregator : aggregators) {
      try {
        aggregator.close();
      }
      catch (Exception e) {
        log.warn(e, "Could not close aggregator [%s], skipping.", aggregator);
      }
    }
  }

  protected Entry<KeyType> bucketEntryForOffset(final int bucketOffset)
  {
    final ByteBuffer tableBuffer = hashTable.getTableBuffer();
    final KeyType key = keySerde.fromByteBuffer(tableBuffer, bucketOffset + HASH_SIZE);
    final Object[] values = new Object[aggregators.length];
    for (int i = 0; i < aggregators.length; i++) {
      values[i] = aggregators[i].get(tableBuffer, bucketOffset + aggregatorOffsets[i]);
    }

    return new Entry<>(key, values);
  }
}
