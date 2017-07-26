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
import com.google.common.base.Supplier;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.query.groupby.epinephelinae.column.StringGroupByColumnSelectorStrategy;
import io.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Iterator;
import java.util.stream.IntStream;

/**
 * A buffer grouper for array-based aggregation.  This grouper stores aggregated values in the buffer using the grouping
 * key as the index.  To do so, the grouping key always has the integer type.
 * <p>
 * The buffer is divided into 3 separate regions, i.e., key buffer, used flag buffer, and value buffer.  The key buffer
 * is used to temporaily store a single key when deserializing the key using {@link Grouper.KeySerde} in
 * {@link #iterator(boolean)}.  The used flag buffer is a bit set to represent which keys are valid.  If a bit of an
 * index is set, that key is valid.  Finally, the value buffer is used to store aggregated values.  The first index is
 * reserved for {@link StringGroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE}.
 * <p>
 * This grouper is available only when the grouping key is a single indexed dimension of a known cardinality because it
 * directly uses the dimension value as the index for array access.  Since the cardinality for the grouping key across
 * different segments cannot be currently retrieved, this grouper can be used only when performing per-segment query
 * execution.
 * <p>
 * Since the index directly represents the grouping key, the grouping key type is always the integer.  However, the
 * {@link KeyType} parameter is preserved because this grouper will be used for merging per-segment aggregation
 * results with a proper cardinality computation in the future.
 */
public class BufferArrayGrouper<KeyType> implements Grouper<KeyType>
{
  private static final Logger LOG = new Logger(BufferArrayGrouper.class);

  private final Supplier<ByteBuffer> bufferSupplier;
  private final KeySerde<KeyType> keySerde;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int cardinalityWithMissingValue;
  private final int recordSize; // size of all aggregated values

  private boolean initialized = false;
  private ByteBuffer keyBuffer;
  private ByteBuffer usedFlagBuffer;
  private ByteBuffer valBuffer;

  static <KeyType> int requiredBufferCapacity(
      KeySerde<KeyType> keySerde,
      int cardinality,
      AggregatorFactory[] aggregatorFactories
  )
  {
    final int cardinalityWithMissingValue = cardinality + 1;
    final int recordSize = Arrays.stream(aggregatorFactories)
                                 .mapToInt(AggregatorFactory::getMaxIntermediateSize)
                                 .sum();

    return keySerde.keySize() +                                         // key size
           (int) Math.ceil((double) cardinalityWithMissingValue / 8) +  // total used flags size
           cardinalityWithMissingValue * recordSize;                    // total values size
  }

  public BufferArrayGrouper(
      final Supplier<ByteBuffer> bufferSupplier,
      final KeySerde<KeyType> keySerde,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int cardinality
  )
  {
    Preconditions.checkNotNull(aggregatorFactories, "aggregatorFactories");
    Preconditions.checkArgument(cardinality > 0, "Cardinality must a non-zero positive number");

    this.bufferSupplier = Preconditions.checkNotNull(bufferSupplier, "bufferSupplier");
    this.keySerde = Preconditions.checkNotNull(keySerde, "keySerde");
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];
    this.cardinalityWithMissingValue = cardinality + 1;

    int offset = 0;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSize();
    }
    recordSize = offset;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      final ByteBuffer buffer = bufferSupplier.get().duplicate();

      buffer.position(0);
      buffer.limit(keySerde.keySize());
      keyBuffer = buffer.slice();

      final int usedBufferEnd = keySerde.keySize() + (int) Math.ceil((double) cardinalityWithMissingValue / 8);
      buffer.position(keySerde.keySize());
      buffer.limit(usedBufferEnd);
      usedFlagBuffer = buffer.slice();

      buffer.position(usedBufferEnd);
      buffer.limit(buffer.capacity());
      valBuffer = buffer.slice();

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
  public AggregateResult aggregate(KeyType key)
  {
    // BufferArrayGrouper is used only for dictionary-indexed single-value string dimensions.
    // Here, the key contains the dictionary-encoded value of the grouping key
    // and we use it as the index for the aggregation array.

    final ByteBuffer fromKey = checkAndGetKeyBuffer(keySerde, key);
    if (fromKey == null) {
      // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message will
      // be correct.
      return Groupers.DICTIONARY_FULL;
    }

    // The first index is reserved for missing value which is represented by -1.
    final int dimIndex = fromKey.getInt() + 1;
    fromKey.rewind();
    return aggregate(key, dimIndex);
  }

  @Override
  public AggregateResult aggregate(KeyType key, int dimIndex)
  {
    Preconditions.checkArgument(
        dimIndex >= 0 && dimIndex < cardinalityWithMissingValue,
        "Invalid dimIndex[%s]",
        dimIndex
    );

    final ByteBuffer fromKey = checkAndGetKeyBuffer(keySerde, key);
    if (fromKey == null) {
      // This may just trigger a spill and get ignored, which is ok. If it bubbles up to the user, the message will
      // be correct.
      return Groupers.DICTIONARY_FULL;
    }

    final int recordOffset = dimIndex * recordSize;

    if (recordOffset + recordSize > valBuffer.capacity()) {
      // This error cannot be recoverd, and the query must fail
      throw new ISE(
          "A record of size [%d] cannot be written to the array buffer at offset[%d] "
          + "because it exceeds the buffer capacity[%d]. Try increasing druid.processing.buffer.sizeBytes",
          recordSize,
          recordOffset,
          valBuffer.capacity()
      );
    }

    if (!isUsedSlot(dimIndex)) {
      initializeSlot(dimIndex);
    }

    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].aggregate(valBuffer, recordOffset + aggregatorOffsets[i]);
    }

    return AggregateResult.ok();
  }

  private ByteBuffer checkAndGetKeyBuffer(KeySerde<KeyType> keySerde, KeyType key)
  {
    final ByteBuffer fromKey = keySerde.toByteBuffer(key);
    if (fromKey == null) {
      return null;
    }

    if (fromKey.remaining() != keySerde.keySize()) {
      throw new IAE(
          "keySerde.toByteBuffer(key).remaining[%s] != keySerde.keySize[%s], buffer was the wrong size?!",
          fromKey.remaining(),
          keySerde.keySize()
      );
    }
    return fromKey;
  }

  private void initializeSlot(int dimIndex)
  {
    final int index = dimIndex / 8;
    final int extraIndex = dimIndex % 8;
    usedFlagBuffer.put(index, (byte) (usedFlagBuffer.get(index) | 1 << extraIndex));

    final int recordOffset = dimIndex * recordSize;
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].init(valBuffer, recordOffset + aggregatorOffsets[i]);
    }
  }

  private boolean isUsedSlot(int dimIndex)
  {
    final int index = dimIndex / 8;
    final int extraIndex = dimIndex % 8;
    final int usedByte = 1 << extraIndex;
    return (usedFlagBuffer.get(index) & usedByte) == usedByte;
  }

  @Override
  public void reset()
  {
    final int usedBufferCapacity = usedFlagBuffer.capacity();
    for (int i = 0; i < usedBufferCapacity; i++) {
      usedFlagBuffer.put(i, (byte) 0);
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
        LOG.warn(e, "Could not close aggregator [%s], skipping.", aggregator);
      }
    }
  }

  @Override
  public Iterator<Entry<KeyType>> iterator(boolean sorted)
  {
    // result is always natually sorted by keys
    return IntStream.range(0, cardinalityWithMissingValue)
                    .filter(this::isUsedSlot)
                    .mapToObj(index -> {
                      keyBuffer.putInt(0, index - 1); // Restore key values from the index

                      final Object[] values = new Object[aggregators.length];
                      final int recordOffset = index * recordSize;
                      for (int i = 0; i < aggregators.length; i++) {
                        values[i] = aggregators[i].get(valBuffer, recordOffset + aggregatorOffsets[i]);
                      }
                      return new Entry<>(keySerde.fromByteBuffer(keyBuffer, 0), values);
                    }).iterator();
  }
}
