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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;
import org.apache.druid.segment.ColumnSelectorFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * A buffer grouper for array-based aggregation.  This grouper stores aggregated values in the buffer using the grouping
 * key as the index.
 * <p>
 * The buffer is divided into 2 separate regions, i.e., used flag buffer and value buffer.  The used flag buffer is a
 * bit set to represent which keys are valid.  If a bit of an index is set, that key is valid.  Finally, the value
 * buffer is used to store aggregated values.  The first index is reserved for
 * {@link GroupByColumnSelectorStrategy#GROUP_BY_MISSING_VALUE}.
 * <p>
 * This grouper is available only when the grouping key is a single indexed dimension of a known cardinality because it
 * directly uses the dimension value as the index for array access.  Since the cardinality for the grouping key across
 * different segments cannot be currently retrieved, this grouper can be used only when performing per-segment query
 * execution.
 */
public class BufferArrayGrouper implements IntGrouper
{
  private static final Logger LOG = new Logger(BufferArrayGrouper.class);

  private final Supplier<ByteBuffer> bufferSupplier;
  private final BufferAggregator[] aggregators;
  private final int[] aggregatorOffsets;
  private final int cardinalityWithMissingValue;
  private final int recordSize; // size of all aggregated values

  private boolean initialized = false;
  private ByteBuffer usedFlagBuffer;
  private ByteBuffer valBuffer;

  static long requiredBufferCapacity(
      int cardinality,
      AggregatorFactory[] aggregatorFactories
  )
  {
    final int cardinalityWithMissingValue = cardinality + 1;
    final int recordSize = Arrays.stream(aggregatorFactories)
                                 .mapToInt(AggregatorFactory::getMaxIntermediateSizeWithNulls)
                                 .sum();

    return getUsedFlagBufferCapacity(cardinalityWithMissingValue) +  // total used flags size
        (long) cardinalityWithMissingValue * recordSize;                 // total values size
  }

  /**
   * Compute the number of bytes to store all used flag bits.
   */
  private static int getUsedFlagBufferCapacity(int cardinalityWithMissingValue)
  {
    return (cardinalityWithMissingValue + Byte.SIZE - 1) / Byte.SIZE;
  }

  public BufferArrayGrouper(
      // the buffer returned from the below supplier can have dirty bits and should be cleared during initialization
      final Supplier<ByteBuffer> bufferSupplier,
      final ColumnSelectorFactory columnSelectorFactory,
      final AggregatorFactory[] aggregatorFactories,
      final int cardinality
  )
  {
    Preconditions.checkNotNull(aggregatorFactories, "aggregatorFactories");
    Preconditions.checkArgument(cardinality > 0, "Cardinality must a non-zero positive number");

    this.bufferSupplier = Preconditions.checkNotNull(bufferSupplier, "bufferSupplier");
    this.aggregators = new BufferAggregator[aggregatorFactories.length];
    this.aggregatorOffsets = new int[aggregatorFactories.length];
    this.cardinalityWithMissingValue = cardinality + 1;

    int offset = 0;
    for (int i = 0; i < aggregatorFactories.length; i++) {
      aggregators[i] = aggregatorFactories[i].factorizeBuffered(columnSelectorFactory);
      aggregatorOffsets[i] = offset;
      offset += aggregatorFactories[i].getMaxIntermediateSizeWithNulls();
    }
    recordSize = offset;
  }

  @Override
  public void init()
  {
    if (!initialized) {
      final ByteBuffer buffer = bufferSupplier.get();

      final int usedFlagBufferEnd = getUsedFlagBufferCapacity(cardinalityWithMissingValue);
      buffer.position(0);
      buffer.limit(usedFlagBufferEnd);
      usedFlagBuffer = buffer.slice();

      buffer.position(usedFlagBufferEnd);
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
  public AggregateResult aggregateKeyHash(int dimIndex)
  {
    Preconditions.checkArgument(
        dimIndex >= 0 && dimIndex < cardinalityWithMissingValue,
        "Invalid dimIndex[%s]",
        dimIndex
    );

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

  private void initializeSlot(int dimIndex)
  {
    final int index = dimIndex / Byte.SIZE;
    final int extraIndex = dimIndex % Byte.SIZE;
    usedFlagBuffer.put(index, (byte) (usedFlagBuffer.get(index) | (1 << extraIndex)));

    final int recordOffset = dimIndex * recordSize;
    for (int i = 0; i < aggregators.length; i++) {
      aggregators[i].init(valBuffer, recordOffset + aggregatorOffsets[i]);
    }
  }

  private boolean isUsedSlot(int dimIndex)
  {
    final int index = dimIndex / Byte.SIZE;
    final int extraIndex = dimIndex % Byte.SIZE;
    final int usedFlagByte = 1 << extraIndex;
    return (usedFlagBuffer.get(index) & usedFlagByte) != 0;
  }

  @Override
  public void reset()
  {
    // Clear the entire usedFlagBuffer
    final int usedFlagBufferCapacity = usedFlagBuffer.capacity();

    // putLong() instead of put() can boost the performance of clearing the buffer
    final int n = (usedFlagBufferCapacity / Long.BYTES) * Long.BYTES;
    for (int i = 0; i < n; i += Long.BYTES) {
      usedFlagBuffer.putLong(i, 0L);
    }

    for (int i = n; i < usedFlagBufferCapacity; i++) {
      usedFlagBuffer.put(i, (byte) 0);
    }
  }

  @Override
  public IntGrouperHashFunction hashFunction()
  {
    return key -> key + 1;
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
  public CloseableIterator<Entry<Integer>> iterator(boolean sorted)
  {
    if (sorted) {
      throw new UnsupportedOperationException("sorted iterator is not supported yet");
    }

    return new CloseableIterator<Entry<Integer>>()
    {
      // initialize to the first used slot
      private int next = findNext(-1);

      @Override
      public boolean hasNext()
      {
        return next >= 0;
      }

      @Override
      public Entry<Integer> next()
      {
        if (next < 0) {
          throw new NoSuchElementException();
        }

        final int current = next;
        next = findNext(current);

        final Object[] values = new Object[aggregators.length];
        final int recordOffset = current * recordSize;
        for (int i = 0; i < aggregators.length; i++) {
          values[i] = aggregators[i].get(valBuffer, recordOffset + aggregatorOffsets[i]);
        }
        // shift by -1 since values are initially shifted by +1 so they are all positive and
        // GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE is -1
        return new Entry<>(current - 1, values);
      }

      @Override
      public void close()
      {
        // do nothing
      }

      private int findNext(int current)
      {
        // shift by +1 since we're looking for the next used slot after the current position
        for (int i = current + 1; i < cardinalityWithMissingValue; i++) {
          if (isUsedSlot(i)) {
            return i;
          }
        }
        // no more slots
        return -1;
      }
    };
  }
}
