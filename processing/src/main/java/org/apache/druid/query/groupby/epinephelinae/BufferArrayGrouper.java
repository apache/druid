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
import com.google.common.primitives.Ints;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;
import org.apache.druid.query.groupby.epinephelinae.column.GroupByColumnSelectorStrategy;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
public class BufferArrayGrouper implements VectorGrouper, IntGrouper
{
  private final Supplier<ByteBuffer> bufferSupplier;
  private final AggregatorAdapters aggregators;
  private final int cardinalityWithMissingValue;
  private final int recordSize; // size of all aggregated values

  private boolean initialized = false;
  private WritableMemory usedFlagMemory;
  private ByteBuffer valBuffer;

  // Scratch objects used by aggregateVector(). Only set if initVectorized() is called.
  @Nullable
  private int[] vAggregationPositions = null;
  @Nullable
  private int[] vAggregationRows = null;

  /**
   * Computes required buffer capacity for a grouping key of the given cardinaltiy and aggregatorFactories.
   * This method assumes that the given cardinality doesn't count nulls.
   *
   * Returns -1 if cardinality + 1 (for null) > Integer.MAX_VALUE. Returns computed required buffer capacity
   * otherwise.
   */
  public static long requiredBufferCapacity(int cardinality, AggregatorFactory[] aggregatorFactories)
  {
    final long cardinalityWithMissingValue = computeCardinalityWithMissingValue(cardinality);
    // Cardinality should be in the integer range. See DimensionDictionarySelector.
    if (cardinalityWithMissingValue > Integer.MAX_VALUE) {
      return -1;
    }
    final long recordSize = Arrays.stream(aggregatorFactories)
                                  .mapToLong(AggregatorFactory::getMaxIntermediateSizeWithNulls)
                                  .sum();

    return getUsedFlagBufferCapacity(cardinalityWithMissingValue) +  // total used flags size
           cardinalityWithMissingValue * recordSize;                 // total values size
  }

  private static long computeCardinalityWithMissingValue(int cardinality)
  {
    return (long) cardinality + 1;
  }

  /**
   * Compute the number of bytes to store all used flag bits.
   */
  private static long getUsedFlagBufferCapacity(long cardinalityWithMissingValue)
  {
    return (cardinalityWithMissingValue + Byte.SIZE - 1) / Byte.SIZE;
  }

  public BufferArrayGrouper(
      // the buffer returned from the below supplier can have dirty bits and should be cleared during initialization
      final Supplier<ByteBuffer> bufferSupplier,
      final AggregatorAdapters aggregators,
      final int cardinality
  )
  {
    Preconditions.checkNotNull(aggregators, "aggregators");
    Preconditions.checkArgument(cardinality > 0, "Cardinality must a non-zero positive number");

    this.bufferSupplier = Preconditions.checkNotNull(bufferSupplier, "bufferSupplier");
    this.aggregators = aggregators;
    this.cardinalityWithMissingValue = Ints.checkedCast(computeCardinalityWithMissingValue(cardinality));
    this.recordSize = aggregators.spaceNeeded();
  }

  @Override
  public void init()
  {
    if (!initialized) {
      final ByteBuffer buffer = bufferSupplier.get();

      final int usedFlagBufferEnd = Ints.checkedCast(getUsedFlagBufferCapacity(cardinalityWithMissingValue));

      // Sanity check on buffer capacity.
      if (usedFlagBufferEnd + (long) cardinalityWithMissingValue * recordSize > buffer.capacity()) {
        // Should not happen in production, since we should only select array-based aggregation if we have
        // enough scratch space.
        throw new ISE(
            "Records of size[%,d] and possible cardinality[%,d] exceeds the buffer capacity[%,d].",
            recordSize,
            cardinalityWithMissingValue,
            valBuffer.capacity()
        );
      }

      // Slice up the buffer.
      buffer.position(0);
      buffer.limit(usedFlagBufferEnd);
      usedFlagMemory = WritableMemory.writableWrap(buffer.slice(), ByteOrder.nativeOrder());

      buffer.position(usedFlagBufferEnd);
      buffer.limit(buffer.capacity());
      valBuffer = buffer.slice();

      reset();

      initialized = true;
    }
  }

  @Override
  public void initVectorized(final int maxVectorSize)
  {
    init();

    this.vAggregationPositions = new int[maxVectorSize];
    this.vAggregationRows = new int[maxVectorSize];
  }

  @Override
  public boolean isInitialized()
  {
    return initialized;
  }

  @Override
  public AggregateResult aggregateKeyHash(final int dimIndex)
  {
    Preconditions.checkArgument(
        dimIndex >= 0 && dimIndex < cardinalityWithMissingValue,
        "Invalid dimIndex[%s]",
        dimIndex
    );

    initializeSlotIfNeeded(dimIndex);
    aggregators.aggregateBuffered(valBuffer, dimIndex * recordSize);
    return AggregateResult.ok();
  }

  @Override
  public AggregateResult aggregateVector(Memory keySpace, int startRow, int endRow)
  {
    final int numRows = endRow - startRow;

    // Hoisted bounds check on keySpace.
    if (keySpace.getCapacity() < (long) numRows * Integer.BYTES) {
      throw new IAE("Not enough keySpace capacity for the provided start/end rows");
    }

    // We use integer indexes into the keySpace.
    if (keySpace.getCapacity() > Integer.MAX_VALUE) {
      throw new ISE("keySpace too large to handle");
    }

    if (vAggregationPositions == null || vAggregationRows == null) {
      throw new ISE("Grouper was not initialized for vectorization");
    }

    if (keySpace.getCapacity() == 0) {
      // Empty key space, assume keys are all zeroes.
      final int dimIndex = 1;

      initializeSlotIfNeeded(dimIndex);

      aggregators.aggregateVector(
          valBuffer,
          dimIndex * recordSize,
          startRow,
          endRow
      );
    } else {
      for (int i = 0; i < numRows; i++) {
        // +1 matches what hashFunction() would do.
        final int dimIndex = keySpace.getInt(((long) i) * Integer.BYTES) + 1;

        if (dimIndex < 0 || dimIndex >= cardinalityWithMissingValue) {
          throw new IAE("Invalid dimIndex[%s]", dimIndex);
        }

        vAggregationPositions[i] = dimIndex * recordSize;

        initializeSlotIfNeeded(dimIndex);
      }

      aggregators.aggregateVector(
          valBuffer,
          numRows,
          vAggregationPositions,
          Groupers.writeAggregationRows(vAggregationRows, startRow, endRow)
      );
    }

    return AggregateResult.ok();
  }

  private void initializeSlotIfNeeded(int dimIndex)
  {
    final int index = dimIndex / Byte.SIZE;
    final int extraIndex = dimIndex % Byte.SIZE;
    final int usedFlagMask = 1 << extraIndex;

    final byte currentByte = usedFlagMemory.getByte(index);

    if ((currentByte & usedFlagMask) == 0) {
      usedFlagMemory.putByte(index, (byte) (currentByte | usedFlagMask));
      aggregators.init(valBuffer, dimIndex * recordSize);
    }
  }

  private boolean isUsedSlot(int dimIndex)
  {
    final int index = dimIndex / Byte.SIZE;
    final int extraIndex = dimIndex % Byte.SIZE;
    final int usedFlagMask = 1 << extraIndex;

    return (usedFlagMemory.getByte(index) & usedFlagMask) != 0;
  }

  @Override
  public void reset()
  {
    // Clear the entire usedFlagBuffer
    usedFlagMemory.clear();
  }

  @Override
  public IntGrouperHashFunction hashFunction()
  {
    return key -> key + 1;
  }

  @Override
  public void close()
  {
    aggregators.close();
  }

  @Override
  public CloseableIterator<Entry<MemoryPointer>> iterator()
  {
    final CloseableIterator<Entry<IntKey>> iterator = iterator(false);
    final WritableMemory keyMemory = WritableMemory.allocate(Integer.BYTES);
    final MemoryPointer reusableKey = new MemoryPointer(keyMemory, 0);
    final ReusableEntry<MemoryPointer> reusableEntry = new ReusableEntry<>(reusableKey, new Object[aggregators.size()]);

    return new CloseableIterator<Entry<MemoryPointer>>()
    {
      @Override
      public boolean hasNext()
      {
        return iterator.hasNext();
      }

      @Override
      public Entry<MemoryPointer> next()
      {
        final Entry<IntKey> integerEntry = iterator.next();
        keyMemory.putInt(0, integerEntry.getKey().intValue());
        reusableEntry.setValues(integerEntry.getValues());
        return reusableEntry;
      }

      @Override
      public void close() throws IOException
      {
        iterator.close();
      }
    };
  }

  @Override
  public CloseableIterator<Entry<IntKey>> iterator(boolean sorted)
  {
    if (sorted) {
      throw new UnsupportedOperationException("sorted iterator is not supported yet");
    }

    return new CloseableIterator<Entry<IntKey>>()
    {
      final ReusableEntry<IntKey> reusableEntry =
          new ReusableEntry<>(new IntKey(0), new Object[aggregators.size()]);

      // initialize to the first used slot
      private int next = findNext(-1);

      @Override
      public boolean hasNext()
      {
        return next >= 0;
      }

      @Override
      public Entry<IntKey> next()
      {
        if (next < 0) {
          throw new NoSuchElementException();
        }

        final int current = next;
        next = findNext(current);

        final int recordOffset = current * recordSize;
        for (int i = 0; i < aggregators.size(); i++) {
          reusableEntry.getValues()[i] = aggregators.get(valBuffer, recordOffset, i);
        }
        // shift by -1 since values are initially shifted by +1 so they are all positive and
        // GroupByColumnSelectorStrategy.GROUP_BY_MISSING_VALUE is -1
        reusableEntry.getKey().setValue(current - 1);
        return reusableEntry;
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
