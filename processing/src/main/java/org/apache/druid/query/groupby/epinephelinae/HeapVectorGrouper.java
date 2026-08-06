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

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import it.unimi.dsi.fastutil.objects.Object2IntOpenCustomHashMap;
import org.apache.datasketches.memory.Memory;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.query.aggregation.AggregatorAdapters;
import org.apache.druid.query.groupby.epinephelinae.collection.MemoryPointer;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Iterator;

/**
 * On-heap {@link VectorGrouper} that grows aggregator state on demand, up to maximum limit of 2GB.
 *
 * Vectorized analogue of {@link org.apache.druid.query.topn.BaseTopNAlgorithm}'s
 * {@code runWithCardinalityUnknown} path: used when dimension cardinality is unknown (numeric columns,
 * non-dict-encoded string virtual columns) or when a dict-encoded string column's cardinality exceeds
 * the processing buffer. Memory footprint is on-heap and grows with the distinct-key count — matching
 * the non-vectorized path's memory profile for the same queries.
 */
public class HeapVectorGrouper implements VectorGrouper
{
  private static final Hash.Strategy<byte[]> BYTE_ARRAY_HASH_STRATEGY = new Hash.Strategy<byte[]>()
  {
    @Override
    public int hashCode(byte[] o)
    {
      return Arrays.hashCode(o);
    }

    @Override
    public boolean equals(byte[] a, byte[] b)
    {
      return Arrays.equals(a, b);
    }
  };

  private static final int MIN_INITIAL_STATE_BUFFER_SIZE = 4096;

  private final AggregatorAdapters aggregators;
  private final int keySize;
  private final int aggStateSize;
  private final Object2IntOpenCustomHashMap<byte[]> keyToOffset;

  private boolean initialized;
  private ByteBuffer aggStateBuffer;
  private int aggStateEnd;

  private int[] vAggregationPositions;
  private int[] vAggregationRows;
  private byte[] keyScratch;

  public HeapVectorGrouper(final AggregatorAdapters aggregators, final int keySize)
  {
    this.aggregators = aggregators;
    this.keySize = keySize;
    this.aggStateSize = aggregators.spaceNeeded();
    this.keyToOffset = new Object2IntOpenCustomHashMap<>(BYTE_ARRAY_HASH_STRATEGY);
    this.keyToOffset.defaultReturnValue(-1);
  }

  @Override
  public void initVectorized(final int maxVectorSize)
  {
    if (initialized) {
      if (vAggregationPositions.length != maxVectorSize) {
        throw new ISE(
            "initVectorized called with different maxVectorSize (existing=%d, new=%d)",
            vAggregationPositions.length,
            maxVectorSize
        );
      }
      return;
    }
    this.aggStateBuffer = ByteBuffer.allocate(MIN_INITIAL_STATE_BUFFER_SIZE);
    this.vAggregationPositions = new int[maxVectorSize];
    this.vAggregationRows = new int[maxVectorSize];
    this.keyScratch = new byte[keySize];
    this.aggStateEnd = 0;
    this.initialized = true;
  }

  /**
   * Contract: keys for rows [startRow, endRow) must be packed contiguously at {@code keySpace[0 ..
   * numRows * keySize)}; {@code startRow}/{@code endRow} are source-vector indices used to look up aggregator
   * input values.
   */
  @Override
  public AggregateResult aggregateVector(final Memory keySpace, final int startRow, final int endRow)
  {
    final int numRows = endRow - startRow;

    for (int i = 0; i < numRows; i++) {
      keySpace.getByteArray((long) i * keySize, keyScratch, 0, keySize);
      int offset = keyToOffset.getInt(keyScratch);
      if (offset == -1) {
        if ((long) aggStateEnd + aggStateSize > aggStateBuffer.capacity()) {
          growBuffer((long) aggStateEnd + aggStateSize);
        }
        offset = aggStateEnd;
        final byte[] keyCopy = Arrays.copyOf(keyScratch, keySize);
        keyToOffset.put(keyCopy, offset);
        aggregators.init(aggStateBuffer, offset);
        aggStateEnd += aggStateSize;
      }
      vAggregationPositions[i] = offset;
    }

    aggregators.aggregateVector(
        aggStateBuffer,
        numRows,
        vAggregationPositions,
        Groupers.writeAggregationRows(vAggregationRows, startRow, endRow)
    );

    return AggregateResult.ok();
  }

  private void growBuffer(final long neededCapacity)
  {
    if (neededCapacity > Integer.MAX_VALUE) {
      throw new ISE("Aggregator state exceeds 2 GB; cardinality too high for HeapVectorGrouper");
    }
    int newCapacity = aggStateBuffer.capacity();
    while (newCapacity < neededCapacity) {
      final long doubled = (long) newCapacity * 2;
      newCapacity = doubled > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) doubled;
    }

    final ByteBuffer oldBuffer = aggStateBuffer;
    final ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
    oldBuffer.position(0);
    oldBuffer.limit(aggStateEnd);
    newBuffer.put(oldBuffer);

    for (int pos = 0; pos < aggStateEnd; pos += aggStateSize) {
      aggregators.relocate(pos, pos, oldBuffer, newBuffer);
    }

    this.aggStateBuffer = newBuffer;
  }

  @Override
  public void reset()
  {
    aggregators.reset();
    keyToOffset.clear();
    aggStateEnd = 0;
  }

  @Override
  public void close()
  {
    reset();
  }

  @Override
  public CloseableIterator<Grouper.Entry<MemoryPointer>> iterator()
  {
    final Iterator<Object2IntMap.Entry<byte[]>> mapIter =
        keyToOffset.object2IntEntrySet().fastIterator();

    return new CloseableIterator<>()
    {
      final ReusableEntry<MemoryPointer> reusableEntry =
          new ReusableEntry<>(new MemoryPointer(), new Object[aggregators.size()]);

      @Override
      public boolean hasNext()
      {
        return mapIter.hasNext();
      }

      @Override
      public Grouper.Entry<MemoryPointer> next()
      {
        final Object2IntMap.Entry<byte[]> mapEntry = mapIter.next();
        reusableEntry.getKey().set(Memory.wrap(mapEntry.getKey(), ByteOrder.nativeOrder()), 0);

        final int position = mapEntry.getIntValue();
        for (int i = 0; i < aggregators.size(); i++) {
          reusableEntry.getValues()[i] = aggregators.get(aggStateBuffer, position, i);
        }
        return reusableEntry;
      }

      @Override
      public void close()
      {
        // Nothing to close.
      }
    };
  }
}
