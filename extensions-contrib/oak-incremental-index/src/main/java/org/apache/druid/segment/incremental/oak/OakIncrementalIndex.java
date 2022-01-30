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

package org.apache.druid.segment.incremental.oak;


import com.google.common.base.Supplier;
import com.google.common.collect.Iterators;
import com.yahoo.oak.OakBuffer;
import com.yahoo.oak.OakMap;
import com.yahoo.oak.OakMapBuilder;
import com.yahoo.oak.OakScopedReadBuffer;
import com.yahoo.oak.OakScopedWriteBuffer;
import com.yahoo.oak.OakSerializer;
import com.yahoo.oak.OakUnsafeDirectBuffer;
import com.yahoo.oak.OakUnscopedBuffer;
import org.apache.druid.annotations.EverythingIsNonnullByDefault;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.incremental.AppendableIndexBuilder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRow;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.IntStream;


/**
 * OakIncrementalIndex has two main attributes that are different from the other IncrementalIndex implementations:
 * 1. It stores both **keys** and **values** off-heap (as opposed to the off-heap implementation that stores only
 *    the **values** off-heap).
 * 2. It is based on OakMap (https://github.com/yahoo/Oak) instead of Java's ConcurrentSkipList.
 * These two changes significantly reduce the number of heap-objects and thus decrease dramatically the GC's memory
 * and performance overhead.
 */
@EverythingIsNonnullByDefault
public class OakIncrementalIndex extends IncrementalIndex implements IncrementalIndex.FactsHolder
{
  private final BufferAggregator[] aggregators;
  private final OakMap<IncrementalIndexRow, OakInputRowContext> facts;

  // Given a ByteBuffer and an offset inside the buffer, offset + aggOffsetInBuffer[i]
  // would give a position in the buffer where the i^th aggregator's value is stored.
  private final int[] aggregatorOffsetInBuffer;
  private final int aggregatorsTotalSize;

  private static final Logger log = new Logger(OakIncrementalIndex.class);

  public OakIncrementalIndex(IncrementalIndexSchema incrementalIndexSchema,
                             boolean deserializeComplexMetrics,
                             boolean concurrentEventAdd,
                             int maxRowCount,
                             long maxBytesInMemory,
                             long oakMaxMemoryCapacity,
                             int oakBlockSize,
                             int oakChunkMaxItems)
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, concurrentEventAdd, maxRowCount, maxBytesInMemory);

    AggregatorFactory[] metrics = getMetricAggs();
    this.aggregators = new BufferAggregator[metrics.length];

    this.aggregatorOffsetInBuffer = new int[metrics.length];

    int curAggOffset = 0;
    for (int i = 0; i < metrics.length; i++) {
      aggregatorOffsetInBuffer[i] = curAggOffset;
      curAggOffset += metrics[i].getMaxIntermediateSizeWithNulls();
    }
    this.aggregatorsTotalSize = curAggOffset;

    final IncrementalIndexRow minRow = new IncrementalIndexRow(
        incrementalIndexSchema.getMinTimestamp(),
        OakIncrementalIndexRow.NO_DIMS,
        dimensionDescsList,
        IncrementalIndexRow.EMPTY_ROW_INDEX
    );

    this.facts = new OakMapBuilder<>(
        new OakKey.Comparator(dimensionDescsList, isRollup()),
        new OakKey.Serializer(dimensionDescsList, indexIncrement),
        new OakValueSerializer(),
        minRow
    ).setPreferredBlockSize(oakBlockSize)
        .setChunkMaxItems(oakChunkMaxItems)
        .setMemoryCapacity(oakMaxMemoryCapacity)
        .buildOrderedMap();
  }

  @Override
  public FactsHolder getFacts()
  {
    return this;
  }

  @Override
  public int size()
  {
    return facts.size();
  }

  @Override
  public long getBytesInMemory()
  {
    return facts.memorySize();
  }

  @Override
  public void close()
  {
    super.close();

    for (BufferAggregator agg : aggregators) {
      if (agg != null) {
        agg.close();
      }
    }

    clear();
  }

  @Override
  protected AddToFactsResult addToFacts(InputRow row,
                                        IncrementalIndexRow key,
                                        ThreadLocal<InputRow> rowContainer,
                                        Supplier<InputRow> rowSupplier,
                                        boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    if (!skipMaxRowsInMemoryCheck) {
      // We validate here as a sanity check that we did not exceed the row and memory limitations
      // in previous insertions.
      if (size() > maxRowCount || (maxBytesInMemory > 0 && getBytesInMemory() > maxBytesInMemory)) {
        throw new IndexSizeExceededException(
            "Maximum number of rows [%d out of %d] or max size in bytes [%d out of %d] reached",
            size(), maxRowCount,
            getBytesInMemory(), maxBytesInMemory
        );
      }
    }

    // In rollup mode, we let the key-serializer assign the row index.
    // Upon lookup, the comparator ignores this special index value and only compares according to the key itself.
    // The serializer is only called on insertion, so it will not increment the index if the key already exits.
    // In plain mode, we force a new row index.
    // Upon lookup, since there is no key with this index, a new key will be inserted every time.
    key.setRowIndex(isRollup() ? OakKey.Serializer.ASSIGN_ROW_INDEX_IF_ABSENT : indexIncrement.getAndIncrement());

    // This call is different from FactsHolder.putIfAbsent() because it also handles the aggregation
    // in case the key already exits.
    final OakInputRowContext ctx = new OakInputRowContext(rowContainer, row);
    facts.zc().putIfAbsentComputeIfPresent(key, ctx, buffer -> aggregate(ctx, buffer));
    return new AddToFactsResult(size(), getBytesInMemory(), ctx.parseExceptionMessages);
  }

  @Override
  public int getLastRowIndex()
  {
    return indexIncrement.get() - 1;
  }

  private int getOffsetInBuffer(int aggIndex)
  {
    assert aggregatorOffsetInBuffer != null;
    return aggregatorOffsetInBuffer[aggIndex];
  }

  @Override
  protected float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggregators[aggIndex].getFloat(oakRow.getAggregationsBuffer(), getOffsetInBuffer(aggIndex));
  }

  @Override
  protected long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggregators[aggIndex].getLong(oakRow.getAggregationsBuffer(), getOffsetInBuffer(aggIndex));
  }

  @Override
  protected Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggregators[aggIndex].get(oakRow.getAggregationsBuffer(), getOffsetInBuffer(aggIndex));
  }

  @Override
  protected double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggregators[aggIndex].getDouble(oakRow.getAggregationsBuffer(), getOffsetInBuffer(aggIndex));
  }

  @Override
  protected boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return aggregators[aggIndex].isNull(oakRow.getAggregationsBuffer(), getOffsetInBuffer(aggIndex));
  }

  private OakIncrementalIndexRow rowFromMapEntry(Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer> entry)
  {
    return new OakIncrementalIndexRow(
        entry.getKey(), dimensionDescsList, entry.getValue()
    );
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(
      @Nullable final List<PostAggregator> postAggs,
      final boolean descending
  )
  {
    return () -> transformIterator(descending, entry -> {
      OakIncrementalIndexRow row = rowFromMapEntry(entry);
      return getMapBasedRowWithPostAggregations(
          row,
          IntStream.range(0, aggregators.length).mapToObj(
              i -> getMetricObjectValue(row, i)
          ),
          postAggs
      );
    });
  }

  // Aggregator management: initialization and aggregation

  public void initAggValue(OakInputRowContext ctx, ByteBuffer aggBuffer)
  {
    AggregatorFactory[] metrics = getMetricAggs();
    assert selectors != null;

    if (aggregators.length > 0 && aggregators[aggregators.length - 1] == null) {
      synchronized (this) {
        if (aggregators[aggregators.length - 1] == null) {
          // note: creation of Aggregators is done lazily when at least one row from input is available
          // so that FilteredAggregators could be initialized correctly.
          ctx.setRow();
          for (int i = 0; i < metrics.length; i++) {
            final AggregatorFactory agg = metrics[i];
            if (aggregators[i] == null) {
              aggregators[i] = agg.factorizeBuffered(selectors.get(agg.getName()));
            }
          }
          ctx.clearRow();
        }
      }
    }

    for (int i = 0; i < metrics.length; i++) {
      aggregators[i].init(aggBuffer, getOffsetInBuffer(i));
    }

    aggregate(ctx, aggBuffer);
  }

  public void aggregate(OakInputRowContext ctx, OakBuffer buffer)
  {
    OakUnsafeDirectBuffer unsafeBuffer = (OakUnsafeDirectBuffer) buffer;
    aggregate(ctx, unsafeBuffer.getByteBuffer());
  }

  public void aggregate(OakInputRowContext ctx, ByteBuffer aggBuffer)
  {
    ctx.setRow();

    for (int i = 0; i < aggregators.length; i++) {
      final BufferAggregator agg = aggregators[i];

      try {
        agg.aggregate(aggBuffer, getOffsetInBuffer(i));
      }
      catch (ParseException e) {
        // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
        log.debug(e, "Encountered parse error, skipping aggregator[%s].", getMetricAggs()[i].getName());
        ctx.addException(e);
      }
    }

    ctx.clearRow();
  }

  /**
   * Responsible for the initialization of the aggregators of a new inserted row.
   * It is activated when a new row is serialized before insertion to the facts map.
   */
  class OakValueSerializer implements OakSerializer<OakInputRowContext>
  {
    @Override
    public void serialize(OakInputRowContext ctx, OakScopedWriteBuffer buffer)
    {
      OakUnsafeDirectBuffer unsafeBuffer = (OakUnsafeDirectBuffer) buffer;
      initAggValue(ctx, unsafeBuffer.getByteBuffer());
    }

    @Override
    public OakInputRowContext deserialize(OakScopedReadBuffer buffer)
    {
      // cannot deserialize without the IncrementalIndexRow
      throw new UnsupportedOperationException();
    }

    @Override
    public int calculateSize(OakInputRowContext row)
    {
      return aggregatorsTotalSize;
    }

    @Override
    public int calculateHash(OakInputRowContext oakInputRowContext)
    {
      // This method should not be called.
      throw new UnsupportedOperationException();
    }
  }

  // FactsHolder helper methods

  public Iterator<Row> transformIterator(
      boolean descending,
      Function<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>, Row> transformer
  )
  {
    OakMap<IncrementalIndexRow, OakInputRowContext> orderedFacts = descending ? facts.descendingMap() : facts;
    return orderedFacts.zc().entrySet().stream().map(transformer).iterator();
  }

  /**
   * Generate a new row object for each iterated item.
   */
  private Iterator<IncrementalIndexRow> transformNonStreamIterator(
      Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iterator)
  {
    return Iterators.transform(iterator, this::rowFromMapEntry);
  }

  /**
   * Since the buffers in the stream iterators are reused, we don't need to create
   * a new row object for each next() call.
   * See {@code OakIncrementalIndexRow.reset()} for more information.
   */
  private Iterator<IncrementalIndexRow> transformStreamIterator(
      Iterator<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>> iterator)
  {
    final OakIncrementalIndexRow[] rowHolder = new OakIncrementalIndexRow[1];

    return Iterators.transform(iterator, entry -> {
      if (rowHolder[0] == null) {
        rowHolder[0] = rowFromMapEntry(entry);
      } else {
        rowHolder[0].reset();
      }
      return rowHolder[0];
    });
  }

  // FactsHolder interface implementation

  @Override
  public int getPriorIndex(IncrementalIndexRow key)
  {
    return 0;
  }

  @Override
  public long getMinTimeMillis()
  {
    return facts.firstKey().getTimestamp();
  }

  @Override
  public long getMaxTimeMillis()
  {
    return facts.lastKey().getTimestamp();
  }

  @Override
  public Iterator<IncrementalIndexRow> iterator(boolean descending)
  {
    // We should never get here because we override iterableWithPostAggregations
    throw new UnsupportedOperationException();
  }

  @Override
  public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
  {
    return () -> {
      IncrementalIndexRow from = null;
      IncrementalIndexRow to = null;
      if (timeStart > getMinTimeMillis()) {
        from = new IncrementalIndexRow(timeStart, OakIncrementalIndexRow.NO_DIMS, dimensionDescsList,
                IncrementalIndexRow.EMPTY_ROW_INDEX);
      }

      if (timeEnd < getMaxTimeMillis()) {
        to = new IncrementalIndexRow(timeEnd, OakIncrementalIndexRow.NO_DIMS, dimensionDescsList,
                IncrementalIndexRow.EMPTY_ROW_INDEX);
      }

      OakMap<IncrementalIndexRow, OakInputRowContext> subMap = facts.subMap(from, true, to, false, descending);
      return transformStreamIterator(subMap.zc().entryStreamSet().iterator());
    };
  }

  @Override
  public Iterable<IncrementalIndexRow> keySet()
  {
    return () -> transformNonStreamIterator(facts.zc().entrySet().iterator());
  }

  @Override
  public Iterable<IncrementalIndexRow> persistIterable()
  {
    return () -> transformStreamIterator(facts.zc().entryStreamSet().iterator());
  }

  @Override
  public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void clear()
  {
    facts.close();
  }

  /**
   * OakIncrementalIndex builder
   */
  public static class Builder extends AppendableIndexBuilder
  {
    // OakMap stores its data outside the JVM heap in memory blocks. Larger blocks consolidate allocations and reduce
    // overhead, but can also waste more memory if not fully utilized. The default has a reasonable balance between
    // performance and memory usage when tested in a batch ingestion scenario.
    public static final int DEFAULT_OAK_BLOCK_SIZE = 8 * (1 << 20);

    // OakMap maintains its memory blocks with an internal data-structure. Structure size is roughly
    // `oakMaxMemoryCapacity/oakBlockSize`. We set this number to a large enough yet reasonable value so that this
    // structure does not consume too much memory.
    public static final long DEFAULT_OAK_MAX_MEMORY_CAPACITY = 32L * (1L << 30);

    // OakMap maintains its entries in small chunks. Using larger chunks reduces the number of on-heap objects, but may
    // incur more overhead when balancing the entries between chunks. The default showed the best performance in batch
    // ingestion scenarios.
    public static final int DEFAULT_OAK_CHUNK_MAX_ITEMS = 256;

    public long oakMaxMemoryCapacity = DEFAULT_OAK_MAX_MEMORY_CAPACITY;
    public int oakBlockSize = DEFAULT_OAK_BLOCK_SIZE;
    public int oakChunkMaxItems = DEFAULT_OAK_CHUNK_MAX_ITEMS;

    public Builder setOakMaxMemoryCapacity(long oakMaxMemoryCapacity)
    {
      this.oakMaxMemoryCapacity = oakMaxMemoryCapacity;
      return this;
    }

    public Builder setOakBlockSize(int oakBlockSize)
    {
      this.oakBlockSize = oakBlockSize;
      return this;
    }

    public Builder setOakChunkMaxItems(int oakChunkMaxItems)
    {
      this.oakChunkMaxItems = oakChunkMaxItems;
      return this;
    }

    @Override
    protected OakIncrementalIndex buildInner()
    {
      return new OakIncrementalIndex(
          Objects.requireNonNull(incrementalIndexSchema, "incrementalIndexSchema is null"),
          deserializeComplexMetrics,
          concurrentEventAdd,
          maxRowCount,
          maxBytesInMemory,
          oakMaxMemoryCapacity,
          oakBlockSize,
          oakChunkMaxItems
      );
    }
  }
}
