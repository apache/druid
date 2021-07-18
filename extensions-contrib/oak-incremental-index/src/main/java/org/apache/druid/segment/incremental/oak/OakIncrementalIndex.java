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
import com.google.common.collect.Maps;
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
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionIndexer;
import org.apache.druid.segment.incremental.AppendableIndexBuilder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexRow;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.incremental.IndexSizeExceededException;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;


/**
 * OakIncrementalIndex has two main attributes that are different from the other IncrementalIndex implementations:
 * 1. It stores both **keys** and **values** off-heap (as opposed to the off-heap implementation that stores only
 *    the **values** off-heap).
 * 2. It is based on OakMap (https://github.com/yahoo/Oak) instead of Java's ConcurrentSkipList.
 * These two changes significantly reduce the number of heap-objects and thus decrease dramatically the GC's memory
 * and performance overhead.
 */
@EverythingIsNonnullByDefault
public class OakIncrementalIndex extends IncrementalIndex<BufferAggregator> implements IncrementalIndex.FactsHolder
{
  protected final int maxRowCount;
  protected final long maxBytesInMemory;
  private final boolean rollup;

  private final OakMap<IncrementalIndexRow, OakInputRowContext> facts;
  private final AtomicInteger rowIndexGenerator;

  @Nullable
  private Map<String, ColumnSelectorFactory> selectors;

  // Given a ByteBuffer and an offset inside the buffer, offset + aggOffsetInBuffer[i]
  // would give a position in the buffer where the i^th aggregator's value is stored.
  @Nullable
  private int[] aggregatorOffsetInBuffer;
  private int aggregatorsTotalSize;

  private static final Logger log = new Logger(OakIncrementalIndex.class);

  @Nullable
  private String outOfRowsReason = null;

  public OakIncrementalIndex(IncrementalIndexSchema incrementalIndexSchema,
                             boolean deserializeComplexMetrics,
                             boolean concurrentEventAdd,
                             int maxRowCount,
                             long maxBytesInMemory,
                             long oakMaxMemoryCapacity,
                             int oakBlockSize,
                             int oakChunkMaxItems)
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, concurrentEventAdd);

    assert selectors != null;
    assert aggregatorOffsetInBuffer != null;

    this.maxRowCount = maxRowCount;
    this.maxBytesInMemory = maxBytesInMemory <= 0 ? Long.MAX_VALUE : maxBytesInMemory;

    this.rowIndexGenerator = new AtomicInteger(0);
    this.rollup = incrementalIndexSchema.isRollup();

    final IncrementalIndexRow minRow = new IncrementalIndexRow(
        incrementalIndexSchema.getMinTimestamp(),
        OakIncrementalIndexRow.NO_DIMS,
        dimensionDescsList,
        IncrementalIndexRow.EMPTY_ROW_INDEX
    );

    this.facts = new OakMapBuilder<>(
        new OakKey.Comparator(dimensionDescsList, this.rollup),
        new OakKey.Serializer(dimensionDescsList, this.rowIndexGenerator),
        new OakValueSerializer(),
        minRow
    ).setPreferredBlockSize(oakBlockSize)
        .setChunkMaxItems(oakChunkMaxItems)
        .setMemoryCapacity(oakMaxMemoryCapacity)
        .build();
  }

  @Override
  public FactsHolder getFacts()
  {
    return this;
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean countCheck = getNumEntries().get() < maxRowCount;
    // if maxBytesInMemory = -1, then ignore sizeCheck
    final boolean sizeCheck = maxBytesInMemory <= 0 || getBytesInMemory().get() < maxBytesInMemory;
    final boolean canAdd = countCheck && sizeCheck;
    if (!countCheck && !sizeCheck) {
      outOfRowsReason = StringUtils.format(
          "Maximum number of rows [%d] and maximum size in bytes [%d] reached",
          maxRowCount,
          maxBytesInMemory
      );
    } else if (!countCheck) {
      outOfRowsReason = StringUtils.format("Maximum number of rows [%d] reached", maxRowCount);
    } else if (!sizeCheck) {
      outOfRowsReason = StringUtils.format("Maximum size in bytes [%d] reached", maxBytesInMemory);
    }

    return canAdd;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  public void close()
  {
    super.close();

    for (BufferAggregator agg : getAggs()) {
      if (agg != null) {
        agg.close();
      }
    }

    if (selectors != null) {
      selectors.clear();
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
      if (getNumEntries().get() > maxRowCount || (maxBytesInMemory > 0 && getBytesInMemory().get() > maxBytesInMemory)) {
        throw new IndexSizeExceededException(
            "Maximum number of rows [%d out of %d] or max size in bytes [%d out of %d] reached",
            getNumEntries().get(), maxRowCount,
            getBytesInMemory().get(), maxBytesInMemory
        );
      }
    }

    final OakInputRowContext ctx = new OakInputRowContext(rowContainer, row);
    if (rollup) {
      // In rollup mode, we let the key-serializer assign the row index.
      // Upon lookup, the comparator ignores this special index value and only compares according to the key itself.
      // The serializer is only called on insertion, so it will not increment the index if the key already exits.
      key.setRowIndex(OakKey.Serializer.ASSIGN_ROW_INDEX_IF_ABSENT);
    } else {
      // In plain mode, we force a new row index.
      // Upon lookup, since there is no key with this index, a new key will be inserted every time.
      key.setRowIndex(rowIndexGenerator.getAndIncrement());
    }

    // This call is different from FactsHolder.putIfAbsent() because it also handles the aggregation
    // in case the key already exits.
    facts.zc().putIfAbsentComputeIfPresent(key, ctx, buffer -> aggregate(ctx, buffer));

    int rowCount = facts.size();
    long memorySize = facts.memorySize();

    getNumEntries().set(rowCount);
    getBytesInMemory().set(memorySize);

    return new AddToFactsResult(rowCount, memorySize, ctx.parseExceptionMessages);
  }

  @Override
  public int getLastRowIndex()
  {
    return rowIndexGenerator.get() - 1;
  }

  @Override
  protected BufferAggregator[] getAggsForRow(int rowOffset)
  {
    // We should never get here because we override iterableWithPostAggregations
    throw new UnsupportedOperationException();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, int rowOffset, int aggPosition)
  {
    // We should never get here because we override iterableWithPostAggregations
    // This implementation does not need an additional structure to keep rowOffset
    throw new UnsupportedOperationException();
  }

  private int getOffsetInBuffer(int aggOffset, int aggIndex)
  {
    assert aggregatorOffsetInBuffer != null;
    return aggOffset + aggregatorOffsetInBuffer[aggIndex];
  }

  private int getOffsetInBuffer(OakIncrementalIndexRow oakRow, int aggIndex)
  {
    return getOffsetInBuffer(oakRow.getAggregationsOffset(), aggIndex);
  }

  @Override
  protected float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return getAggs()[aggIndex].getFloat(oakRow.getAggregationsBuffer(), getOffsetInBuffer(oakRow, aggIndex));
  }

  @Override
  protected long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return getAggs()[aggIndex].getLong(oakRow.getAggregationsBuffer(), getOffsetInBuffer(oakRow, aggIndex));
  }

  @Override
  protected Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return getAggs()[aggIndex].get(oakRow.getAggregationsBuffer(), getOffsetInBuffer(oakRow, aggIndex));
  }

  @Override
  protected double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return getAggs()[aggIndex].getDouble(oakRow.getAggregationsBuffer(), getOffsetInBuffer(oakRow, aggIndex));
  }

  @Override
  protected boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    OakIncrementalIndexRow oakRow = (OakIncrementalIndexRow) incrementalIndexRow;
    return getAggs()[aggIndex].isNull(oakRow.getAggregationsBuffer(), getOffsetInBuffer(oakRow, aggIndex));
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(
      @Nullable final List<PostAggregator> postAggs,
      final boolean descending
  )
  {
    final AggregatorFactory[] metrics = getMetricAggs();
    final BufferAggregator[] aggregators = getAggs();

    // It might be possible to rewrite this function to return a serialized row.
    Function<Map.Entry<OakUnscopedBuffer, OakUnscopedBuffer>, Row> transformer = entry -> {
      OakUnsafeDirectBuffer keyOakBuff = (OakUnsafeDirectBuffer) entry.getKey();
      OakUnsafeDirectBuffer valueOakBuff = (OakUnsafeDirectBuffer) entry.getValue();
      long serializedKeyAddress = keyOakBuff.getAddress();

      long timeStamp = OakKey.getTimestamp(serializedKeyAddress);
      int dimsLength = OakKey.getDimsLength(serializedKeyAddress);

      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < dimsLength; ++i) {
        Object dim = OakKey.getDim(serializedKeyAddress, i);
        DimensionDesc dimensionDesc = dimensionDescsList.get(i);
        if (dimensionDesc == null) {
          continue;
        }
        String dimensionName = dimensionDesc.getName();
        DimensionHandler handler = dimensionDesc.getHandler();
        if (dim == null || handler.getLengthOfEncodedKeyComponent(dim) == 0) {
          theVals.put(dimensionName, null);
          continue;
        }
        final DimensionIndexer indexer = dimensionDesc.getIndexer();
        Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualList(dim);
        theVals.put(dimensionName, rowVals);
      }

      ByteBuffer valueBuff = valueOakBuff.getByteBuffer();
      int valueOffset = valueOakBuff.getOffset();
      for (int i = 0; i < aggregators.length; ++i) {
        Object theVal = aggregators[i].get(valueBuff, valueOffset + aggregatorOffsetInBuffer[i]);
        theVals.put(metrics[i].getName(), theVal);
      }

      return new MapBasedRow(timeStamp, theVals);
    };

    return () -> transformIterator(descending, transformer);
  }

  // Aggregator management: initialization and aggregation

  @Override
  protected BufferAggregator[] initAggs(AggregatorFactory[] metrics,
                                        Supplier<InputRow> rowSupplier,
                                        boolean deserializeComplexMetrics,
                                        boolean concurrentEventAdd)
  {
    this.selectors = new HashMap<>();
    this.aggregatorOffsetInBuffer = new int[metrics.length];

    int curAggOffset = 0;
    for (int i = 0; i < metrics.length; i++) {
      aggregatorOffsetInBuffer[i] = curAggOffset;
      curAggOffset += metrics[i].getMaxIntermediateSizeWithNulls();
    }
    this.aggregatorsTotalSize = curAggOffset;

    for (AggregatorFactory agg : metrics) {
      ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactory(
          agg,
          rowSupplier,
          deserializeComplexMetrics
      );

      this.selectors.put(
          agg.getName(),
          new CachingColumnSelectorFactory(columnSelectorFactory, concurrentEventAdd)
      );
    }

    return new BufferAggregator[metrics.length];
  }

  public void initAggValue(OakInputRowContext ctx, ByteBuffer aggBuffer, int aggOffset)
  {
    AggregatorFactory[] metrics = getMetricAggs();
    BufferAggregator[] aggregators = getAggs();
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
      aggregators[i].init(aggBuffer, getOffsetInBuffer(aggOffset, i));
    }

    aggregate(ctx, aggBuffer, aggOffset);
  }

  public void aggregate(OakInputRowContext ctx, OakBuffer buffer)
  {
    OakUnsafeDirectBuffer unsafeBuffer = (OakUnsafeDirectBuffer) buffer;
    aggregate(ctx, unsafeBuffer.getByteBuffer(), unsafeBuffer.getOffset());
  }

  public void aggregate(OakInputRowContext ctx, ByteBuffer aggBuffer, int aggOffset)
  {
    final BufferAggregator[] aggregators = getAggs();

    ctx.setRow();

    for (int i = 0; i < aggregators.length; i++) {
      final BufferAggregator agg = aggregators[i];

      try {
        agg.aggregate(aggBuffer, getOffsetInBuffer(aggOffset, i));
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
      initAggValue(ctx, unsafeBuffer.getByteBuffer(), unsafeBuffer.getOffset());
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
    return Iterators.transform(iterator, entry ->
        new OakIncrementalIndexRow(entry.getKey(), dimensionDescsList, entry.getValue()));
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
        rowHolder[0] = new OakIncrementalIndexRow(entry.getKey(), dimensionDescsList, entry.getValue());
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
    // OakMap stores its data in off-heap memory blocks. Larger blocks consolidates allocations
    // and reduce its overhead, but has the potential to waste more memory if the block is not fully utilized.
    // The default is 8 MiB as it has a good balance between performance and memory usage in tested
    // batch ingestion scenario.
    public static final int DEFAULT_OAK_BLOCK_SIZE = 8 * (1 << 20);

    // OakMap has a predefined maximal capacity, that allows it to minimize the internal data-structure
    // for maintaining off-heap memory blocks.
    // The default is arbirtrary large number (32 GiB) that won't limit the users.
    public static final long DEFAULT_OAK_MAX_MEMORY_CAPACITY = 32L * (1L << 30);

    // OakMap internal data-structure maintains its entries in small chunks. Larger chunks reduces the number of
    // on-heap objects, but might incure more overhead when balancing the entries between the chunks.
    // The default is 256 as it showed best performance in tested batch ingestion scenario.
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
