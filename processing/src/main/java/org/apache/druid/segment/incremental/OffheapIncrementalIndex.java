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

package org.apache.druid.segment.incremental;

import com.google.common.base.Supplier;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.BufferAggregator;
import org.apache.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *
 */
public class OffheapIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  private static final Logger log = new Logger(OffheapIncrementalIndex.class);

  private final NonBlockingPool<ByteBuffer> bufferPool;

  private final List<ResourceHolder<ByteBuffer>> aggBuffers = new ArrayList<>();
  private final List<int[]> indexAndOffsets = new ArrayList<>();

  private final FactsHolder facts;

  private final AtomicInteger indexIncrement = new AtomicInteger(0);

  protected final int maxRowCount;

  @Nullable
  private volatile Map<String, ColumnSelectorFactory> selectors;

  //given a ByteBuffer and an offset where all aggregates for a row are stored
  //offset + aggOffsetInBuffer[i] would give position in ByteBuffer where ith aggregate
  //is stored
  @Nullable
  private volatile int[] aggOffsetInBuffer;
  private volatile int aggsTotalSize;

  @Nullable
  private String outOfRowsReason = null;

  OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean concurrentEventAdd,
      boolean sortFacts,
      int maxRowCount,
      NonBlockingPool<ByteBuffer> bufferPool
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, concurrentEventAdd);
    this.maxRowCount = maxRowCount;
    this.bufferPool = bufferPool;

    this.facts = incrementalIndexSchema.isRollup() ? new RollupFactsHolder(sortFacts, dimsComparator(), getDimensions())
                                                   : new PlainFactsHolder(sortFacts, dimsComparator());

    //check that stupid pool gives buffers that can hold at least one row's aggregators
    ResourceHolder<ByteBuffer> bb = bufferPool.take();
    if (bb.get().capacity() < aggsTotalSize) {
      bb.close();
      throw new IAE("bufferPool buffers capacity must be >= [%s]", aggsTotalSize);
    }
    aggBuffers.add(bb);
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  protected BufferAggregator[] initAggs(
      final AggregatorFactory[] metrics,
      final Supplier<InputRow> rowSupplier,
      final boolean deserializeComplexMetrics,
      final boolean concurrentEventAdd
  )
  {
    selectors = new HashMap<>();
    aggOffsetInBuffer = new int[metrics.length];

    int aggsCurOffsetInBuffer = 0;

    for (int i = 0; i < metrics.length; i++) {
      AggregatorFactory agg = metrics[i];

      ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactory(
          agg,
          rowSupplier,
          deserializeComplexMetrics
      );

      selectors.put(
          agg.getName(),
          new OnheapIncrementalIndex.CachingColumnSelectorFactory(columnSelectorFactory, concurrentEventAdd)
      );

      aggOffsetInBuffer[i] = aggsCurOffsetInBuffer;
      aggsCurOffsetInBuffer += agg.getMaxIntermediateSizeWithNulls();
    }

    aggsTotalSize = aggsCurOffsetInBuffer;

    return new BufferAggregator[metrics.length];
  }

  @Override
  protected AddToFactsResult addToFacts(
      InputRow row,
      IncrementalIndexRow key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier,
      boolean skipMaxRowsInMemoryCheck // ignored, we always want to check this for offheap
  ) throws IndexSizeExceededException
  {
    synchronized (this) {
      final AggregatorFactory[] metrics = getMetrics();
      final int priorIndex = facts.getPriorIndex(key);
      if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
        final int[] indexAndOffset = indexAndOffsets.get(priorIndex);
        ByteBuffer aggBuffer = aggBuffers.get(indexAndOffset[0]).get();
        return aggregate(row, rowContainer, aggBuffer, indexAndOffset[1]);
      } else {
        if (metrics.length > 0 && getAggs()[0] == null) {
          // note: creation of Aggregators is done lazily when at least one row from input is available
          // so that FilteredAggregators could be initialized correctly.
          rowContainer.set(row);
          for (int i = 0; i < metrics.length; i++) {
            final AggregatorFactory agg = metrics[i];
            getAggs()[i] = agg.factorizeBuffered(selectors.get(agg.getName()));
          }
          rowContainer.set(null);
        }

        int bufferIndex = aggBuffers.size() - 1;
        ByteBuffer lastBuffer = aggBuffers.isEmpty() ? null : aggBuffers.get(aggBuffers.size() - 1).get();
        int[] lastAggregatorsIndexAndOffset = indexAndOffsets.isEmpty()
                                              ? null
                                              : indexAndOffsets.get(indexAndOffsets.size() - 1);

        if (lastAggregatorsIndexAndOffset != null && lastAggregatorsIndexAndOffset[0] != bufferIndex) {
          throw new ISE("last row's aggregate's buffer and last buffer index must be same");
        }

        int bufferOffset = aggsTotalSize + (lastAggregatorsIndexAndOffset != null ? lastAggregatorsIndexAndOffset[1] : 0);
        ByteBuffer aggBuffer;
        if (lastBuffer != null &&
            lastBuffer.capacity() - bufferOffset >= aggsTotalSize) {
          aggBuffer = lastBuffer;
        } else {
          ResourceHolder<ByteBuffer> bb = bufferPool.take();
          aggBuffers.add(bb);
          bufferIndex = aggBuffers.size() - 1;
          bufferOffset = 0;
          aggBuffer = bb.get();
        }

        for (int i = 0; i < metrics.length; i++) {
          getAggs()[i].init(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
        }

        // Last ditch sanity checks
        if (getNumEntries().get() >= maxRowCount && facts.getPriorIndex(key) == IncrementalIndexRow.EMPTY_ROW_INDEX) {
          throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
        }

        final int rowIndex = indexIncrement.getAndIncrement();

        // note that we must update indexAndOffsets and the aggregator's buffers before facts, because as soon as we
        // update facts concurrent readers get hold of it and might ask for newly added row
        AddToFactsResult res = aggregate(row, rowContainer, aggBuffer, bufferOffset);
        indexAndOffsets.add(new int[]{bufferIndex, bufferOffset});
        final int prev = facts.putIfAbsent(key, rowIndex);
        if (IncrementalIndexRow.EMPTY_ROW_INDEX == prev) {
          getNumEntries().incrementAndGet();
        } else {
          throw new ISE("Unexpected state: Concurrent fact addition.");
        }

        return res;
      }
    }
  }

  public AddToFactsResult aggregate(
      InputRow row,
      ThreadLocal<InputRow> rowContainer,
      ByteBuffer aggBuffer,
      int bufferOffset
  )
  {
    final List<String> parseExceptionMessages = new ArrayList<>();

    rowContainer.set(row);
    for (int i = 0; i < getMetrics().length; i++) {
      final BufferAggregator agg = getAggs()[i];

      synchronized (agg) {
        try {
          agg.aggregate(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          log.debug(e, "Encountered parse error, skipping aggregator[%s].", getMetricAggs()[i].getName());
          parseExceptionMessages.add(e.getMessage());
        }
      }
    }
    rowContainer.set(null);

    return new AddToFactsResult(getNumEntries().get(), 0, parseExceptionMessages);
  }


  @Override
  public int getLastRowIndex()
  {
    return indexIncrement.get() - 1;
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = StringUtils.format("Maximum number of rows [%d] reached", maxRowCount);
    }
    return canAdd;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected BufferAggregator[] getAggsForRow(int rowOffset)
  {
    return getAggs();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, int rowOffset, int aggPosition)
  {
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.get(bb, indexAndOffset[1] + aggOffsetInBuffer[aggPosition]);
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.getFloat(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.getLong(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.get(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.getDouble(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public boolean isNull(int rowOffset, int aggOffset)
  {
    BufferAggregator agg = getAggs()[aggOffset];
    int[] indexAndOffset = indexAndOffsets.get(rowOffset);
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return agg.isNull(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  /**
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    facts.clear();
    indexAndOffsets.clear();

    if (selectors != null) {
      selectors.clear();
    }

    Closer c = Closer.create();
    aggBuffers.forEach(c::register);
    try {
      c.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    aggBuffers.clear();
  }

  public static class Builder extends AppendableIndexBuilder
  {
    @Nullable
    NonBlockingPool<ByteBuffer> bufferPool = null;

    public Builder setBufferPool(final NonBlockingPool<ByteBuffer> bufferPool)
    {
      this.bufferPool = bufferPool;
      return this;
    }

    @Override
    public void validate()
    {
      super.validate();
      if (bufferPool == null) {
        throw new IllegalArgumentException("bufferPool cannot be null");
      }
    }

    @Override
    protected OffheapIncrementalIndex buildInner()
    {
      return new OffheapIncrementalIndex(
          Objects.requireNonNull(incrementalIndexSchema, "incrementalIndexSchema is null"),
          deserializeComplexMetrics,
          concurrentEventAdd,
          sortFacts,
          maxRowCount,
          Objects.requireNonNull(bufferPool, "bufferPool is null")
      );
    }
  }
}
