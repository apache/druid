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

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.ResourceHolder;
import io.druid.data.input.InputRow;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnValueSelector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class OffheapIncrementalIndex extends ExternalDataIncrementalIndex<BufferAggregator>
{
  private static final Logger log = new Logger(OffheapIncrementalIndex.class);

  private final List<ResourceHolder<ByteBuffer>> aggBuffers = new ArrayList<>();
  private final List<int[]> indexAndOffsets = new ArrayList<>();

  private final AtomicInteger indexIncrement = new AtomicInteger(0);

  protected OffheapAggsManager aggsManager;

  OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean concurrentEventAdd,
      boolean sortFacts,
      int maxRowCount,
      NonBlockingPool<ByteBuffer> bufferPool
  )
  {
    super(incrementalIndexSchema, reportParseExceptions, sortFacts, maxRowCount);
    this.aggsManager = new OffheapAggsManager(incrementalIndexSchema, deserializeComplexMetrics,
            reportParseExceptions, concurrentEventAdd, rowSupplier,
            columnCapabilities, bufferPool, this);

    //check that stupid pool gives buffers that can hold at least one row's aggregators
    ResourceHolder<ByteBuffer> bb = aggsManager.bufferPool.take();
    if (bb.get().capacity() < this.aggsManager.aggsTotalSize) {
      bb.close();
      throw new IAE("bufferPool buffers capacity must be >= [%s]", this.aggsManager.aggsTotalSize);
    }
    aggBuffers.add(bb);
  }

  @Override
  protected FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  public BufferAggregator[] getAggs()
  {
    return aggsManager.getAggs();
  }

  @Override
  public AggregatorFactory[] getMetricAggs()
  {
    return aggsManager.getMetricAggs();
  }

  @Override
  protected AddToFactsResult addToFacts(
      boolean reportParseExceptions,
      InputRow row,
      AtomicInteger numEntries,
      AtomicLong sizeInBytes, // ignored, added to make abstract class method impl happy
      IncrementalIndexRow key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier,
      boolean skipMaxRowsInMemoryCheck // ignored, we always want to check this for offheap
  ) throws IndexSizeExceededException
  {
    ByteBuffer aggBuffer;
    int bufferIndex;
    int bufferOffset;

    synchronized (this) {
      final int priorIndex = facts.getPriorIndex(key);
      if (IncrementalIndexRow.EMPTY_ROW_INDEX != priorIndex) {
        final int[] indexAndOffset = indexAndOffsets.get(priorIndex);
        bufferIndex = indexAndOffset[0];
        bufferOffset = indexAndOffset[1];
        aggBuffer = aggBuffers.get(bufferIndex).get();
      } else {
        if (aggsManager.metrics.length > 0 && getAggs()[0] == null) {
          // note: creation of Aggregators is done lazily when at least one row from input is available
          // so that FilteredAggregators could be initialized correctly.
          rowContainer.set(row);
          for (int i = 0; i < aggsManager.metrics.length; i++) {
            final AggregatorFactory agg = aggsManager.metrics[i];
            getAggs()[i] = agg.factorizeBuffered(aggsManager.selectors.get(agg.getName()));
          }
          rowContainer.set(null);
        }

        bufferIndex = aggBuffers.size() - 1;
        ByteBuffer lastBuffer = aggBuffers.isEmpty() ? null : aggBuffers.get(aggBuffers.size() - 1).get();
        int[] lastAggregatorsIndexAndOffset = indexAndOffsets.isEmpty()
                ? null
                : indexAndOffsets.get(indexAndOffsets.size() - 1);

        if (lastAggregatorsIndexAndOffset != null && lastAggregatorsIndexAndOffset[0] != bufferIndex) {
          throw new ISE("last row's aggregate's buffer and last buffer index must be same");
        }

        bufferOffset = aggsManager.aggsTotalSize + (lastAggregatorsIndexAndOffset != null ? lastAggregatorsIndexAndOffset[1] : 0);
        if (lastBuffer != null &&
            lastBuffer.capacity() - bufferOffset >= aggsManager.aggsTotalSize) {
          aggBuffer = lastBuffer;
        } else {
          ResourceHolder<ByteBuffer> bb = aggsManager.bufferPool.take();
          aggBuffers.add(bb);
          bufferIndex = aggBuffers.size() - 1;
          bufferOffset = 0;
          aggBuffer = bb.get();
        }

        for (int i = 0; i < aggsManager.metrics.length; i++) {
          getAggs()[i].init(aggBuffer, bufferOffset + aggsManager.aggOffsetInBuffer[i]);
        }

        // Last ditch sanity checks
        if (numEntries.get() >= maxRowCount && facts.getPriorIndex(key) == IncrementalIndexRow.EMPTY_ROW_INDEX) {
          throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
        }

        final int rowIndex = indexIncrement.getAndIncrement();

        // note that indexAndOffsets must be updated before facts, because as soon as we update facts
        // concurrent readers get hold of it and might ask for newly added row
        indexAndOffsets.add(new int[]{bufferIndex, bufferOffset});
        final int prev = facts.putIfAbsent(key, rowIndex);
        if (IncrementalIndexRow.EMPTY_ROW_INDEX == prev) {
          numEntries.incrementAndGet();
        } else {
          throw new ISE("WTF! we are in sychronized block.");
        }
      }
    }

    rowContainer.set(row);

    for (int i = 0; i < aggsManager.metrics.length; i++) {
      final BufferAggregator agg = getAggs()[i];

      synchronized (agg) {
        try {
          agg.aggregate(aggBuffer, bufferOffset + aggsManager.aggOffsetInBuffer[i]);
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", getMetricAggs()[i].getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", getMetricAggs()[i].getName());
          }
        }
      }
    }
    rowContainer.set(null);
    return new AddToFactsResult(numEntries.get(), 0, new ArrayList<>());
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
  protected BufferAggregator[] getAggsForRow(IncrementalIndexRow incrementalIndexRow)
  {
    return getAggs();
  }

  @Override
  protected Object getAggVal(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int[] indexAndOffset = indexAndOffsets.get(incrementalIndexRow.getRowIndex());
    ByteBuffer aggBuffer = aggBuffers.get(indexAndOffset[0]).get();
    BufferAggregator agg = getAggs()[aggIndex];
    return agg.get(aggBuffer, indexAndOffset[1] + aggsManager.aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int[] indexAndOffset = indexAndOffsets.get(incrementalIndexRow.getRowIndex());
    ByteBuffer aggBuffer = aggBuffers.get(indexAndOffset[0]).get();
    BufferAggregator agg = getAggs()[aggIndex];
    return agg.getFloat(aggBuffer, indexAndOffset[1] + aggsManager.aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int[] indexAndOffset = indexAndOffsets.get(incrementalIndexRow.getRowIndex());
    ByteBuffer aggBuffer = aggBuffers.get(indexAndOffset[0]).get();
    BufferAggregator agg = getAggs()[aggIndex];
    return agg.getLong(aggBuffer, indexAndOffset[1] + aggsManager.aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int[] indexAndOffset = indexAndOffsets.get(incrementalIndexRow.getRowIndex());
    ByteBuffer aggBuffer = aggBuffers.get(indexAndOffset[0]).get();
    BufferAggregator agg = getAggs()[aggIndex];
    return agg.get(aggBuffer, indexAndOffset[1] + aggsManager.aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int[] indexAndOffset = indexAndOffsets.get(incrementalIndexRow.getRowIndex());
    ByteBuffer aggBuffer = aggBuffers.get(indexAndOffset[0]).get();
    BufferAggregator agg = getAggs()[aggIndex];
    return agg.getDouble(aggBuffer, indexAndOffset[1] + aggsManager.aggOffsetInBuffer[aggIndex]);
  }

  @Override
  public boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggIndex)
  {
    int[] indexAndOffset = indexAndOffsets.get(incrementalIndexRow.getRowIndex());
    ByteBuffer aggBuffer = aggBuffers.get(indexAndOffset[0]).get();
    BufferAggregator agg = getAggs()[aggIndex];
    return agg.isNull(aggBuffer, indexAndOffset[1] + aggsManager.aggOffsetInBuffer[aggIndex]);
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

    aggsManager.clearSelectors();

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

  @Nullable
  @Override
  public String getMetricType(String metric)
  {
    return aggsManager.getMetricType(metric);
  }

  @Override
  public ColumnValueSelector<?> makeMetricColumnValueSelector(String metric, IncrementalIndexRowHolder currEntry)
  {
    return aggsManager.makeMetricColumnValueSelector(metric, currEntry);
  }

  @Override
  public List<String> getMetricNames()
  {
    return aggsManager.getMetricNames();
  }

  @Override
  protected String getMetricName(int metricIndex)
  {
    return aggsManager.metrics[metricIndex].getName();
  }
}
