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

package io.druid.segment.incremental;

import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import io.druid.collections.ResourceHolder;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OffheapIncrementalIndex extends IncrementalIndex<BufferAggregator>
{
  private final StupidPool<ByteBuffer> bufferPool;

  private final List<ResourceHolder<ByteBuffer>> aggBuffers = new ArrayList<>();
  private final List<int[]> indexAndOffsets = new ArrayList<>();

  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts;

  private final AtomicInteger indexIncrement = new AtomicInteger(0);

  protected final int maxRowCount;

  private volatile Map<String, ColumnSelectorFactory> selectors;

  //given a ByteBuffer and an offset where all aggregates for a row are stored
  //offset + aggOffsetInBuffer[i] would give position in ByteBuffer where ith aggregate
  //is stored
  private volatile int[] aggOffsetInBuffer;
  private volatile int aggsTotalSize;

  private String outOfRowsReason = null;

  public OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      int maxRowCount,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics);
    this.maxRowCount = maxRowCount;
    this.bufferPool = bufferPool;
    this.facts = new ConcurrentSkipListMap<>(dimsComparator());

    //check that stupid pool gives buffers that can hold at least one row's aggregators
    ResourceHolder<ByteBuffer> bb = bufferPool.take();
    if (bb.get().capacity() < aggsTotalSize) {
      RuntimeException ex = new IAE("bufferPool buffers capacity must be >= [%s]", aggsTotalSize);
      try {
        bb.close();
      } catch(IOException ioe){
        ex.addSuppressed(ioe);
      }
      throw ex;
    }
    aggBuffers.add(bb);
  }

  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      int maxRowCount,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        deserializeComplexMetrics,
        maxRowCount,
        bufferPool
    );
  }

  public OffheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      int maxRowCount,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        true,
        maxRowCount,
        bufferPool
    );
  }

  @Override
  public ConcurrentNavigableMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  @Override
  protected DimDim makeDimDim(String dimension, Object lock)
  {
    return new OnheapIncrementalIndex.OnHeapDimDim(lock);
  }

  @Override
  protected BufferAggregator[] initAggs(
      AggregatorFactory[] metrics, Supplier<InputRow> rowSupplier, boolean deserializeComplexMetrics
  )
  {
    selectors = Maps.newHashMap();
    aggOffsetInBuffer = new int[metrics.length];

    BufferAggregator[] aggregators = new BufferAggregator[metrics.length];

    for (int i = 0; i < metrics.length; i++) {
      AggregatorFactory agg = metrics[i];

      ColumnSelectorFactory columnSelectorFactory = makeColumnSelectorFactory(
          agg,
          rowSupplier,
          deserializeComplexMetrics
      );

      selectors.put(
          agg.getName(),
          new OnheapIncrementalIndex.ObjectCachingColumnSelectorFactory(columnSelectorFactory)
      );

      aggregators[i] = agg.factorizeBuffered(columnSelectorFactory);
      if (i == 0) {
        aggOffsetInBuffer[i] = 0;
      } else {
        aggOffsetInBuffer[i] = aggOffsetInBuffer[i-1] + metrics[i-1].getMaxIntermediateSize();
      }
    }

    aggsTotalSize = aggOffsetInBuffer[metrics.length - 1] + metrics[metrics.length - 1].getMaxIntermediateSize();

    return aggregators;
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      InputRow row,
      AtomicInteger numEntries,
      TimeAndDims key,
      ThreadLocal<InputRow> rowContainer,
      Supplier<InputRow> rowSupplier
  ) throws IndexSizeExceededException
  {
    ByteBuffer aggBuffer;
    int bufferIndex;
    int bufferOffset;

    synchronized (this) {
      final Integer priorIndex = facts.get(key);
      if (null != priorIndex) {
        final int[] indexAndOffset = indexAndOffsets.get(priorIndex);
        bufferIndex = indexAndOffset[0];
        bufferOffset = indexAndOffset[1];
        aggBuffer = aggBuffers.get(bufferIndex).get();
      } else {
        bufferIndex = aggBuffers.size() - 1;
        ByteBuffer lastBuffer = aggBuffers.isEmpty() ? null : aggBuffers.get(aggBuffers.size() - 1).get();
        int[] lastAggregatorsIndexAndOffset = indexAndOffsets.isEmpty()
                                              ? null
                                              : indexAndOffsets.get(indexAndOffsets.size() - 1);

        if (lastAggregatorsIndexAndOffset != null && lastAggregatorsIndexAndOffset[0] != bufferIndex) {
          throw new ISE("last row's aggregate's buffer and last buffer index must be same");
        }

        bufferOffset = aggsTotalSize + (lastAggregatorsIndexAndOffset != null ? lastAggregatorsIndexAndOffset[1] : 0);
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
        if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
          throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
        }

        final Integer rowIndex = indexIncrement.getAndIncrement();
        final Integer prev = facts.putIfAbsent(key, rowIndex);
        if (null == prev) {
          numEntries.incrementAndGet();
          indexAndOffsets.add(new int[]{bufferIndex, bufferOffset});
        } else {
          throw new ISE("WTF! we are in sychronized block.");
        }
      }
    }

    rowContainer.set(row);

    for (int i = 0; i < metrics.length; i++) {
      final BufferAggregator agg = getAggs()[i];

      synchronized (agg) {
        agg.aggregate(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
      }
    }
    rowContainer.set(null);
    return numEntries.get();
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < maxRowCount;
    if (!canAdd) {
      outOfRowsReason = String.format("Maximum number of rows [%d] reached", maxRowCount);
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

    RuntimeException ex = null;
    for (ResourceHolder<ByteBuffer> buffHolder : aggBuffers) {
      try {
        buffHolder.close();
      } catch(IOException ioe) {
        if (ex == null) {
          ex = Throwables.propagate(ioe);
        } else {
          ex.addSuppressed(ioe);
        }
      }
    }
    aggBuffers.clear();
    if (ex != null) {
      throw ex;
    }
  }
}
