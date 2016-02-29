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
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.parsers.ParseException;
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
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OffheapIncrementalIndex extends IncrementalIndex<int[]>
{
  private final StupidPool<ByteBuffer> bufferPool;

  private final List<ResourceHolder<ByteBuffer>> aggBuffers = new ArrayList<>();

  private final int[] nextBufferIndexAndOffset = new int[]{0, 0};

  private ColumnSelectorFactory[] selectorFactories;
  private BufferAggregator[] aggregators;

  //given a ByteBuffer and an offset where all aggregates for a row are stored
  //offset + aggOffsetInBuffer[i] would give position in ByteBuffer where ith aggregate
  //is stored
  private volatile int[] aggOffsetInBuffer;
  private volatile int aggsTotalSize;

  public OffheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      int maxRowCount,
      StupidPool<ByteBuffer> bufferPool
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, maxRowCount);
    this.bufferPool = bufferPool;

    //check that stupid pool gives buffers that can hold at least one row's aggregators
    ResourceHolder<ByteBuffer> bb = bufferPool.take();
    if (bb.get().capacity() < aggsTotalSize) {
      RuntimeException ex = new IAE("bufferPool buffers capacity must be >= [%s]", aggsTotalSize);
      try {
        bb.close();
      }
      catch (IOException ioe) {
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
      boolean reportParseExceptions,
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
        reportParseExceptions,
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
        true,
        maxRowCount,
        bufferPool
    );
  }

  @Override
  protected DimDim makeDimDim(String dimension, Object lock)
  {
    return new OnheapIncrementalIndex.OnHeapDimDim(lock);
  }

  @Override
  protected void initAggs(
      AggregatorFactory[] metrics, Supplier<InputRow> rowSupplier, boolean deserializeComplexMetrics
  )
  {
    aggOffsetInBuffer = new int[metrics.length];
    selectorFactories = new ColumnSelectorFactory[metrics.length];

    for (int i = 0; i < metrics.length; i++) {
      selectorFactories[i] = makeColumnSelectorFactory(
          metrics[i],
          rowSupplier,
          deserializeComplexMetrics
      );
      if (i > 0) {
        aggOffsetInBuffer[i] = aggOffsetInBuffer[i - 1] + metrics[i - 1].getMaxIntermediateSize();
      }
    }

    if (metrics.length > 0) {
      aggsTotalSize = aggOffsetInBuffer[metrics.length - 1] + metrics[metrics.length - 1].getMaxIntermediateSize();
    }
  }

  @Override
  protected Integer addToFacts(
      AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
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
      int[] indexAndOffset = facts.get(key);
      if (indexAndOffset != null) {
        bufferIndex = indexAndOffset[0];
        bufferOffset = indexAndOffset[1];
        aggBuffer = aggBuffers.get(bufferIndex).get();
      } else {
        if (aggregators == null) {
          // note: creation of Aggregators is done lazily when at least one row from input is available
          // so that FilteredAggregators could be initialized correctly.
          rowContainer.set(row);
          aggregators = new BufferAggregator[metrics.length];
          for (int i = 0; i < metrics.length; i++) {
            aggregators[i] = metrics[i].factorizeBuffered(selectorFactories[i]);
          }
          rowContainer.set(null);
        }

        bufferIndex = aggBuffers.size() - 1;
        ByteBuffer lastBuffer = aggBuffers.get(bufferIndex).get();

        if (nextBufferIndexAndOffset[0] != bufferIndex) {
          throw new ISE("last row's aggregate's buffer and last buffer index must be same");
        }

        if (lastBuffer.capacity() >= nextBufferIndexAndOffset[1] + aggsTotalSize) {
          aggBuffer = lastBuffer;
          bufferOffset = nextBufferIndexAndOffset[1];
        } else {
          ResourceHolder<ByteBuffer> bb = bufferPool.take();
          aggBuffers.add(bb);
          bufferIndex = aggBuffers.size() - 1;
          bufferOffset = 0;
          aggBuffer = bb.get();
        }

        for (int i = 0; i < metrics.length; i++) {
          aggregators[i].init(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
        }

        // Last ditch sanity checks
        if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
          throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
        }

        final Object prev = facts.putIfAbsent(key, new int[]{bufferIndex, bufferOffset});
        if (null == prev) {
          numEntries.incrementAndGet();
          nextBufferIndexAndOffset[0] = bufferIndex;
          nextBufferIndexAndOffset[1] = bufferOffset + aggsTotalSize;
        } else {
          throw new ISE("WTF! we are in synchronized block.");
        }
      }
    }

    rowContainer.set(row);
    for (int i = 0; i < aggregators.length; i++) {
      final BufferAggregator agg = aggregators[i];
      synchronized (agg) {
        try {
          agg.aggregate(aggBuffer, bufferOffset + aggOffsetInBuffer[i]);
        } catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw e;
          }
        }
      }
    }
    rowContainer.set(null);

    return numEntries.get();
  }

  @Override
  public float getMetricFloatValue(int[] indexAndOffset, int aggOffset)
  {
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return aggregators[aggOffset].getFloat(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public long getMetricLongValue(int[] indexAndOffset, int aggOffset)
  {
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return aggregators[aggOffset].getLong(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  public Object getMetricObjectValue(int[] indexAndOffset, int aggOffset)
  {
    ByteBuffer bb = aggBuffers.get(indexAndOffset[0]).get();
    return aggregators[aggOffset].get(bb, indexAndOffset[1] + aggOffsetInBuffer[aggOffset]);
  }

  @Override
  protected void destroy(int[] indexAndOffset)
  {
  }

  /**
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();

    RuntimeException ex = null;
    for (ResourceHolder<ByteBuffer> buffHolder : aggBuffers) {
      try {
        buffHolder.close();
      }
      catch (IOException ioe) {
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
