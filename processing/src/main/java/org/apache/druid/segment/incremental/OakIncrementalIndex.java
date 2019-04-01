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
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.oath.oak.OakMap;
import com.oath.oak.OakMapBuilder;
import com.oath.oak.OakRBuffer;
import com.oath.oak.OakWBuffer;
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
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;


/**
 */
public class OakIncrementalIndex extends IncrementalIndex<BufferAggregator>
{




  private final OakFactsHolder facts;
  private AggsManager aggsManager;

  private final boolean reportParseExceptions;
  private static final Logger log = new Logger(OakIncrementalIndex.class);
  private String outOfRowsReason;
  private final List<DimensionDesc> dimensionDescsList;

  public OakIncrementalIndex(IncrementalIndexSchema incrementalIndexSchema,
                             boolean deserializeComplexMetrics,
                             boolean reportParseExceptions,
                             boolean concurrentEventAdd,
                             int maxRowCount)
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, concurrentEventAdd);
    facts = new OakFactsHolder(incrementalIndexSchema, getDimensionDescsList(), aggsManager, getIn(),
            incrementalIndexSchema.isRollup(), maxRowCount);
    this.dimensionDescsList = getDimensionDescsList();
    this.reportParseExceptions = reportParseExceptions;
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  public boolean canAppendRow()
  {
    final boolean canAdd = size() < facts.getMaxRowCount();
    if (!canAdd) {
      outOfRowsReason = StringUtils.format("Maximum number of rows [%d] reached", facts.getMaxRowCount());
    }
    return canAdd;
  }

  @Override
  public String getOutOfRowsReason()
  {
    return outOfRowsReason;
  }

  @Override
  protected BufferAggregator[] initAggs(AggregatorFactory[] metrics,
                                        Supplier<InputRow> rowSupplier,
                                        boolean deserializeComplexMetrics,
                                        boolean concurrentEventAdd)
  {
    Map<String, ColumnSelectorFactory> selectors = new HashMap<>();
    int[] aggOffsetInBuffer = new int[metrics.length];

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

      if (i == 0) {
        aggOffsetInBuffer[i] = 0;
      } else {
        aggOffsetInBuffer[i] = aggOffsetInBuffer[i - 1] + metrics[i - 1].getMaxIntermediateSizeWithNulls();
      }
    }

    int aggsTotalSize;
    if (metrics.length == 0) {
      aggsTotalSize = 0;
    } else {
      aggsTotalSize = aggOffsetInBuffer[metrics.length - 1] + metrics[metrics.length - 1].getMaxIntermediateSizeWithNulls();
    }

    BufferAggregator[] aggs = new BufferAggregator[metrics.length];
    aggsManager = new AggsManager(aggs, selectors, aggOffsetInBuffer, aggsTotalSize, metrics, reportParseExceptions);
    return aggs;
  }

  @Override
  public void close()
  {
    facts.close();
  }

  @Override
  protected AddToFactsResult addToFacts(InputRow row,
                                        IncrementalIndexRow key,
                                        ThreadLocal<InputRow> rowContainer,
                                        Supplier<InputRow> rowSupplier,
                                        boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    final AtomicInteger numEntries = getNumEntries();
    final AtomicLong bytesInMemory = getBytesInMemory();
    return facts.addToOak(row, numEntries, bytesInMemory, key, rowContainer, skipMaxRowsInMemoryCheck);
  }

  @Override
  public int getLastRowIndex()
  {
    return this.facts.getRowIndexGenerator() - 1;
  }

  @Override
  protected BufferAggregator[] getAggsForRow(int rowOffset)
  {
    // We should never get here because we override iterableWithPostAggregations
    throw new NotImplementedException();
  }

  @Override
  protected Object getAggVal(BufferAggregator agg, int rowOffset, int aggPosition)
  {
    // We should never get here because we override iterableWithPostAggregations
    // oakII doesnt have different structures to keep rowOffset
    throw new NotImplementedException();
  }

  @Override
  protected float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    return facts.getMetricFloatValue(incrementalIndexRow, aggOffset);
  }

  @Override
  protected long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    return facts.getMetricLongValue(incrementalIndexRow, aggOffset);
  }

  @Override
  protected Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    return facts.getMetricObjectValue(incrementalIndexRow, aggOffset);
  }

  @Override
  protected double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    return facts.getMetricDoubleValue(incrementalIndexRow, aggOffset);
  }

  @Override
  protected boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggOffset)
  {
    return facts.isNull(incrementalIndexRow, aggOffset);
  }

  @Override
  public Iterable<Row> iterableWithPostAggregations(final List<PostAggregator> postAggs, final boolean descending)
  {
    //TODO YONIGO - rewrite this function. maybe return an unserialized row?
    Function<Map.Entry<OakRBuffer, OakRBuffer>, Row> transformer = entry -> {
      OakRBuffer serializedKey = entry.getKey();
      OakRBuffer serializedValue = entry.getValue();
      long timeStamp = OakUtils.getTimestamp(serializedKey);
      int dimsLength = OakUtils.getDimsLength(serializedKey);
      Map<String, Object> theVals = Maps.newLinkedHashMap();
      for (int i = 0; i < dimsLength; ++i) {
        Object dim = OakUtils.getDimValue(serializedKey, i);
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

      BufferAggregator[] aggs = aggsManager.getAggs();
      for (int i = 0; i < aggs.length; ++i) {
        BufferAggregator agg = aggs[i];
        int aggOffsetInBuffer = aggsManager.aggOffsetInBuffer[i];
        Object theVal = serializedValue.transform(bb -> agg.get(bb, aggOffsetInBuffer));
        theVals.put(aggsManager.metrics[i].getName(), theVal);
      }

      return new MapBasedRow(timeStamp, theVals);
    };

    return () -> facts.transformIterator(descending, transformer);
  }



  static class AggsManager
  {
    private final AggregatorFactory[] metrics;
    private volatile Map<String, ColumnSelectorFactory> selectors;
    private final boolean reportParseExceptions;

    //given a ByteBuffer and an offset where all aggregates for a row are stored
    //offset + aggOffsetInBuffer[i] would give position in ByteBuffer where ith aggregate
    //is stored
    private volatile int[] aggOffsetInBuffer;
    private volatile int aggsTotalSize;
    private final BufferAggregator[] aggs;

    public AggsManager(BufferAggregator[] aggs,
                       Map<String, ColumnSelectorFactory> selectors,
                       int[] aggOffsetInBuffer,
                       int aggsTotalSize, AggregatorFactory[] metrics,
                       boolean reportParseExceptions)
    {
      this.aggs = aggs;
      this.selectors = selectors;
      this.aggOffsetInBuffer = aggOffsetInBuffer;
      this.aggsTotalSize = aggsTotalSize;
      this.metrics = metrics;
      this.reportParseExceptions = reportParseExceptions;
    }

    public void initValue(ByteBuffer byteBuffer,
                          InputRow row,
                          ThreadLocal<InputRow> rowContainer)
    {
      if (metrics.length > 0 && aggs[aggs.length - 1] == null) {
        synchronized (this) {
          if (aggs[aggs.length - 1] == null) {
            // note: creation of Aggregators is done lazily when at least one row from input is available
            // so that FilteredAggregators could be initialized correctly.
            rowContainer.set(row);
            for (int i = 0; i < metrics.length; i++) {
              final AggregatorFactory agg = metrics[i];
              if (aggs[i] == null) {
                aggs[i] = agg.factorizeBuffered(selectors.get(agg.getName()));
              }
            }
            rowContainer.set(null);
          }
        }
      }

      for (int i = 0; i < metrics.length; i++) {
        aggs[i].init(byteBuffer, aggOffsetInBuffer[i]);
      }
      aggregate(row, rowContainer, byteBuffer);
    }

    public void aggregate(
            InputRow row,
            ThreadLocal<InputRow> rowContainer,
            ByteBuffer aggBuffer
    )
    {
      rowContainer.set(row);

      for (int i = 0; i < metrics.length; i++) {
        final BufferAggregator agg = aggs[i];

        try {
          agg.aggregate(aggBuffer, aggBuffer.position() + aggOffsetInBuffer[i]);
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            //TODO YONIGO - is this the right bevaviour? not the same in on/offheap
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", metrics[i].getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          }
        }
      }
      rowContainer.set(null);
    }

    public int aggsTotalSize()
    {
      return aggsTotalSize;
    }

    public BufferAggregator[] getAggs()
    {
      return aggs;
    }
  }


  private static class OakFactsHolder implements FactsHolder
  {
    private final OakMap<IncrementalIndexRow, Row> oak;

    private final long minTimestamp;
    private final List<DimensionDesc> dimensionDescsList;
    private final int maxRowCount;

    private final AtomicInteger rowIndexGenerator;

    private final AggsManager aggsManager;

    public OakFactsHolder(IncrementalIndexSchema incrementalIndexSchema,
                          List<DimensionDesc> dimensionDescsList,
                          AggsManager aggsManager, ThreadLocal<InputRow> in,
                          boolean rollup,
                          int maxRowCount)
    {
      OakMapBuilder<IncrementalIndexRow, Row> builder = new OakMapBuilder<>();
      builder.setComparator(new OakKeysComparator(dimensionDescsList, rollup))
              .setKeySerializer(new OakKeySerializer(dimensionDescsList))
              .setValueSerializer(new OakValueSerializer(aggsManager, in))
              .setMinKey(getMinIncrementalIndexRow());
      oak = builder.build();
      this.minTimestamp = incrementalIndexSchema.getMinTimestamp();
      this.dimensionDescsList = dimensionDescsList;
      this.maxRowCount = maxRowCount;
      this.rowIndexGenerator = new AtomicInteger(0);
      this.aggsManager = aggsManager;
    }


    public Iterator<Row> transformIterator(boolean descending, Function<Map.Entry<OakRBuffer, OakRBuffer>, Row> transformer)
    {
      OakMap<IncrementalIndexRow, Row> tmpOakMap = descending ? oak.descendingMap() : oak;
      return tmpOakMap.zc().entrySet().stream().map(transformer).iterator();
    }


    private IncrementalIndexRow getMinIncrementalIndexRow()
    {
      return new IncrementalIndexRow(minTimestamp, null, dimensionDescsList, IncrementalIndexRow.EMPTY_ROW_INDEX);
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      return 0;
    }

    @Override
    public long getMinTimeMillis()
    {
      return oak.firstKey().getTimestamp();
    }

    @Override
    public long getMaxTimeMillis()
    {
      return oak.lastKey().getTimestamp();
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      // We should never get here because we override iterableWithPostAggregations
      throw new NotImplementedException();
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      if (timeStart > timeEnd) {
        return null;
      }
      return () -> {
        IncrementalIndexRow from = new IncrementalIndexRow(timeStart, null, dimensionDescsList, IncrementalIndexRow.EMPTY_ROW_INDEX);
        IncrementalIndexRow to = new IncrementalIndexRow(timeEnd, null, dimensionDescsList, IncrementalIndexRow.EMPTY_ROW_INDEX);

        try (OakMap<IncrementalIndexRow, Row> subMap = oak.subMap(from, true, to, false, descending)) {
          Iterator<Map.Entry<OakRBuffer, OakRBuffer>> iterator = subMap
                  .zc()
                  .entrySet()
                  .iterator();
          //TODO YONIGO - what sorcery is this transform function doing to allow this?!
          return Iterators.transform(iterator,
              entry -> new OakIncrementalIndexRow(entry.getKey(), dimensionDescsList, entry.getValue()));
        }
      };
    }

    @Override
    public Iterable<IncrementalIndexRow> keySet()
    {
      return () -> Iterators.transform(oak.zc().entrySet().iterator(), entry ->
              new OakIncrementalIndexRow(entry.getKey(), dimensionDescsList, entry.getValue()));
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      return keySet();
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      //Oak is pigibacking the FactsHolder and doesnt really have rowIndex stored in it.
      throw new NotImplementedException();
    }

    @Override
    public void clear()
    {
      oak.clear();
    }

    private AddToFactsResult addToOak(
            InputRow row,
            AtomicInteger numEntries,
            AtomicLong bytesInMemory, IncrementalIndexRow incrementalIndexRow,
            ThreadLocal<InputRow> rowContainer,
            boolean skipMaxRowsInMemoryCheck
    ) throws IndexSizeExceededException
    {

      Consumer<OakWBuffer> computer = buffer -> aggsManager.aggregate(row, rowContainer, buffer.getByteBuffer());
      incrementalIndexRow.setRowIndex(rowIndexGenerator.getAndIncrement());
      boolean added = oak.zc().putIfAbsentComputeIfPresent(incrementalIndexRow, row, computer);
      if (added) {
        numEntries.incrementAndGet();
      }

      //TODO YONIGO - we will continue to add and throw exceptions. Cannot check before because of rollup.
      if ((numEntries.get() > maxRowCount) //TODO YONIGO: || sizeInBytes.get() >= maxBytesInMemory
              && !skipMaxRowsInMemoryCheck) {
        throw new IndexSizeExceededException(
                "Maximum number of rows [%d] or max size in bytes [%d] reached",
                maxRowCount
        );
      }

      return new AddToFactsResult(oak.size(), 0, new ArrayList<>());
    }

    public int getMaxRowCount()
    {
      return maxRowCount;
    }

    public int getRowIndexGenerator()
    {
      return rowIndexGenerator.get();
    }

    public void close()
    {
      //TODO YONIGO - aggregators maybe should be closed like on/offheapindex?
      oak.close();
    }

    protected float getMetricFloatValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
    {
      Function<ByteBuffer, Float> transformer = serializedValue -> {
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.getFloat(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      };

      OakRBuffer rBuffer = ((OakIncrementalIndexRow) incrementalIndexRow).getAggregations();
      return rBuffer.transform(transformer);
    }


    protected long getMetricLongValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
    {
      Function<ByteBuffer, Long> transformer = serializedValue -> {
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.getLong(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      };

      OakRBuffer rBuffer = ((OakIncrementalIndexRow) incrementalIndexRow).getAggregations();
      return rBuffer.transform(transformer);
    }


    protected Object getMetricObjectValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
    {
      Function<ByteBuffer, Object> transformer = serializedValue -> {
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.get(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      };

      OakRBuffer rBuffer = ((OakIncrementalIndexRow) incrementalIndexRow).getAggregations();
      return rBuffer.transform(transformer);
    }


    protected double getMetricDoubleValue(IncrementalIndexRow incrementalIndexRow, int aggIndex)
    {
      Function<ByteBuffer, Double> transformer = serializedValue -> {
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.getDouble(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      };

      OakRBuffer rBuffer = ((OakIncrementalIndexRow) incrementalIndexRow).getAggregations();
      return rBuffer.transform(transformer);
    }


    protected boolean isNull(IncrementalIndexRow incrementalIndexRow, int aggIndex)
    {
      Function<ByteBuffer, Boolean> transformer = serializedValue -> {
        BufferAggregator agg = aggsManager.getAggs()[aggIndex];
        return agg.isNull(serializedValue, serializedValue.position() + aggsManager.aggOffsetInBuffer[aggIndex]);
      };

      OakRBuffer rBuffer = ((OakIncrementalIndexRow) incrementalIndexRow).getAggregations();
      return rBuffer.transform(transformer);
    }
  }
}
