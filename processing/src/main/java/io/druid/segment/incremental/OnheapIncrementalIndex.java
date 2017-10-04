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
import io.druid.data.input.InputRow;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.io.Closer;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator>
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);

  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final FactsHolder facts;
  private final AtomicInteger indexIncrement = new AtomicInteger(0);
  protected final int maxRowCount;
  private volatile Map<String, ColumnSelectorFactory> selectors;

  private String outOfRowsReason = null;

  OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean concurrentEventAdd,
      boolean sortFacts,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, concurrentEventAdd);
    this.maxRowCount = maxRowCount;

    this.facts = incrementalIndexSchema.isRollup() ? new RollupFactsHolder(sortFacts, dimsComparator(), getDimensions())
                                                   : new PlainFactsHolder(sortFacts);
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  protected Aggregator[] initAggs(
      final AggregatorFactory[] metrics,
      final Supplier<InputRow> rowSupplier,
      final boolean deserializeComplexMetrics,
      final boolean concurrentEventAdd
  )
  {
    selectors = Maps.newHashMap();
    for (AggregatorFactory agg : metrics) {
      selectors.put(
          agg.getName(),
          new ObjectCachingColumnSelectorFactory(
              makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics),
              concurrentEventAdd
          )
      );
    }

    return new Aggregator[metrics.length];
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
    final int priorIndex = facts.getPriorIndex(key);

    Aggregator[] aggs;

    if (TimeAndDims.EMPTY_ROW_INDEX != priorIndex) {
      aggs = concurrentGet(priorIndex);
      doAggregate(metrics, aggs, rowContainer, row, reportParseExceptions);
    } else {
      aggs = new Aggregator[metrics.length];
      factorizeAggs(metrics, aggs, rowContainer, row);
      doAggregate(metrics, aggs, rowContainer, row, reportParseExceptions);

      final int rowIndex = indexIncrement.getAndIncrement();
      concurrentSet(rowIndex, aggs);

      // Last ditch sanity checks
      if (numEntries.get() >= maxRowCount && facts.getPriorIndex(key) == TimeAndDims.EMPTY_ROW_INDEX) {
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
      final int prev = facts.putIfAbsent(key, rowIndex);
      if (TimeAndDims.EMPTY_ROW_INDEX == prev) {
        numEntries.incrementAndGet();
      } else {
        // We lost a race
        aggs = concurrentGet(prev);
        doAggregate(metrics, aggs, rowContainer, row, reportParseExceptions);
        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    return numEntries.get();
  }

  @Override
  public int getLastRowIndex()
  {
    return indexIncrement.get() - 1;
  }

  private void factorizeAggs(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      ThreadLocal<InputRow> rowContainer,
      InputRow row
  )
  {
    rowContainer.set(row);
    for (int i = 0; i < metrics.length; i++) {
      final AggregatorFactory agg = metrics[i];
      aggs[i] = agg.factorize(selectors.get(agg.getName()));
    }
    rowContainer.set(null);
  }

  private void doAggregate(
      AggregatorFactory[] metrics,
      Aggregator[] aggs,
      ThreadLocal<InputRow> rowContainer,
      InputRow row,
      boolean reportParseExceptions
  )
  {
    rowContainer.set(row);

    for (int i = 0; i < aggs.length; i++) {
      final Aggregator agg = aggs[i];
      synchronized (agg) {
        try {
          agg.aggregate();
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", metrics[i].getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", metrics[i].getName());
          }
        }
      }
    }

    rowContainer.set(null);
  }

  private void closeAggregators()
  {
    Closer closer = Closer.create();
    for (Aggregator[] aggs : aggregators.values()) {
      for (Aggregator agg : aggs) {
        closer.register(agg);
      }
    }

    try {
      closer.close();
    }
    catch (IOException e) {
      Throwables.propagate(e);
    }
  }

  protected Aggregator[] concurrentGet(int offset)
  {
    // All get operations should be fine
    return aggregators.get(offset);
  }

  protected void concurrentSet(int offset, Aggregator[] value)
  {
    aggregators.put(offset, value);
  }

  protected void concurrentRemove(int offset)
  {
    aggregators.remove(offset);
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
  protected Aggregator[] getAggsForRow(int rowOffset)
  {
    return concurrentGet(rowOffset);
  }

  @Override
  protected Object getAggVal(Aggregator agg, int rowOffset, int aggPosition)
  {
    return agg.get();
  }

  @Override
  public float getMetricFloatValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getFloat();
  }

  @Override
  public long getMetricLongValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getLong();
  }

  @Override
  public Object getMetricObjectValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].get();
  }

  @Override
  protected double getMetricDoubleValue(int rowOffset, int aggOffset)
  {
    return concurrentGet(rowOffset)[aggOffset].getDouble();
  }

  /**
   * Clear out maps to allow GC
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    closeAggregators();
    aggregators.clear();
    facts.clear();
    if (selectors != null) {
      selectors.clear();
    }
  }

  // Caches references to selector objects for each column instead of creating a new object each time in order to save heap space.
  // In general the selectorFactory need not to thread-safe.
  // If required, set concurrentEventAdd to true to use concurrent hash map instead of vanilla hash map for thread-safe
  // operations.
  static class ObjectCachingColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final Map<String, LongColumnSelector> longColumnSelectorMap;
    private final Map<String, FloatColumnSelector> floatColumnSelectorMap;
    private final Map<String, ObjectColumnSelector> objectColumnSelectorMap;
    private final Map<String, DoubleColumnSelector> doubleColumnSelectorMap;
    private final ColumnSelectorFactory delegate;

    public ObjectCachingColumnSelectorFactory(ColumnSelectorFactory delegate, boolean concurrentEventAdd)
    {
      this.delegate = delegate;

      if (concurrentEventAdd) {
        longColumnSelectorMap = new ConcurrentHashMap<>();
        floatColumnSelectorMap = new ConcurrentHashMap<>();
        objectColumnSelectorMap = new ConcurrentHashMap<>();
        doubleColumnSelectorMap = new ConcurrentHashMap<>();
      } else {
        longColumnSelectorMap = new HashMap<>();
        floatColumnSelectorMap = new HashMap<>();
        objectColumnSelectorMap = new HashMap<>();
        doubleColumnSelectorMap = new HashMap<>();
      }
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      final FloatColumnSelector existing = floatColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      }
      return floatColumnSelectorMap.computeIfAbsent(columnName, delegate::makeFloatColumnSelector);
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      final LongColumnSelector existing = longColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      }
      return longColumnSelectorMap.computeIfAbsent(columnName, delegate::makeLongColumnSelector);
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      final ObjectColumnSelector existing = objectColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      }
      return objectColumnSelectorMap.computeIfAbsent(columnName, delegate::makeObjectColumnSelector);
    }

    @Override
    public DoubleColumnSelector makeDoubleColumnSelector(String columnName)
    {
      final DoubleColumnSelector existing = doubleColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      }
      return doubleColumnSelectorMap.computeIfAbsent(columnName, delegate::makeDoubleColumnSelector);
    }

    @Nullable
    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return delegate.getColumnCapabilities(columnName);
    }
  }

}
