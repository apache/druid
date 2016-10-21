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
import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.logger.Logger;
import io.druid.java.util.common.parsers.ParseException;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.NumericColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.ColumnCapabilities;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
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

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions);
    this.maxRowCount = maxRowCount;

    this.facts = incrementalIndexSchema.isRollup() ? new RollupFactsHolder(sortFacts, dimsComparator(), getDimensions())
                                                   : new PlainFactsHolder(sortFacts);
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean sortFacts,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .withRollup(IncrementalIndexSchema.DEFAULT_ROLLUP)
                                            .build(),
        deserializeComplexMetrics,
        reportParseExceptions,
        sortFacts,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      boolean rollup,
      final AggregatorFactory[] metrics,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .withRollup(rollup)
                                            .build(),
        true,
        true,
        true,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      int maxRowCount
  )
  {
    this(
        minTimestamp,
        gran,
        IncrementalIndexSchema.DEFAULT_ROLLUP,
        metrics,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean reportParseExceptions,
      int maxRowCount
  )
  {
    this(incrementalIndexSchema, true, reportParseExceptions, true, maxRowCount);
  }

  @Override
  public FactsHolder getFacts()
  {
    return facts;
  }

  @Override
  protected Aggregator[] initAggs(
      AggregatorFactory[] metrics, Supplier<InputRow> rowSupplier, boolean deserializeComplexMetrics
  )
  {
    selectors = Maps.newHashMap();
    for (AggregatorFactory agg : metrics) {
      selectors.put(
          agg.getName(),
          new ObjectCachingColumnSelectorFactory(makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics, getColumnCapabilities()))
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
    final Integer priorIndex = facts.getPriorIndex(key);

    Aggregator[] aggs;

    if (null != priorIndex) {
      aggs = concurrentGet(priorIndex);
      doAggregate(aggs, rowContainer, row, reportParseExceptions);
    } else {
      aggs = new Aggregator[metrics.length];
      factorizeAggs(metrics, aggs, rowContainer, row);
      doAggregate(aggs, rowContainer, row, reportParseExceptions);

      final Integer rowIndex = indexIncrement.getAndIncrement();
      concurrentSet(rowIndex, aggs);

      // Last ditch sanity checks
      if (numEntries.get() >= maxRowCount && facts.getPriorIndex(key) == null) {
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
      final Integer prev = facts.putIfAbsent(key, rowIndex);
      if (null == prev) {
        numEntries.incrementAndGet();
      } else {
        // We lost a race
        aggs = concurrentGet(prev);
        doAggregate(aggs, rowContainer, row, reportParseExceptions);
        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    return numEntries.get();
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
      Aggregator[] aggs,
      ThreadLocal<InputRow> rowContainer,
      InputRow row,
      boolean reportParseExceptions
  )
  {
    rowContainer.set(row);

    for (Aggregator agg : aggs) {
      synchronized (agg) {
        try {
          agg.aggregate();
        }
        catch (ParseException e) {
          // "aggregate" can throw ParseExceptions if a selector expects something but gets something else.
          if (reportParseExceptions) {
            throw new ParseException(e, "Encountered parse error for aggregator[%s]", agg.getName());
          } else {
            log.debug(e, "Encountered parse error, skipping aggregator[%s].", agg.getName());
          }
        }
      }
    }

    rowContainer.set(null);
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

  /**
   * Clear out maps to allow GC
   * NOTE: This is NOT thread-safe with add... so make sure all the adding is DONE before closing
   */
  @Override
  public void close()
  {
    super.close();
    aggregators.clear();
    facts.clear();
    if (selectors != null) {
      selectors.clear();
    }
  }

  // Caches references to selector objects for each column instead of creating a new object each time in order to save heap space.
  // In general the selectorFactory need not to thread-safe.
  // here its made thread safe to support the special case of groupBy where the multiple threads can add concurrently to the IncrementalIndex.
  static class ObjectCachingColumnSelectorFactory implements ColumnSelectorFactory
  {
    private final ConcurrentMap<String, LongColumnSelector> longColumnSelectorMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, FloatColumnSelector> floatColumnSelectorMap = Maps.newConcurrentMap();
    private final ConcurrentMap<String, ObjectColumnSelector> objectColumnSelectorMap = Maps.newConcurrentMap();
    private final ColumnSelectorFactory delegate;

    public ObjectCachingColumnSelectorFactory(ColumnSelectorFactory delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return delegate.makeDimensionSelector(dimensionSpec);
    }

    @Override
    public FloatColumnSelector makeFloatColumnSelector(String columnName)
    {
      FloatColumnSelector existing = floatColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      } else {
        FloatColumnSelector newSelector = delegate.makeFloatColumnSelector(columnName);
        FloatColumnSelector prev = floatColumnSelectorMap.putIfAbsent(
            columnName,
            newSelector
        );
        return prev != null ? prev : newSelector;
      }
    }

    @Override
    public LongColumnSelector makeLongColumnSelector(String columnName)
    {
      LongColumnSelector existing = longColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      } else {
        LongColumnSelector newSelector = delegate.makeLongColumnSelector(columnName);
        LongColumnSelector prev = longColumnSelectorMap.putIfAbsent(
            columnName,
            newSelector
        );
        return prev != null ? prev : newSelector;
      }
    }

    @Override
    public ObjectColumnSelector makeObjectColumnSelector(String columnName)
    {
      ObjectColumnSelector existing = objectColumnSelectorMap.get(columnName);
      if (existing != null) {
        return existing;
      } else {
        ObjectColumnSelector newSelector = delegate.makeObjectColumnSelector(columnName);
        ObjectColumnSelector prev = objectColumnSelectorMap.putIfAbsent(
            columnName,
            newSelector
        );
        return prev != null ? prev : newSelector;
      }
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String columnName)
    {
      return delegate.getColumnCapabilities(columnName);
    }

    @Override
    public NumericColumnSelector makeMathExpressionSelector(String expression)
    {
      return delegate.makeMathExpressionSelector(expression);
    }
  }

}
