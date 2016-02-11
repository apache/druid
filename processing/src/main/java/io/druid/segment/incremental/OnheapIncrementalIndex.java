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
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator>
{
  private final ConcurrentHashMap<Integer, Aggregator[]> aggregators = new ConcurrentHashMap<>();
  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts;
  private final AtomicInteger indexIncrement = new AtomicInteger(0);
  protected final int maxRowCount;
  private volatile Map<String, ColumnSelectorFactory> selectors;

  private String outOfRowsReason = null;

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics);
    this.maxRowCount = maxRowCount;
    this.facts = new ConcurrentSkipListMap<>(dimsComparator());
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        deserializeComplexMetrics,
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
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        true,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      int maxRowCount
  )
  {
    this(incrementalIndexSchema, true, maxRowCount);
  }

  @Override
  public ConcurrentNavigableMap<TimeAndDims, Integer> getFacts()
  {
    return facts;
  }

  @Override
  protected DimDim makeDimDim(String dimension, Object lock)
  {
    return new OnHeapDimDim(lock);
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
          new ObjectCachingColumnSelectorFactory(makeColumnSelectorFactory(agg, rowSupplier, deserializeComplexMetrics))
      );
    }

    return new Aggregator[metrics.length];
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
    final Integer priorIndex = facts.get(key);

    Aggregator[] aggs;

    if (null != priorIndex) {
      aggs = concurrentGet(priorIndex);
    } else {
      aggs = new Aggregator[metrics.length];

      for (int i = 0; i < metrics.length; i++) {
        final AggregatorFactory agg = metrics[i];
        aggs[i] = agg.factorize(
            selectors.get(agg.getName())
        );
      }
      final Integer rowIndex = indexIncrement.getAndIncrement();

      concurrentSet(rowIndex, aggs);

      // Last ditch sanity checks
      if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
      final Integer prev = facts.putIfAbsent(key, rowIndex);
      if (null == prev) {
        numEntries.incrementAndGet();
      } else {
        // We lost a race
        aggs = concurrentGet(prev);
        // Free up the misfire
        concurrentRemove(rowIndex);
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    rowContainer.set(row);

    for (Aggregator agg : aggs) {
      synchronized (agg) {
        agg.aggregate();
      }
    }

    rowContainer.set(null);


    return numEntries.get();
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

  static class OnHeapDimDim implements DimDim
  {
    private final Map<String, Integer> valueToId = Maps.newHashMap();

    private final List<String> idToValue = Lists.newArrayList();
    private final Object lock;

    public OnHeapDimDim(Object lock)
    {
      this.lock = lock;
    }

    public int getId(String value)
    {
      synchronized (lock) {
        final Integer id = valueToId.get(value);
        return id == null ? -1 : id;
      }
    }

    public String getValue(int id)
    {
      synchronized (lock) {
        return idToValue.get(id);
      }
    }

    public boolean contains(String value)
    {
      synchronized (lock) {
        return valueToId.containsKey(value);
      }
    }

    public int size()
    {
      synchronized (lock) {
        return valueToId.size();
      }
    }

    public int add(String value)
    {
      synchronized (lock) {
        Integer prev = valueToId.get(value);
        if (prev != null) {
          return prev;
        }
        final int index = size();
        valueToId.put(value, index);
        idToValue.add(value);
        return index;
      }
    }

    public OnHeapDimLookup sort()
    {
      synchronized (lock) {
        return new OnHeapDimLookup(idToValue, size());
      }
    }
  }

  static class OnHeapDimLookup implements SortedDimLookup
  {
    private final String[] sortedVals;
    private final int[] idToIndex;
    private final int[] indexToId;

    public OnHeapDimLookup(List<String> idToValue, int length)
    {
      Map<String, Integer> sortedMap = Maps.newTreeMap();
      for (int id = 0; id < length; id++) {
        sortedMap.put(idToValue.get(id), id);
      }
      this.sortedVals = sortedMap.keySet().toArray(new String[length]);
      this.idToIndex = new int[length];
      this.indexToId = new int[length];
      int index = 0;
      for (Integer id : sortedMap.values()) {
        idToIndex[id] = index;
        indexToId[index] = id;
        index++;
      }
    }

    @Override
    public int size()
    {
      return sortedVals.length;
    }

    @Override
    public int indexToId(int index)
    {
      return indexToId[index];
    }

    @Override
    public String getValue(int index)
    {
      return sortedVals[index];
    }

    @Override
    public int idToIndex(int id)
    {
      return idToIndex[id];
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
  }

}
