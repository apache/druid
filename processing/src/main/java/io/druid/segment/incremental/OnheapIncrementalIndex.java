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
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator[]>
{
  private static final Logger log = new Logger(OnheapIncrementalIndex.class);

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, maxRowCount);
  }

  public OnheapIncrementalIndex(
      long minTimestamp,
      QueryGranularity gran,
      final AggregatorFactory[] metrics,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      int maxRowCount
  )
  {
    this(
        new IncrementalIndexSchema.Builder().withMinTimestamp(minTimestamp)
                                            .withQueryGranularity(gran)
                                            .withMetrics(metrics)
                                            .build(),
        deserializeComplexMetrics,
        reportParseExceptions,
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
        true,
        maxRowCount
    );
  }

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean reportParseExceptions,
      int maxRowCount
  )
  {
    this(incrementalIndexSchema, true, reportParseExceptions, maxRowCount);
  }

  @Override
  protected DimDim makeDimDim(String dimension, Object lock)
  {
    return new OnHeapDimDim(lock);
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
    Aggregator[] aggs = facts.get(key);

    if (aggs == null) {
      aggs = new Aggregator[metrics.length];

      rowContainer.set(row);
      for (int i = 0; i < metrics.length; i++) {
        final AggregatorFactory agg = metrics[i];
        aggs[i] = agg.factorize(selectors.get(agg.getName()));
      }
      rowContainer.set(null);

      // Last ditch sanity checks
      if (numEntries.get() >= maxRowCount && !facts.containsKey(key)) {
        throw new IndexSizeExceededException("Maximum number of rows [%d] reached", maxRowCount);
      }
      final Aggregator[] prev = facts.putIfAbsent(key, aggs);
      if (prev == null) {
        numEntries.incrementAndGet();
      } else {
        // We lost a race, free up the misfire
        destroy(aggs);
        aggs = prev;
        // This is expected to occur ~80% of the time in the worst scenarios
      }
    }

    rowContainer.set(row);
    for (final Aggregator agg : aggs) {
      synchronized (agg) {
        try {
          agg.aggregate();
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
  protected void destroy(Aggregator[] aggregators)
  {
    for (Aggregator aggregator : aggregators) {
      try {
        aggregator.close();
      }
      catch (Throwable t) {
        log.error(t, t.toString());
      }
    }
  }

  @Override
  public float getMetricFloatValue(Aggregator[] agg, int aggOffset)
  {
    return agg[aggOffset].getFloat();
  }

  @Override
  public long getMetricLongValue(Aggregator[] agg, int aggOffset)
  {
    return agg[aggOffset].getLong();
  }

  @Override
  public Object getMetricObjectValue(Aggregator[] agg, int aggOffset)
  {
    return agg[aggOffset].get();
  }

  static class OnHeapDimDim<T extends Comparable<? super T>> implements DimDim<T>
  {
    private final Map<T, Integer> valueToId = Maps.newHashMap();
    private T minValue = null;
    private T maxValue = null;

    private final List<T> idToValue = Lists.newArrayList();
    private final Object lock;

    public OnHeapDimDim(Object lock)
    {
      this.lock = lock;
    }

    public int getId(T value)
    {
      synchronized (lock) {
        final Integer id = valueToId.get(value);
        return id == null ? -1 : id;
      }
    }

    public T getValue(int id)
    {
      synchronized (lock) {
        return idToValue.get(id);
      }
    }

    public boolean contains(T value)
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

    public int add(T value)
    {
      synchronized (lock) {
        Integer prev = valueToId.get(value);
        if (prev != null) {
          return prev;
        }
        final int index = size();
        valueToId.put(value, index);
        idToValue.add(value);
        minValue = minValue == null || minValue.compareTo(value) > 0 ? value : minValue;
        maxValue = maxValue == null || maxValue.compareTo(value) < 0 ? value : maxValue;
        return index;
      }
    }

    @Override
    public T getMinValue()
    {
      return minValue;
    }

    @Override
    public T getMaxValue()
    {
      return maxValue;
    }

    public OnHeapDimLookup sort()
    {
      synchronized (lock) {
        return new OnHeapDimLookup(idToValue, size());
      }
    }
  }

  static class OnHeapDimLookup<T extends Comparable<? super T>> implements SortedDimLookup<T>
  {
    private final List<T> sortedVals;
    private final int[] idToIndex;
    private final int[] indexToId;

    public OnHeapDimLookup(List<T> idToValue, int length)
    {
      Map<T, Integer> sortedMap = Maps.newTreeMap();
      for (int id = 0; id < length; id++) {
        sortedMap.put(idToValue.get(id), id);
      }
      this.sortedVals = Lists.newArrayList(sortedMap.keySet());
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
      return sortedVals.size();
    }

    @Override
    public int getUnsortedIdFromSortedId(int index)
    {
      return indexToId[index];
    }

    @Override
    public T getValueFromSortedId(int index)
    {
      return sortedVals.get(index);
    }

    @Override
    public int getSortedIdFromUnsortedId(int id)
    {
      return idToIndex[id];
    }
  }
}
