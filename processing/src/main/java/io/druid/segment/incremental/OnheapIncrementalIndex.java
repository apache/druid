/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.segment.incremental;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.ColumnSelectorFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 */
public class OnheapIncrementalIndex extends IncrementalIndex<Aggregator>
{
  private final ConcurrentHashMap<Integer, List<Aggregator>> aggregators = new ConcurrentHashMap<>();
  private final ConcurrentNavigableMap<TimeAndDims, Integer> facts = new ConcurrentSkipListMap<>();
  protected final int maxRowCount;

  private String outOfRowsReason = null;

  public OnheapIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      int maxRowCount
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics);
    this.maxRowCount = maxRowCount;
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
  protected DimDim makeDimDim(String dimension)
  {
    return new OnHeapDimDim();
  }

  @Override
  protected List<Aggregator> initAggs(
      AggregatorFactory[] metrics,
      ThreadLocal<InputRow> in,
      boolean deserializeComplexMetrics
  )
  {
    return new ArrayList<>(metrics.length);
  }

  @Override
  protected List<Aggregator> concurrentGet(int offset)
  {
    // All get operations should be fine
    return aggregators.get(offset);
  }


  @Override
  protected void concurrentSet(int offset, List<Aggregator> value)
  {
    aggregators.put(offset, value);
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
  protected List<Aggregator> getAggsForRow(int rowIndex)
  {
    return concurrentGet(rowIndex);
  }

  @Override
  protected Object getAggVal(Aggregator agg, int rowIndex, int aggPosition)
  {
    return agg.get();
  }

  @Override
  public float getMetricFloatValue(int rowIndex, int aggOffset)
  {
    return concurrentGet(rowIndex).get(aggOffset).getFloat();
  }

  @Override
  public long getMetricLongValue(int rowIndex, int aggOffset)
  {
    return concurrentGet(rowIndex).get(aggOffset).getLong();
  }

  @Override
  public Object getMetricObjectValue(int rowIndex, int aggOffset)
  {
    return concurrentGet(rowIndex).get(aggOffset).get();
  }

  @Override
  protected void initializeAggs(List<Aggregator> aggs, Integer rowIndex)
  {
    // NoOp
  }

  @Override
  protected Function<ColumnSelectorFactory, Aggregator> getFactorizeFunction(final AggregatorFactory agg)
  {
    return new Function<ColumnSelectorFactory, Aggregator>()
    {
      @Nullable
      @Override
      public Aggregator apply(ColumnSelectorFactory input)
      {
        return agg.factorize(input);
      }
    };
  }

  @Override
  protected void applyAggregators(Integer rowIndex)
  {
    final List<Aggregator> aggs = aggregators.get(rowIndex);
    for (Aggregator agg : aggs) {
      synchronized (agg) {
        agg.aggregate();
      }
    }
  }

  protected static class OnHeapDimDim implements DimDim
  {
    private static final int NUM_STRIPE_BUCKETS = 64;
    private final Object[] stripeLocks = new Object[NUM_STRIPE_BUCKETS];
    private final ConcurrentMap<String, Integer> falseIds = new ConcurrentHashMap<>();
    private final Map<Integer, String> falseIdsReverse = Maps.newHashMap();
    private final AtomicReference<String[]> sortedVals = new AtomicReference<>();
    final ConcurrentMap<String, String> poorMansInterning = new ConcurrentHashMap<>();

    public OnHeapDimDim()
    {
      for (int i = 0; i < stripeLocks.length; ++i) {
        stripeLocks[i] = new Object();
      }
    }

    private int getStripeIndex(int id)
    {
      return id % NUM_STRIPE_BUCKETS;
    }

    /**
     * Returns the interned String value to allow fast comparisons using `==` instead of `.equals()`
     *
     * @see io.druid.segment.incremental.IncrementalIndexStorageAdapter.EntryHolderValueMatcherFactory#makeValueMatcher(String, String)
     */
    @Override
    public String intern(String str)
    {
      String prev = poorMansInterning.putIfAbsent(str, str);
      return prev != null ? prev : str;
    }

    public int getId(String value)
    {
      if (value == null) {
        value = "";
      }
      final Integer id = falseIds.get(value);
      return id == null ? -1 : id;
    }

    public String getValue(int id)
    {
      synchronized (stripeLocks[getStripeIndex(id)]) {
        return falseIdsReverse.get(id);
      }
    }

    public boolean contains(String value)
    {
      if(value == null){
        return false; // ConcurrentHashMaps don't like null keys
      }
      return falseIds.containsKey(value);
    }

    public int size()
    {
      return falseIds.size();
    }

    public int add(String value)
    {
      if (isClosed.get()) {
        throw new ISE("Already closed");
      }
      Integer id = falseIds.get(value);
      if (null != id) {
        return id;
      }
      id = falseIds.size();
      synchronized (stripeLocks[getStripeIndex(id)]) {
        final Integer priorId = falseIds.putIfAbsent(value, id);
        if (priorId == null) {
          // Won the race
          falseIdsReverse.put(id, value);
        } else {
          // Lost a race
          id = priorId;
        }
        return id;
      }
    }

    private final Lock sortLock = new ReentrantLock();

    public int getSortedId(String value)
    {
      assertSorted();
      return Arrays.binarySearch(sortedVals.get(), value);
    }

    public String getSortedValue(int index)
    {
      assertSorted();
      return sortedVals.get()[index];
    }

    public void sort()
    {
      if (sortedVals.get() != null) {
        return;
      }
      sortLock.lock();
      try {
        if (sortedVals.get() == null) {
          final String[] newVals = new String[falseIds.size()];

          int index = 0;
          for (String value : falseIds.keySet()) {
            newVals[index++] = value;
          }
          Arrays.sort(newVals);
          sortedVals.set(newVals);
        }
      }
      finally {
        sortLock.unlock();
      }
    }

    private void assertSorted()
    {
      if (sortedVals == null) {
        throw new ISE("Call sort() before calling the getSorted* methods.");
      }
    }

    public boolean compareCannonicalValues(String s1, String s2)
    {
      return s1 == s2;
    }

    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    @Override
    public synchronized void close() throws IOException
    {
      if (isClosed.get()) {
        throw new ISE("Already closed");
      }
      falseIds.clear();
      falseIdsReverse.clear();
      poorMansInterning.clear();
      isClosed.set(true);
    }
  }
}
