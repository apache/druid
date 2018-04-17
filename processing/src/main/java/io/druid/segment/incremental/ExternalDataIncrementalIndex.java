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
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;

import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.segment.DimensionHandler;
import io.druid.segment.DimensionIndexer;

import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public abstract class ExternalDataIncrementalIndex<AggregatorType> extends IncrementalIndex<AggregatorType>
{

  /* basic constractor */
  ExternalDataIncrementalIndex(
      IncrementalIndexSchema incrementalIndexSchema,
      boolean deserializeComplexMetrics,
      boolean reportParseExceptions,
      boolean concurrentEventAdd
  )
  {
    super(incrementalIndexSchema, deserializeComplexMetrics, reportParseExceptions, concurrentEventAdd);
  }

  protected abstract FactsHolder getFacts();

  // Note: This method needs to be thread safe.
  protected abstract Integer addToFacts(
          AggregatorFactory[] metrics,
          boolean deserializeComplexMetrics,
          boolean reportParseExceptions,
          InputRow row,
          AtomicInteger numEntries,
          TimeAndDims key,
          ThreadLocal<InputRow> rowContainer,
          Supplier<InputRow> rowSupplier,
          boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException;

  @Override
  public int add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    TimeAndDims key = toTimeAndDims(row);
    final int rv = addToFacts(
            metrics,
            deserializeComplexMetrics,
            reportParseExceptions,
            row,
            numEntries,
            key,
            in,
            rowSupplier,
            skipMaxRowsInMemoryCheck
    );
    updateMaxIngestedTime(row.getTimestamp());
    return rv;
  }

  protected long getMinTimeMillis()
  {
    return getFacts().getMinTimeMillis();
  }

  protected long getMaxTimeMillis()
  {
    return getFacts().getMaxTimeMillis();
  }

  public Iterable<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> timeRangeIterable(
      boolean descending, long timeStart, long timeEnd)
  {
    return getFacts().timeRangeIterable(descending, timeStart, timeEnd);
  }

  public Iterable<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> keySet()
  {
    return getFacts().keySet();
  }

  public Iterable<Row> iterableWithPostAggregations(final List<PostAggregator> postAggs, final boolean descending)
  {
    return new Iterable<Row>()
    {
      @Override
      public Iterator<Row> iterator()
      {
        final List<DimensionDesc> dimensions = getDimensions();

        return Iterators.transform(
            getFacts().iterator(descending),
            timeAndDims -> {
              final int rowOffset = timeAndDims.getRowIndex();

              Object[] theDims = timeAndDims.getDims();

              Map<String, Object> theVals = Maps.newLinkedHashMap();
              for (int i = 0; i < theDims.length; ++i) {
                Object dim = theDims[i];
                DimensionDesc dimensionDesc = dimensions.get(i);
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
                Object rowVals = indexer.convertUnsortedEncodedKeyComponentToActualArrayOrList(dim, DimensionIndexer.LIST);
                theVals.put(dimensionName, rowVals);
              }

              AggregatorType[] aggs = getAggsForRow(timeAndDims);
              for (int i = 0; i < aggs.length; ++i) {
                theVals.put(metrics[i].getName(), getAggVal(aggs[i], timeAndDims, i));
              }

              if (postAggs != null) {
                for (PostAggregator postAgg : postAggs) {
                  theVals.put(postAgg.getName(), postAgg.compute(theVals));
                }
              }

              return new MapBasedRow(timeAndDims.getTimestamp(), theVals);
            }
        );
      }
    };
  }


  interface FactsHolder
  {
    /**
     * @return the previous rowIndex associated with the specified key, or
     * {@code TimeAndDims#EMPTY_ROW_INDEX} if there was no mapping for the key.
     */
    int getPriorIndex(io.druid.segment.incremental.IncrementalIndex.TimeAndDims key);

    long getMinTimeMillis();

    long getMaxTimeMillis();

    Iterator<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> iterator(boolean descending);

    Iterable<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> timeRangeIterable(boolean descending, long timeStart, long timeEnd);

    Iterable<TimeAndDims> keySet();

    /**
     * @return the previous rowIndex associated with the specified key, or
     * {@code TimeAndDims#EMPTY_ROW_INDEX} if there was no mapping for the key.
     */
    int putIfAbsent(io.druid.segment.incremental.IncrementalIndex.TimeAndDims key, int rowIndex);

    void clear();
  }

  static class RollupFactsHolder implements FactsHolder
  {
    private final boolean sortFacts;
    // Can't use Set because we need to be able to get from collection
    private final ConcurrentMap<io.druid.segment.incremental.IncrementalIndex.TimeAndDims, io.druid.segment.incremental.IncrementalIndex.TimeAndDims>
        facts;
    private final List<io.druid.segment.incremental.IncrementalIndex.DimensionDesc> dimensionDescsList;

    public RollupFactsHolder(boolean sortFacts, Comparator<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> timeAndDimsComparator, List<io.druid.segment.incremental.IncrementalIndex.DimensionDesc> dimensionDescsList)
    {
      this.sortFacts = sortFacts;
      if (sortFacts) {
        this.facts = new ConcurrentSkipListMap<>(timeAndDimsComparator);
      } else {
        this.facts = new ConcurrentHashMap<>();
      }
      this.dimensionDescsList = dimensionDescsList;
    }

    @Override
    public int getPriorIndex(io.druid.segment.incremental.IncrementalIndex.TimeAndDims key)
    {
      io.druid.segment.incremental.IncrementalIndex.TimeAndDims timeAndDims = facts.get(key);
      return timeAndDims == null ?
          io.druid.segment.incremental.IncrementalIndex.TimeAndDims.EMPTY_ROW_INDEX :
          timeAndDims.getRowIndex();
    }

    @Override
    public long getMinTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<io.druid.segment.incremental.IncrementalIndex.TimeAndDims, io.druid.segment.incremental.IncrementalIndex.TimeAndDims>) facts).firstKey().getTimestamp();
      } else {
        throw new UnsupportedOperationException("can't get minTime from unsorted facts data.");
      }
    }

    @Override
    public long getMaxTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<io.druid.segment.incremental.IncrementalIndex.TimeAndDims, io.druid.segment.incremental.IncrementalIndex.TimeAndDims>) facts).lastKey().getTimestamp();
      } else {
        throw new UnsupportedOperationException("can't get maxTime from unsorted facts data.");
      }
    }

    @Override
    public Iterator<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> iterator(boolean descending)
    {
      if (descending && sortFacts) {
        return ((ConcurrentNavigableMap<io.druid.segment.incremental.IncrementalIndex.TimeAndDims, io.druid.segment.incremental.IncrementalIndex.TimeAndDims>) facts).descendingMap().keySet().iterator();
      }
      return keySet().iterator();
    }

    @Override
    public Iterable<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      if (!sortFacts) {
        throw new UnsupportedOperationException("can't get timeRange from unsorted facts data.");
      }
      io.druid.segment.incremental.IncrementalIndex.TimeAndDims
          start = new io.druid.segment.incremental.IncrementalIndex.TimeAndDims(timeStart, new Object[]{}, dimensionDescsList);
      io.druid.segment.incremental.IncrementalIndex.TimeAndDims
          end = new io.druid.segment.incremental.IncrementalIndex.TimeAndDims(timeEnd, new Object[]{}, dimensionDescsList);
      ConcurrentNavigableMap<io.druid.segment.incremental.IncrementalIndex.TimeAndDims, io.druid.segment.incremental.IncrementalIndex.TimeAndDims> subMap =
          ((ConcurrentNavigableMap<io.druid.segment.incremental.IncrementalIndex.TimeAndDims, io.druid.segment.incremental.IncrementalIndex.TimeAndDims>) facts).subMap(start, end);
      final Map<io.druid.segment.incremental.IncrementalIndex.TimeAndDims, io.druid.segment.incremental.IncrementalIndex.TimeAndDims> rangeMap = descending ? subMap.descendingMap() : subMap;
      return rangeMap.keySet();
    }

    @Override
    public Iterable<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> keySet()
    {
      return facts.keySet();
    }

    @Override
    public int putIfAbsent(io.druid.segment.incremental.IncrementalIndex.TimeAndDims key, int rowIndex)
    {
      // setRowIndex() must be called before facts.putIfAbsent() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      io.druid.segment.incremental.IncrementalIndex.TimeAndDims prev = facts.putIfAbsent(key, key);
      return prev == null ? io.druid.segment.incremental.IncrementalIndex.TimeAndDims.EMPTY_ROW_INDEX : prev.getRowIndex();
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }

  static class PlainFactsHolder implements FactsHolder
  {
    private final boolean sortFacts;
    private final ConcurrentMap<Long, Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims>> facts;

    public PlainFactsHolder(boolean sortFacts)
    {
      this.sortFacts = sortFacts;
      if (sortFacts) {
        this.facts = new ConcurrentSkipListMap<>();
      } else {
        this.facts = new ConcurrentHashMap<>();
      }
    }

    @Override
    public int getPriorIndex(io.druid.segment.incremental.IncrementalIndex.TimeAndDims key)
    {
      // always return EMPTY_ROW_INDEX to indicate that no prior key cause we always add new row
      return io.druid.segment.incremental.IncrementalIndex.TimeAndDims.EMPTY_ROW_INDEX;
    }

    @Override
    public long getMinTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<Long, Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims>>) facts).firstKey();
      } else {
        throw new UnsupportedOperationException("can't get minTime from unsorted facts data.");
      }
    }

    @Override
    public long getMaxTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<Long, Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims>>) facts).lastKey();
      } else {
        throw new UnsupportedOperationException("can't get maxTime from unsorted facts data.");
      }
    }

    @Override
    public Iterator<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> iterator(boolean descending)
    {
      if (descending && sortFacts) {
        return concat(((ConcurrentNavigableMap<Long, Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims>>) facts)
            .descendingMap().values(), true).iterator();
      }
      return concat(facts.values(), false).iterator();
    }

    @Override
    public Iterable<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      ConcurrentNavigableMap<Long, Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims>> subMap =
          ((ConcurrentNavigableMap<Long, Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims>>) facts).subMap(timeStart, timeEnd);
      final Map<Long, Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims>> rangeMap = descending ? subMap.descendingMap() : subMap;
      return concat(rangeMap.values(), descending);
    }

    private Iterable<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> concat(
        final Iterable<Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims>> iterable,
        final boolean descending
    )
    {
      return () -> Iterators.concat(
          Iterators.transform(
              iterable.iterator(),
              input -> descending ? input.descendingIterator() : input.iterator()
          )
      );
    }

    @Override
    public Iterable<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> keySet()
    {
      return concat(facts.values(), false);
    }

    @Override
    public int putIfAbsent(io.druid.segment.incremental.IncrementalIndex.TimeAndDims key, int rowIndex)
    {
      Long time = key.getTimestamp();
      Deque<io.druid.segment.incremental.IncrementalIndex.TimeAndDims> rows = facts.get(time);
      if (rows == null) {
        facts.putIfAbsent(time, new ConcurrentLinkedDeque<>());
        // in race condition, rows may be put by other thread, so always get latest status from facts
        rows = facts.get(time);
      }
      // setRowIndex() must be called before rows.add() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      rows.add(key);
      // always return EMPTY_ROW_INDEX to indicate that we always add new row
      return io.druid.segment.incremental.IncrementalIndex.TimeAndDims.EMPTY_ROW_INDEX;
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }

}
