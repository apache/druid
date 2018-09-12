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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionIndexer;

import java.util.Map;
import java.util.Comparator;
import java.util.List;
import java.util.Iterator;
import java.util.Deque;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

/**
 */
public abstract class ExternalDataIncrementalIndex<AggregatorType> extends IncrementalIndex<AggregatorType>
{
  protected FactsHolder facts;

  /* basic constractor */
  ExternalDataIncrementalIndex(
          IncrementalIndexSchema incrementalIndexSchema,
          boolean reportParseExceptions,
          boolean sortFacts,
          int maxRowCount
  )
  {
    super(incrementalIndexSchema, reportParseExceptions, maxRowCount);
    this.facts = incrementalIndexSchema.isRollup() ? new RollupFactsHolder(sortFacts, dimsComparator(), getDimensions())
            : new PlainFactsHolder(sortFacts, dimsComparator());
  }

  protected abstract FactsHolder getFacts();

  // Note: This method needs to be thread safe.
  protected abstract AddToFactsResult addToFacts(
          boolean reportParseExceptions,
          InputRow row,
          AtomicInteger numEntries,
          AtomicLong sizeInBytes,
          IncrementalIndexRow key,
          ThreadLocal<InputRow> rowContainer,
          Supplier<InputRow> rowSupplier,
          boolean skipMaxRowsInMemoryCheck
  ) throws IndexSizeExceededException;

  @Override
  public IncrementalIndexAddResult add(InputRow row, boolean skipMaxRowsInMemoryCheck) throws IndexSizeExceededException
  {
    IncrementalIndexRowResult incrementalIndexRowResult = toIncrementalIndexRow(row);
    final AddToFactsResult addToFactsResult = addToFacts(
            reportParseExceptions,
            row,
            numEntries,
            bytesInMemory,
            incrementalIndexRowResult.getIncrementalIndexRow(),
            in,
            rowSupplier,
            skipMaxRowsInMemoryCheck
    );
    updateMaxIngestedTime(row.getTimestamp());
    ParseException parseException = getCombinedParseException(
            row,
            incrementalIndexRowResult.getParseExceptionMessages(),
            addToFactsResult.getParseExceptionMessages()
    );
    return new IncrementalIndexAddResult(addToFactsResult.getRowCount(), addToFactsResult.getBytesInMemory(), parseException);
  }

  @Override
  protected long getMinTimeMillis()
  {
    return getFacts().getMinTimeMillis();
  }

  @Override
  protected long getMaxTimeMillis()
  {
    return getFacts().getMaxTimeMillis();
  }

  protected abstract String getMetricName(int metricIndex);

  @Override
  public Iterable<IncrementalIndexRow> timeRangeIterable(
          boolean descending, long timeStart, long timeEnd)
  {
    return getFacts().timeRangeIterable(descending, timeStart, timeEnd);
  }

  @Override
  public Iterable<IncrementalIndexRow> keySet()
  {
    return getFacts().keySet();
  }

  @Override
  public Iterable<IncrementalIndexRow> persistIterable()
  {
    return getFacts().persistIterable();
  }

  @Override
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
          incrementalIndexRow -> {

            Object[] theDims = incrementalIndexRow.getDims();

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

            AggregatorType[] aggs = getAggsForRow(incrementalIndexRow);
            for (int i = 0; i < aggs.length; ++i) {
              theVals.put(getMetricName(i), getAggVal(incrementalIndexRow, i));
            }

            if (postAggs != null) {
              for (PostAggregator postAgg : postAggs) {
                theVals.put(postAgg.getName(), postAgg.compute(theVals));
              }
            }

            return new MapBasedRow(incrementalIndexRow.getTimestamp(), theVals);
          }
        );
      }
    };
  }


  interface FactsHolder
  {
    /**
     * @return the previous rowIndex associated with the specified key, or
     * {@link IncrementalIndexRow#EMPTY_ROW_INDEX} if there was no mapping for the key.
     */
    int getPriorIndex(IncrementalIndexRow key);

    long getMinTimeMillis();

    long getMaxTimeMillis();

    Iterator<IncrementalIndexRow> iterator(boolean descending);

    Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd);

    Iterable<IncrementalIndexRow> keySet();

    /**
     * Get all {@link IncrementalIndexRow} to persist, ordered with {@link Comparator<IncrementalIndexRow>}
     * @return
     */
    Iterable<IncrementalIndexRow> persistIterable();

    /**
     * @return the previous rowIndex associated with the specified key, or
     * {@link IncrementalIndexRow#EMPTY_ROW_INDEX} if there was no mapping for the key.
     */
    int putIfAbsent(IncrementalIndexRow key, int rowIndex);

    void clear();
  }

  static class RollupFactsHolder implements FactsHolder
  {
    private final boolean sortFacts;
    // Can't use Set because we need to be able to get from collection
    private final ConcurrentMap<IncrementalIndexRow, IncrementalIndexRow> facts;
    private final List<DimensionDesc> dimensionDescsList;

    RollupFactsHolder(
            boolean sortFacts,
            Comparator<IncrementalIndexRow> incrementalIndexRowComparator,
            List<DimensionDesc> dimensionDescsList
    )
    {
      this.sortFacts = sortFacts;
      if (sortFacts) {
        this.facts = new ConcurrentSkipListMap<>(incrementalIndexRowComparator);
      } else {
        this.facts = new ConcurrentHashMap<>();
      }
      this.dimensionDescsList = dimensionDescsList;
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      IncrementalIndexRow row = facts.get(key);
      return row == null ? IncrementalIndexRow.EMPTY_ROW_INDEX : row.getRowIndex();
    }

    @Override
    public long getMinTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow>) facts).firstKey().getTimestamp();
      } else {
        throw new UnsupportedOperationException("can't get minTime from unsorted facts data.");
      }
    }

    @Override
    public long getMaxTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow>) facts).lastKey().getTimestamp();
      } else {
        throw new UnsupportedOperationException("can't get maxTime from unsorted facts data.");
      }
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      if (descending && sortFacts) {
        return ((ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow>) facts).descendingMap().keySet().iterator();
      }
      return keySet().iterator();
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      if (!sortFacts) {
        throw new UnsupportedOperationException("can't get timeRange from unsorted facts data.");
      }
      IncrementalIndexRow start = new IncrementalIndexRow(timeStart, new Object[]{}, dimensionDescsList);
      IncrementalIndexRow end = new IncrementalIndexRow(timeEnd, new Object[]{}, dimensionDescsList);
      ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow> subMap =
              ((ConcurrentNavigableMap<IncrementalIndexRow, IncrementalIndexRow>) facts).subMap(start, end);
      final Map<IncrementalIndexRow, IncrementalIndexRow> rangeMap = descending ? subMap.descendingMap() : subMap;
      return rangeMap.keySet();
    }

    @Override
    public Iterable<IncrementalIndexRow> keySet()
    {
      return facts.keySet();
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      // with rollup, facts are already pre-sorted so just return keyset
      return keySet();
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      // setRowIndex() must be called before facts.putIfAbsent() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      IncrementalIndexRow prev = facts.putIfAbsent(key, key);
      return prev == null ? IncrementalIndexRow.EMPTY_ROW_INDEX : prev.getRowIndex();
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
    private final ConcurrentMap<Long, Deque<IncrementalIndexRow>> facts;

    private final Comparator<IncrementalIndexRow> incrementalIndexRowComparator;

    public PlainFactsHolder(boolean sortFacts, Comparator<IncrementalIndexRow> incrementalIndexRowComparator)
    {
      this.sortFacts = sortFacts;
      if (sortFacts) {
        this.facts = new ConcurrentSkipListMap<>();
      } else {
        this.facts = new ConcurrentHashMap<>();
      }
      this.incrementalIndexRowComparator = incrementalIndexRowComparator;
    }

    @Override
    public int getPriorIndex(IncrementalIndexRow key)
    {
      // always return EMPTY_ROW_INDEX to indicate that no prior key cause we always add new row
      return IncrementalIndexRow.EMPTY_ROW_INDEX;
    }

    @Override
    public long getMinTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>>) facts).firstKey();
      } else {
        throw new UnsupportedOperationException("can't get minTime from unsorted facts data.");
      }
    }

    @Override
    public long getMaxTimeMillis()
    {
      if (sortFacts) {
        return ((ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>>) facts).lastKey();
      } else {
        throw new UnsupportedOperationException("can't get maxTime from unsorted facts data.");
      }
    }

    @Override
    public Iterator<IncrementalIndexRow> iterator(boolean descending)
    {
      if (descending && sortFacts) {
        return timeOrderedConcat(((ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>>) facts)
                .descendingMap().values(), true).iterator();
      }
      return timeOrderedConcat(facts.values(), false).iterator();
    }

    @Override
    public Iterable<IncrementalIndexRow> timeRangeIterable(boolean descending, long timeStart, long timeEnd)
    {
      ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>> subMap =
              ((ConcurrentNavigableMap<Long, Deque<IncrementalIndexRow>>) facts).subMap(timeStart, timeEnd);
      final Map<Long, Deque<IncrementalIndexRow>> rangeMap = descending ? subMap.descendingMap() : subMap;
      return timeOrderedConcat(rangeMap.values(), descending);
    }

    private Iterable<IncrementalIndexRow> timeOrderedConcat(
            final Iterable<Deque<IncrementalIndexRow>> iterable,
            final boolean descending
    )
    {
      return () -> Iterators.concat(
              Iterators.transform(iterable.iterator(),
                input -> descending ? input.descendingIterator() : input.iterator())
      );
    }

    private Stream<IncrementalIndexRow> timeAndDimsOrderedConcat(
            final Collection<Deque<IncrementalIndexRow>> rowGroups
    )
    {
      return rowGroups.stream()
              .flatMap(Collection::stream)
              .sorted(incrementalIndexRowComparator);
    }

    private Iterable<IncrementalIndexRow> concat(
            final Iterable<Deque<IncrementalIndexRow>> iterable,
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
    public Iterable<IncrementalIndexRow> keySet()
    {
      return timeOrderedConcat(facts.values(), false);
    }

    @Override
    public Iterable<IncrementalIndexRow> persistIterable()
    {
      return () -> timeAndDimsOrderedConcat(facts.values()).iterator();
    }

    @Override
    public int putIfAbsent(IncrementalIndexRow key, int rowIndex)
    {
      Long time = key.getTimestamp();
      Deque<IncrementalIndexRow> rows = facts.get(time);
      if (rows == null) {
        facts.putIfAbsent(time, new ConcurrentLinkedDeque<>());
        // in race condition, rows may be put by other thread, so always get latest status from facts
        rows = facts.get(time);
      }
      // setRowIndex() must be called before rows.add() for visibility of rowIndex from concurrent readers.
      key.setRowIndex(rowIndex);
      rows.add(key);
      // always return EMPTY_ROW_INDEX to indicate that we always add new row
      return IncrementalIndexRow.EMPTY_ROW_INDEX;
    }

    @Override
    public void clear()
    {
      facts.clear();
    }
  }

}
