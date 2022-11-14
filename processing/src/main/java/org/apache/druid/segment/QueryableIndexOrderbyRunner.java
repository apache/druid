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

package org.apache.druid.segment;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.collections.QueueBasedSorter;
import org.apache.druid.collections.Sorter;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.NumericColumn;

import org.apache.druid.segment.data.Offset;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryableIndexOrderbyRunner
{
  static final String LEGACY_TIMESTAMP_KEY = "timestamp";

  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final ResponseContext responseContext,
      @Nullable final QueryMetrics<?> queryMetrics,
      QueryableIndex index
  )
  {
    if (index != null && index.isFromTombstone()) {
      return Sequences.empty();
    }
    // "legacy" should be non-null due to toolChest.mergeResults
    final boolean legacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");
    final Long numScannedRows = responseContext.getRowScanCount();
    if (numScannedRows != null && numScannedRows >= query.getScanRowsLimit() && query.getTimeOrder()
                                                                                     .equals(ScanQuery.Order.NONE)) {
      return Sequences.empty();
    }
    final boolean hasTimeout = query.context().hasTimeout();
    final Long timeoutAt = responseContext.getTimeoutTime();

    if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
      throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", query.getId()));
    }

    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final List<String> allColumns = new ArrayList<>();

    if (query.getColumns() != null && !query.getColumns().isEmpty()) {
      if (legacy && !query.getColumns().contains(LEGACY_TIMESTAMP_KEY)) {
        allColumns.add(LEGACY_TIMESTAMP_KEY);
      }

      // Unless we're in legacy mode, allColumns equals query.getColumns() exactly. This is nice since it makes
      // the compactedList form easier to use.
      allColumns.addAll(query.getColumns());
    } else {
      final Set<String> availableColumns = Sets.newLinkedHashSet(
          Iterables.concat(
              Collections.singleton(legacy ? LEGACY_TIMESTAMP_KEY : ColumnHolder.TIME_COLUMN_NAME),
              Iterables.transform(
                  Arrays.asList(query.getVirtualColumns().getVirtualColumns()),
                  VirtualColumn::getOutputName
              ),
              adapter.getAvailableDimensions(),
              adapter.getAvailableMetrics()
          )
      );

      allColumns.addAll(availableColumns);

      if (legacy) {
        allColumns.remove(ColumnHolder.TIME_COLUMN_NAME);
      }
    }

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);
    final SegmentId segmentId = segment.getId();
    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));

    VirtualColumns virtualColumns = query.getVirtualColumns();

    final Closer closer = Closer.create();
    // Column caches shared amongst all cursors in this sequence.
    final ColumnCache columnCache = new ColumnCache(index, closer);

    final ColumnSelectorColumnIndexSelector bitmapIndexSelector = new ColumnSelectorColumnIndexSelector(
        index.getBitmapFactoryForDimensions(),
        virtualColumns,
        columnCache
    );

    final FilterAnalysis filterAnalysis = FilterAnalysis.analyzeFilter(
        filter,
        bitmapIndexSelector,
        queryMetrics,
        index.getNumRows()
    );
    final ImmutableBitmap filterBitmap = filterAnalysis.getPreFilterBitmap();
    final Filter postFilter = filterAnalysis.getPostFilter();
    Offset baseOffset;
    boolean descending = false;
    if (filterBitmap == null) {
      baseOffset = descending
                   ? new SimpleDescendingOffset(index.getNumRows())
                   : new SimpleAscendingOffset(index.getNumRows());
    } else {
      baseOffset = BitmapOffset.of(filterBitmap, descending, index.getNumRows());
    }

    Granularity gran = Granularities.ALL;
    final DateTime minTime;
    final DateTime maxTime;
    final NumericColumn timestamps = (NumericColumn) columnCache.getColumn(ColumnHolder.TIME_COLUMN_NAME);
    minTime = DateTimes.utc(timestamps.getLongSingleValueRow(0));
    maxTime = DateTimes.utc(timestamps.getLongSingleValueRow(timestamps.length() - 1));

    final Interval dataInterval = new Interval(minTime, gran.bucketEnd(maxTime));

    final Interval actualInterval = intervals.get(0).overlap(dataInterval);

    if (actualInterval == null) {
      return Sequences.empty();
    }

    Iterable<Interval> iterable = gran.getIterable(dataInterval);
    if (descending) {
      iterable = Lists.reverse(ImmutableList.copyOf(iterable));
    }

    return Sequences.simple(iterable)
                    .map(inputInterval -> {
                      final long timeStart = Math.max(inputInterval.getStartMillis(), inputInterval.getStartMillis());
                      final long timeEnd = Math.min(
                          inputInterval.getEndMillis(),
                          gran.increment(inputInterval.getStartMillis())
                      );
                      //Fast filtering based on time range
                      if (descending) {
                        for (; baseOffset.withinBounds(); baseOffset.increment()) {
                          if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) < timeEnd) {
                            break;
                          }
                        }
                      } else {
                        for (; baseOffset.withinBounds(); baseOffset.increment()) {
                          if (timestamps.getLongSingleValueRow(baseOffset.getOffset()) >= timeStart) {
                            break;
                          }
                        }
                      }

                      final Offset offset = descending ?
                                            new QueryableIndexCursorSequenceBuilder.DescendingTimestampCheckingOffset(
                                                baseOffset,
                                                timestamps,
                                                timeStart,
                                                segment.asStorageAdapter().getMinTime().getMillis() >= timeStart
                                            ) :
                                            new QueryableIndexCursorSequenceBuilder.AscendingTimestampCheckingOffset(
                                                baseOffset,
                                                timestamps,
                                                timeEnd,
                                                segment.asStorageAdapter().getMaxTime().getMillis() < timeEnd
                                            );

                      final Offset baseCursorOffset = offset.clone();

                      ColumnSelectorFactory columnSelectorFactory = new QueryableIndexColumnSelectorFactory(
                          virtualColumns,
                          descending,
                          baseCursorOffset.getBaseReadableOffset(),
                          columnCache
                      );

                      Offset iterativeOffset;
                      if (postFilter == null) {
                        iterativeOffset = baseCursorOffset;
                      } else {
                        iterativeOffset = new FilteredOffset(
                            baseCursorOffset,
                            columnSelectorFactory,
                            descending,
                            postFilter,
                            bitmapIndexSelector
                        );
                      }

                      int limit;
                      if (query.getScanRowsLimit() > Integer.MAX_VALUE) {
                        limit = Integer.MAX_VALUE;
                      } else {
                        limit = Math.toIntExact(query.getScanRowsLimit());
                      }

                      List<String> orderByDims = query.getOrderBys()
                                                      .stream()
                                                      .map(o -> o.getColumnName())
                                                      .collect(Collectors.toList());

                      ColumnSelectorFactory finalColumnSelectorFactory = columnSelectorFactory;
                      List<ColumnValueSelector> columnValueSelectors = orderByDims.stream()
                                                                                  .map(d -> finalColumnSelectorFactory.makeColumnValueSelector(d))
                                                                                  .collect(Collectors.toList());

                      List<Integer> sortColumnIdxs = new ArrayList<>(orderByDims.size());
                      for (int i = 0; i < orderByDims.size(); i++) {
                        sortColumnIdxs.add(i + 1);
                      }

                      Sorter<Object> sorter = new QueueBasedSorter<>(limit, getOrderByNoneTimeResultOrdering(query, sortColumnIdxs));
                      while (iterativeOffset.withinBounds()) {
                        int rowId = iterativeOffset.getOffset();
                        final List<Object> theEvent = new ArrayList<>(columnValueSelectors.size());
                        theEvent.add(rowId);
                        for (ColumnValueSelector selector : columnValueSelectors) {
                          //The strings will be sorted based on the dictionary Id
                          theEvent.add(selector.getObjectOrDictionaryId());
                        }
                        sorter.add(theEvent);
                        iterativeOffset.increment();
                      }


                      final List<List<Object>> sortedElements = new ArrayList<>(sorter.size());
                      Iterators.addAll(sortedElements, sorter.drainElement());
                      MutableRoaringBitmap mutableBitmap = new MutableRoaringBitmap();
                      sortedElements.forEach(rowId -> mutableBitmap.add((Integer) rowId.get(0)));
                      ImmutableBitmap bitmap = new WrappedImmutableRoaringBitmap(mutableBitmap.toImmutableRoaringBitmap());
                      Offset selectOffset = BitmapOffset.of(bitmap, descending, sortedElements.size());

                      //Random access based on rowId
                      QueryableIndexColumnSelectorFactory columnValueSelectorFactory = new QueryableIndexColumnSelectorFactory(
                          virtualColumns,
                          descending,
                          selectOffset.getBaseReadableOffset(),
                          columnCache
                      );

                      columnValueSelectors = allColumns.stream()
                                                       .map(d -> columnValueSelectorFactory.makeColumnValueSelector(d))
                                                       .collect(Collectors.toList());

                      int i = 0;
                      while (selectOffset.withinBounds()) {
                        final List<Object> theEvent = new ArrayList<>(columnValueSelectors.size());
                        for (ColumnValueSelector selector : columnValueSelectors) {
                          theEvent.add(selector.getObject());
                        }
                        sortedElements.set(i++, theEvent);
                        selectOffset.increment();
                      }
                      return new ScanResultValue(segmentId.toString(), allColumns, sortedElements);
                    }).withBaggage(closer);
  }

  public Ordering<List<Object>> getOrderByNoneTimeResultOrdering(ScanQuery query, List<Integer> sortColumnIdxs)
  {
    List<String> orderByDirection = query.getOrderBys().stream()
                                         .map(orderBy -> orderBy.getOrder().toString())
                                         .collect(Collectors.toList());


    Ordering<Comparable>[] orderings = new Ordering[orderByDirection.size()];
    for (int i = 0; i < orderByDirection.size(); i++) {
      orderings[i] = ScanQuery.Order.ASCENDING.equals(ScanQuery.Order.fromString(orderByDirection.get(i)))
                     ? Comparators.naturalNullsFirst()
                     : Comparators.<Comparable>naturalNullsFirst().reverse();
    }

    Comparator<List<Object>> comparator = new Comparator<List<Object>>()
    {
      @Override
      public int compare(
          List<Object> o1,
          List<Object> o2
      )
      {
        for (int i = 0; i < sortColumnIdxs.size(); i++) {
          int compare = orderings[i].compare(
              (Comparable) o1.get(sortColumnIdxs.get(i)),
              (Comparable) o2.get(sortColumnIdxs.get(i))
          );
          if (compare != 0) {
            return compare;
          }
        }
        return 0;
      }
    };
    return Ordering.from(comparator);
  }
}
