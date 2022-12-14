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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.collections.QueueBasedSorter;
import org.apache.druid.collections.Sorter;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.bitmap.WrappedImmutableRoaringBitmap;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.NumericColumn;
import org.apache.druid.segment.data.Offset;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.roaringbitmap.buffer.MutableRoaringBitmap;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class QueryableIndexOrderbyRunner
{
  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      @Nullable final QueryMetrics<?> queryMetrics,
      QueryableIndex index,
      final List<String> allColumns,
      final Filter filter,
      final List<Interval> intervals
  )
  {
    final SegmentId segmentId = segment.getId();
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
    boolean descending = query.getOrderBys()
                              .stream()
                              .anyMatch(o -> ColumnHolder.TIME_COLUMN_NAME.equals(o.getColumnName())
                                             && ScanQuery.Order.DESCENDING.name().equals(o.getOrder().name()));
    if (filterBitmap == null) {
      baseOffset = descending
                   ? new SimpleDescendingOffset(index.getNumRows())
                   : new SimpleAscendingOffset(index.getNumRows());
    } else {
      baseOffset = BitmapOffset.of(filterBitmap, descending, index.getNumRows());
    }

    Granularity gran = Granularities.ALL;
    final NumericColumn timestamps = (NumericColumn) columnCache.getColumn(ColumnHolder.TIME_COLUMN_NAME);
    final DateTime minTime = DateTimes.utc(timestamps.getLongSingleValueRow(0));
    final DateTime maxTime = DateTimes.utc(timestamps.getLongSingleValueRow(timestamps.length() - 1));

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

                      final ColumnSelectorFactory finalColumnSelectorFactory = columnSelectorFactory;
                      List<ColumnValueSelector> columnValueSelectors = orderByDims.stream()
                                                                                  .map(d -> finalColumnSelectorFactory.makeColumnValueSelector(d))
                                                                                  .collect(Collectors.toList());

                      int[] sortColumnIdxs = new int[orderByDims.size()];
                      for (int i = 0; i < orderByDims.size(); i++) {
                        sortColumnIdxs[i] = i + 1;
                      }

                      Sorter<Object> sorter = new QueueBasedSorter<>(limit, query.getGenericResultOrdering(sortColumnIdxs));
                      while (iterativeOffset.withinBounds()) {
                        int rowId = iterativeOffset.getOffset();
                        final Object[] theEvent = new Object[columnValueSelectors.size() + 1];
                        int j = 0;
                        theEvent[j++] = rowId;
                        for (ColumnValueSelector selector : columnValueSelectors) {
                          //The strings will be sorted based on the dictionary Id
                          theEvent[j++] = selector.getObjectOrDictionaryId();
                        }
                        sorter.add(theEvent);
                        iterativeOffset.increment();
                      }


                      final List<Object[]> sortedElements = new ArrayList<>(sorter.size());
                      Iterators.addAll(sortedElements, sorter.drainElement());
                      MutableRoaringBitmap mutableBitmap = new MutableRoaringBitmap();
                      sortedElements.forEach(sortedElement -> mutableBitmap.add((Integer) sortedElement[0]));
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
                        final Object[] theEvent = new Object[columnValueSelectors.size()];
                        for (int k = 0; k < columnValueSelectors.size(); k++) {
                          theEvent[k] = columnValueSelectors.get(k).getObject();
                        }
                        sortedElements.set(i++, theEvent);
                        selectOffset.increment();
                      }
                      return new ScanResultValue(segmentId.toString(), allColumns, sortedElements);
                    }).withBaggage(closer);
  }

}
