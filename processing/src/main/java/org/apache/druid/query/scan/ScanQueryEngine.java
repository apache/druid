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

package org.apache.druid.query.scan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeoutException;

public class ScanQueryEngine
{
  private static final String LEGACY_TIMESTAMP_KEY = "timestamp";

  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final Map<String, Object> responseContext
  )
  {
    // "legacy" should be non-null due to toolChest.mergeResults
    final boolean legacy = Preconditions.checkNotNull(query.isLegacy(), "WTF?! Expected non-null legacy");

    if (responseContext.get(ScanQueryRunnerFactory.CTX_COUNT) != null) {
      long count = (long) responseContext.get(ScanQueryRunnerFactory.CTX_COUNT);
      if (count >= query.getLimit()) {
        return Sequences.empty();
      }
    }
    final boolean hasTimeout = QueryContexts.hasTimeout(query);
    final long timeoutAt = (long) responseContext.get(ScanQueryRunnerFactory.CTX_TIMEOUT_AT);
    final long start = System.currentTimeMillis();
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

    if (responseContext.get(ScanQueryRunnerFactory.CTX_COUNT) == null) {
      responseContext.put(ScanQueryRunnerFactory.CTX_COUNT, 0L);
    }
    final long limit = query.getLimit() - (long) responseContext.get(ScanQueryRunnerFactory.CTX_COUNT);
    return Sequences.concat(
            adapter
                .makeCursors(
                    filter,
                    intervals.get(0),
                    query.getVirtualColumns(),
                    Granularities.ALL,
                    query.isDescending(),
                    null
                )
                .map(cursor -> new BaseSequence<>(
                    new BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>()
                    {
                      @Override
                      public Iterator<ScanResultValue> make()
                      {
                        final List<BaseObjectColumnValueSelector> columnSelectors = new ArrayList<>(allColumns.size());

                        for (String column : allColumns) {
                          final BaseObjectColumnValueSelector selector;

                          if (legacy && LEGACY_TIMESTAMP_KEY.equals(column)) {
                            selector = cursor.getColumnSelectorFactory()
                                             .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
                          } else {
                            selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
                          }

                          columnSelectors.add(selector);
                        }

                        final int batchSize = query.getBatchSize();
                        return new Iterator<ScanResultValue>()
                        {
                          private long offset = 0;

                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone() && offset < limit;
                          }

                          @Override
                          public ScanResultValue next()
                          {
                            if (!hasNext()) {
                              throw new NoSuchElementException();
                            }
                            if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
                              throw new QueryInterruptedException(new TimeoutException());
                            }
                            final long lastOffset = offset;
                            final Object events;
                            final String resultFormat = query.getResultFormat();
                            if (ScanQuery.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat)) {
                              events = rowsToCompactedList();
                            } else if (ScanQuery.RESULT_FORMAT_LIST.equals(resultFormat)) {
                              events = rowsToList();
                            } else {
                              throw new UOE("resultFormat[%s] is not supported", resultFormat);
                            }
                            responseContext.put(
                                ScanQueryRunnerFactory.CTX_COUNT,
                                (long) responseContext.get(ScanQueryRunnerFactory.CTX_COUNT) + (offset - lastOffset)
                            );
                            if (hasTimeout) {
                              responseContext.put(
                                  ScanQueryRunnerFactory.CTX_TIMEOUT_AT,
                                  timeoutAt - (System.currentTimeMillis() - start)
                              );
                            }
                            return new ScanResultValue(segmentId.toString(), allColumns, events);
                          }

                          @Override
                          public void remove()
                          {
                            throw new UnsupportedOperationException();
                          }

                          private List<Object> rowsToCompactedList()
                          {
                            final List<Object> events = new ArrayList<>(batchSize);
                            final long iterLimit = Math.min(limit, offset + batchSize);
                            for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
                              final List<Object> theEvent = new ArrayList<>(allColumns.size());
                              for (int j = 0; j < allColumns.size(); j++) {
                                theEvent.add(getColumnValue(j));
                              }
                              events.add(theEvent);
                            }
                            return events;
                          }

                          private List<Map<String, Object>> rowsToList()
                          {
                            List<Map<String, Object>> events = Lists.newArrayListWithCapacity(batchSize);
                            final long iterLimit = Math.min(limit, offset + batchSize);
                            for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
                              final Map<String, Object> theEvent = new LinkedHashMap<>();
                              for (int j = 0; j < allColumns.size(); j++) {
                                theEvent.put(allColumns.get(j), getColumnValue(j));
                              }
                              events.add(theEvent);
                            }
                            return events;
                          }

                          private Object getColumnValue(int i)
                          {
                            final BaseObjectColumnValueSelector selector = columnSelectors.get(i);
                            final Object value;

                            if (legacy && allColumns.get(i).equals(LEGACY_TIMESTAMP_KEY)) {
                              value = DateTimes.utc((long) selector.getObject());
                            } else {
                              value = selector == null ? null : selector.getObject();
                            }

                            return value;
                          }
                        };
                      }

                      @Override
                      public void cleanup(Iterator<ScanResultValue> iterFromMake)
                      {
                      }
                    }
            ))
    );
  }
}
