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
package io.druid.query.scan;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.ColumnSelectorPlus;
import io.druid.query.QueryContexts;
import io.druid.query.QueryInterruptedException;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.Filter;
import io.druid.query.select.SelectQueryEngine;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionHandlerUtils;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.VirtualColumns;
import io.druid.segment.column.Column;
import io.druid.segment.filter.Filters;
import org.joda.time.Interval;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

public class ScanQueryEngine
{
  private static final SelectQueryEngine.SelectStrategyFactory STRATEGY_FACTORY = new SelectQueryEngine.SelectStrategyFactory();
  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final Map<String, Object> responseContext
  )
  {
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

    List<String> allDims = Lists.newLinkedList(adapter.getAvailableDimensions());
    List<String> allMetrics = Lists.newLinkedList(adapter.getAvailableMetrics());
    final List<String> allColumns = Lists.newLinkedList();
    if (query.getColumns() != null && !query.getColumns().isEmpty()) {
      if (!query.getColumns().contains(ScanResultValue.timestampKey)) {
        allColumns.add(ScanResultValue.timestampKey);
      }
      allColumns.addAll(query.getColumns());
      allDims.retainAll(query.getColumns());
      allMetrics.retainAll(query.getColumns());
    } else {
      if (!allDims.contains(ScanResultValue.timestampKey)) {
        allColumns.add(ScanResultValue.timestampKey);
      }
      allColumns.addAll(allDims);
      allColumns.addAll(allMetrics);
    }
    final List<DimensionSpec> dims = DefaultDimensionSpec.toSpec(allDims);
    final List<String> metrics = allMetrics;

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    final String segmentId = segment.getIdentifier();

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));

    if (responseContext.get(ScanQueryRunnerFactory.CTX_COUNT) == null) {
      responseContext.put(ScanQueryRunnerFactory.CTX_COUNT, 0L);
    }
    final long limit = query.getLimit() - (long) responseContext.get(ScanQueryRunnerFactory.CTX_COUNT);
    return Sequences.concat(
        Sequences.map(
            adapter.makeCursors(
                filter,
                intervals.get(0),
                VirtualColumns.EMPTY,
                Granularities.ALL,
                query.isDescending(),
                null
            ),
            new Function<Cursor, Sequence<ScanResultValue>>()
            {
              @Override
              public Sequence<ScanResultValue> apply(final Cursor cursor)
              {
                return new BaseSequence<>(
                    new BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>()
                    {
                      @Override
                      public Iterator<ScanResultValue> make()
                      {
                        final LongColumnSelector timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);

                        final List<ColumnSelectorPlus<SelectQueryEngine.SelectColumnSelectorStrategy>> selectorPlusList = Arrays.asList(
                            DimensionHandlerUtils.createColumnSelectorPluses(
                                STRATEGY_FACTORY,
                                Lists.newArrayList(dims),
                                cursor
                            )
                        );

                        final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
                        for (String metric : metrics) {
                          final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
                          metSelectors.put(metric, metricSelector);
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
                            if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
                              throw new QueryInterruptedException(new TimeoutException());
                            }
                            long lastOffset = offset;
                            Object events = null;
                            String resultFormat = query.getResultFormat();
                            if (ScanQuery.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
                              throw new UnsupportedOperationException("valueVector is not supported now");
                            } else if (ScanQuery.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat)) {
                              events = rowsToCompactedList();
                            } else {
                              events = rowsToList();
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
                            return new ScanResultValue(segmentId, allColumns, events);
                          }

                          @Override
                          public void remove()
                          {
                            throw new UnsupportedOperationException();
                          }

                          private Object rowsToCompactedList()
                          {
                            return Lists.transform(
                                (List<Map<String, Object>>) rowsToList(),
                                new Function<Map<String, Object>, Object>()
                                {
                                  @Override
                                  public Object apply(Map<String, Object> input)
                                  {
                                    List eventValues = Lists.newArrayListWithExpectedSize(allColumns.size());
                                    for (String expectedColumn : allColumns) {
                                      eventValues.add(input.get(expectedColumn));
                                    }
                                    return eventValues;
                                  }
                                }
                            );
                          }

                          private Object rowsToList()
                          {
                            List<Map<String, Object>> events = Lists.newArrayListWithCapacity(batchSize);
                            for (int i = 0; !cursor.isDone()
                                   && i < batchSize
                                   && offset < limit; cursor.advance(), i++, offset++) {
                              final Map<String, Object> theEvent = SelectQueryEngine.singleEvent(
                                  ScanResultValue.timestampKey,
                                  timestampColumnSelector,
                                  selectorPlusList,
                                  metSelectors
                              );
                              events.add(theEvent);
                            }
                            return events;
                          }

                          private Object rowsToValueVector()
                          {
                            // only support list now, we can support ValueVector or Arrow in future
                            return rowsToList();
                          }
                        };
                      }

                      @Override
                      public void cleanup(Iterator<ScanResultValue> iterFromMake)
                      {
                      }
                    }
                );
              }
            }
        )
    );
  }
}
