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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.BaseSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.Filter;
import io.druid.query.select.SelectQueryEngine;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.filter.Filters;
import io.druid.timeline.DataSegmentUtils;
import org.joda.time.Interval;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ScanQueryEngine
{
  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final Map<String, Object> responseContext
  )
  {
    if (responseContext.get("count") != null) {
      int count = (int) responseContext.get("count");
      if (count >= query.getLimit()) {
        return Sequences.empty();
      }
    }
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    // at the point where this code is called, only one datasource should exist.
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    List<String> allDims = Lists.newLinkedList(adapter.getAvailableDimensions());
    List<String> allMetrics = Lists.newLinkedList(adapter.getAvailableMetrics());
    if (query.getColumns() != null && !query.getColumns().isEmpty()) {
      allDims.retainAll(query.getColumns());
      allMetrics.retainAll(query.getColumns());
    }
    final List<DimensionSpec> dims = DefaultDimensionSpec.toSpec(allDims);
    final List<String> metrics = allMetrics;

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    // should be rewritten with given interval
    final String segmentId = DataSegmentUtils.withInterval(dataSource, segment.getIdentifier(), intervals.get(0));

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));

    if (responseContext.get("count") == null) {
      responseContext.put("count", 0);
    }
    final int limit = query.getLimit() - (int) responseContext.get("count");
    return Sequences.concat(
        Sequences.map(
            adapter.makeCursors(
                filter,
                intervals.get(0),
                QueryGranularities.ALL,
                query.isDescending()
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

                        final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
                        for (DimensionSpec dim : dims) {
                          final DimensionSelector dimSelector = cursor.makeDimensionSelector(dim);
                          dimSelectors.put(dim.getOutputName(), dimSelector);
                        }

                        final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
                        for (String metric : metrics) {
                          final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
                          metSelectors.put(metric, metricSelector);
                        }
                        final int batchSize = query.getBatchSize();
                        return new Iterator<ScanResultValue>()
                        {
                          private int offset = 0;

                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone() && offset < limit;
                          }

                          @Override
                          public ScanResultValue next()
                          {
                            int lastOffset = offset;
                            Object events = null;
                            String resultFormat = query.getResultFormat();
                            if (ScanQuery.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
                              throw new UnsupportedOperationException("valueVector is not supported now");
                            } else {
                              events = rowsToList();
                            }
                            responseContext.put("count", (int) responseContext.get("count") + (offset - lastOffset));
                            return new ScanResultValue(segmentId, lastOffset, events);
                          }

                          @Override
                          public void remove()
                          {
                            throw new UnsupportedOperationException();
                          }

                          private Object rowsToList()
                          {
                            int i = 0;
                            List<Map<String, Object>> events = Lists.newArrayListWithCapacity(batchSize);
                            for (; !cursor.isDone()
                                   && i < batchSize
                                   && offset < limit; cursor.advance(), i++, offset++) {
                              final Map<String, Object> theEvent = SelectQueryEngine.singleEvent(
                                  ScanResultValue.timestampKey,
                                  timestampColumnSelector,
                                  dimSelectors,
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
