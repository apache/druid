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

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class SelectQueryEngine
{
  public Sequence<Result<SelectResultValue>> process(final SelectQuery query, final Segment segment)
  {
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    final Iterable<String> dims;
    if (query.getDimensions() == null || query.getDimensions().isEmpty()) {
      dims = adapter.getAvailableDimensions();
    } else {
      dims = query.getDimensions();
    }

    final Iterable<String> metrics;
    if (query.getMetrics() == null || query.getMetrics().isEmpty()) {
      metrics = adapter.getAvailableMetrics();
    } else {
      metrics = query.getMetrics();
    }

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query.getQuerySegmentSpec().getIntervals(),
        Filters.convertDimensionFilters(query.getDimensionsFilter()),
        query.isDescending(),
        query.getGranularity(),
        new Function<Cursor, Result<SelectResultValue>>()
        {
          @Override
          public Result<SelectResultValue> apply(Cursor cursor)
          {
            final SelectResultValueBuilder builder = new SelectResultValueBuilder(
                cursor.getTime(),
                query.getPagingSpec(),
                query.isDescending()
            );

            final LongColumnSelector timestampColumnSelector = cursor.makeLongColumnSelector(Column.TIME_COLUMN_NAME);

            final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
            for (String dim : dims) {
              // switching to using DimensionSpec for select would allow the use of extractionFn here.
              final DimensionSelector dimSelector = cursor.makeDimensionSelector(new DefaultDimensionSpec(dim, dim));
              dimSelectors.put(dim, dimSelector);
            }

            final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
            for (String metric : metrics) {
              final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
              metSelectors.put(metric, metricSelector);
            }

            final PagingOffset offset = query.getPagingOffset(segment.getIdentifier());

            cursor.advanceTo(offset.startDelta());

            for (; !cursor.isDone() && offset.hasNext(); cursor.advance(), offset.next()) {
              final Map<String, Object> theEvent = Maps.newLinkedHashMap();
              theEvent.put(EventHolder.timestampKey, new DateTime(timestampColumnSelector.get()));

              for (Map.Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
                final String dim = dimSelector.getKey();
                final DimensionSelector selector = dimSelector.getValue();

                if (selector == null) {
                  theEvent.put(dim, null);
                } else {
                  final IndexedInts vals = selector.getRow();

                  if (vals.size() == 1) {
                    final String dimVal = selector.lookupName(vals.get(0));
                    theEvent.put(dim, dimVal);
                  } else {
                    List<String> dimVals = Lists.newArrayList();
                    for (int i = 0; i < vals.size(); ++i) {
                      dimVals.add(selector.lookupName(vals.get(i)));
                    }
                    theEvent.put(dim, dimVals);
                  }
                }
              }

              for (Map.Entry<String, ObjectColumnSelector> metSelector : metSelectors.entrySet()) {
                final String metric = metSelector.getKey();
                final ObjectColumnSelector selector = metSelector.getValue();

                if (selector == null) {
                  theEvent.put(metric, null);
                } else {
                  theEvent.put(metric, selector.get());
                }
              }

              builder.addEntry(
                  new EventHolder(
                      segment.getIdentifier(),
                      offset.current(),
                      theEvent
                  )
              );
            }

            return builder.build();
          }
        }
    );
  }
}
