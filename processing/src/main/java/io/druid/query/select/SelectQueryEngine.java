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
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.filter.Filter;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.column.Column;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import io.druid.timeline.DataSegmentUtils;
import org.joda.time.DateTime;
import org.joda.time.Interval;

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

    // at the point where this code is called, only one datasource should exist.
    String dataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    final Iterable<DimensionSpec> dims;
    if (query.getDimensions() == null || query.getDimensions().isEmpty()) {
      dims = DefaultDimensionSpec.toSpec(adapter.getAvailableDimensions());
    } else {
      dims = query.getDimensions();
    }

    final Iterable<String> metrics;
    if (query.getMetrics() == null || query.getMetrics().isEmpty()) {
      metrics = adapter.getAvailableMetrics();
    } else {
      metrics = query.getMetrics();
    }
    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);

    // should be rewritten with given interval
    final String segmentId = DataSegmentUtils.withInterval(dataSource, segment.getIdentifier(), intervals.get(0));

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query.getQuerySegmentSpec().getIntervals(),
        filter,
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
            for (DimensionSpec dim : dims) {
              final DimensionSelector dimSelector = cursor.makeDimensionSelector(dim);
              dimSelectors.put(dim.getOutputName(), dimSelector);
              builder.addDimension(dim.getOutputName());
            }

            final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
            for (String metric : metrics) {
              final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
              metSelectors.put(metric, metricSelector);
              builder.addMetric(metric);
            }

            final PagingOffset offset = query.getPagingOffset(segmentId);

            cursor.advanceTo(offset.startDelta());

            int lastOffset = offset.startOffset();
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
                      segmentId,
                      lastOffset = offset.current(),
                      theEvent
                  )
              );
            }

            builder.finished(segmentId, lastOffset);

            return builder.build();
          }
        }
    );
  }
}
