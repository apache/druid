/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.select;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.segment.Cursor;
import io.druid.segment.DimensionSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import io.druid.segment.TimestampColumnSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.filter.Filters;
import org.joda.time.DateTime;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class SelectQueryEngine
{
  public Sequence<Result<SelectResultValue>> process(final SelectQuery query, final Segment segment)
  {
    return new BaseSequence<>(
        new BaseSequence.IteratorMaker<Result<SelectResultValue>, Iterator<Result<SelectResultValue>>>()
        {
          @Override
          public Iterator<Result<SelectResultValue>> make()
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
                query.getGranularity(),
                new Function<Cursor, Result<SelectResultValue>>()
                {
                  @Override
                  public Result<SelectResultValue> apply(Cursor cursor)
                  {
                    final SelectResultValueBuilder builder = new SelectResultValueBuilder(
                        cursor.getTime(),
                        query.getPagingSpec()
                             .getThreshold()
                    );

                    final TimestampColumnSelector timestampColumnSelector = cursor.makeTimestampColumnSelector();

                    final Map<String, DimensionSelector> dimSelectors = Maps.newHashMap();
                    for (String dim : dims) {
                      final DimensionSelector dimSelector = cursor.makeDimensionSelector(dim);
                      dimSelectors.put(dim, dimSelector);
                    }

                    final Map<String, ObjectColumnSelector> metSelectors = Maps.newHashMap();
                    for (String metric : metrics) {
                      final ObjectColumnSelector metricSelector = cursor.makeObjectColumnSelector(metric);
                      metSelectors.put(metric, metricSelector);
                    }

                    int startOffset;
                    if (query.getPagingSpec().getPagingIdentifiers() == null) {
                      startOffset = 0;
                    } else {
                      Integer offset = query.getPagingSpec().getPagingIdentifiers().get(segment.getIdentifier());
                      startOffset = (offset == null) ? 0 : offset;
                    }

                    cursor.advanceTo(startOffset);

                    int offset = 0;
                    while (!cursor.isDone() && offset < query.getPagingSpec().getThreshold()) {
                      final Map<String, Object> theEvent = Maps.newLinkedHashMap();
                      theEvent.put(EventHolder.timestampKey, new DateTime(timestampColumnSelector.getTimestamp()));

                      for (Map.Entry<String, DimensionSelector> dimSelector : dimSelectors.entrySet()) {
                        final String dim = dimSelector.getKey();
                        final DimensionSelector selector = dimSelector.getValue();
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

                      for (Map.Entry<String, ObjectColumnSelector> metSelector : metSelectors.entrySet()) {
                        final String metric = metSelector.getKey();
                        final ObjectColumnSelector selector = metSelector.getValue();
                        theEvent.put(metric, selector.get());
                      }

                      builder.addEntry(
                          new EventHolder(
                              segment.getIdentifier(),
                              startOffset + offset,
                              theEvent
                          )
                      );
                      cursor.advance();
                      offset++;
                    }

                    return builder.build();
                  }
                }
            ).iterator();
          }

          @Override
          public void cleanup(Iterator<Result<SelectResultValue>> toClean)
          {
            // https://github.com/metamx/druid/issues/128
            while (toClean.hasNext()) {
              toClean.next();
            }
          }
        }
    );
  }
}
