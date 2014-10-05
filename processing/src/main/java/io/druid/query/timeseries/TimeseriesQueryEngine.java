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

package io.druid.query.timeseries;

import com.google.common.base.Function;
import com.metamx.common.guava.Sequence;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.Cursor;
import io.druid.segment.SegmentMissingException;
import io.druid.segment.StorageAdapter;
import io.druid.segment.filter.Filters;

import java.util.List;

/**
 */
public class TimeseriesQueryEngine
{
  public Sequence<Result<TimeseriesResultValue>> process(final TimeseriesQuery query, final StorageAdapter adapter)
  {
    if (adapter == null) {
      throw new SegmentMissingException(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query.getQuerySegmentSpec().getIntervals(),
        Filters.convertDimensionFilters(query.getDimensionsFilter()),
        query.getGranularity(),
        new Function<Cursor, Result<TimeseriesResultValue>>()
        {
          private final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

          @Override
          public Result<TimeseriesResultValue> apply(Cursor cursor)
          {
            Aggregator[] aggregators = QueryRunnerHelper.makeAggregators(cursor, aggregatorSpecs);
            try {
              while (!cursor.isDone()) {
                for (Aggregator aggregator : aggregators) {
                  aggregator.aggregate();
                }
                cursor.advance();
              }

              TimeseriesResultBuilder bob = new TimeseriesResultBuilder(cursor.getTime());

              for (Aggregator aggregator : aggregators) {
                bob.addMetric(aggregator);
              }

              Result<TimeseriesResultValue> retVal = bob.build();
              return retVal;
            }
            finally {
              // cleanup
              for (Aggregator agg : aggregators) {
                agg.close();
              }
            }
          }
        }
    );
  }
}
