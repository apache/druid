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

package io.druid.query.timeseries;

import com.google.common.base.Function;
import io.druid.java.util.common.guava.Sequence;
import io.druid.query.QueryRunnerHelper;
import io.druid.query.Result;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.filter.Filter;
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

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getDimensionsFilter()));

    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query.getQuerySegmentSpec().getIntervals(),
        filter,
        query.isDescending(),
        query.getGranularity(),
        new Function<Cursor, Result<TimeseriesResultValue>>()
        {
          private final boolean skipEmptyBuckets = query.isSkipEmptyBuckets();
          private final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

          @Override
          public Result<TimeseriesResultValue> apply(Cursor cursor)
          {
            Aggregator[] aggregators = QueryRunnerHelper.makeAggregators(cursor, aggregatorSpecs);

            if (skipEmptyBuckets && cursor.isDone()) {
              return null;
            }

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
