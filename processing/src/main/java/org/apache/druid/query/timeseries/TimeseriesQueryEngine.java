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

package org.apache.druid.query.timeseries;

import com.google.common.base.Function;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryRunnerHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.filter.Filters;

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
    final int limit = query.getLimit();
    Sequence<Result<TimeseriesResultValue>> result = generateTimeseriesResult(adapter, query, filter);
    if (limit < Integer.MAX_VALUE) {
      return result.limit(limit);
    }
    return result;
  }

  private Sequence<Result<TimeseriesResultValue>> generateTimeseriesResult(StorageAdapter adapter, TimeseriesQuery query, Filter filter)
  {
    return QueryRunnerHelper.makeCursorBasedQuery(
        adapter,
        query.getQuerySegmentSpec().getIntervals(),
        filter,
        query.getVirtualColumns(),
        query.isDescending(),
        query.getGranularity(),
        new Function<Cursor, Result<TimeseriesResultValue>>()
        {
          private final boolean skipEmptyBuckets = query.isSkipEmptyBuckets();
          private final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();

          @Override
          public Result<TimeseriesResultValue> apply(Cursor cursor)
          {
            if (skipEmptyBuckets && cursor.isDone()) {
              return null;
            }

            Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
            String[] aggregatorNames = new String[aggregatorSpecs.size()];

            for (int i = 0; i < aggregatorSpecs.size(); i++) {
              aggregators[i] = aggregatorSpecs.get(i).factorize(cursor.getColumnSelectorFactory());
              aggregatorNames[i] = aggregatorSpecs.get(i).getName();
            }

            try {
              while (!cursor.isDone()) {
                for (Aggregator aggregator : aggregators) {
                  aggregator.aggregate();
                }
                cursor.advance();
              }
              TimeseriesResultBuilder bob = new TimeseriesResultBuilder(cursor.getTime());

              for (int i = 0; i < aggregatorSpecs.size(); i++) {
                bob.addMetric(aggregatorNames[i], aggregators[i]);
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
