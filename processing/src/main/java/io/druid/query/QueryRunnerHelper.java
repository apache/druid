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

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.guava.ResourceClosingSequence;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.filter.Filter;
import io.druid.segment.Cursor;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 */
public class QueryRunnerHelper
{
  private static final Logger log = new Logger(QueryRunnerHelper.class);

  public static Aggregator[] makeAggregators(Cursor cursor, List<AggregatorFactory> aggregatorSpecs)
  {
    Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
    int aggregatorIndex = 0;
    for (AggregatorFactory spec : aggregatorSpecs) {
      aggregators[aggregatorIndex] = spec.factorize(cursor);
      ++aggregatorIndex;
    }
    return aggregators;
  }

  public static <T> Sequence<Result<T>> makeCursorBasedQuery(
      final StorageAdapter adapter,
      List<Interval> queryIntervals,
      Filter filter,
      boolean descending,
      QueryGranularity granularity,
      final Function<Cursor, Result<T>> mapFn
  )
  {
    Preconditions.checkArgument(
        queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
    );

    return Sequences.filter(
        Sequences.map(
            adapter.makeCursors(filter, queryIntervals.get(0), granularity, descending),
            new Function<Cursor, Result<T>>()
            {
              @Override
              public Result<T> apply(Cursor input)
              {
                log.debug("Running over cursor[%s]", adapter.getInterval(), input.getTime());
                return mapFn.apply(input);
              }
            }
        ),
        Predicates.<Result<T>>notNull()
    );
  }

  public static <T>  QueryRunner<T> makeClosingQueryRunner(final QueryRunner<T> runner, final Closeable closeable){
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query, Map<String, Object> responseContext)
      {
        return new ResourceClosingSequence<>(runner.run(query, responseContext), closeable);
      }
    };
  }
}
