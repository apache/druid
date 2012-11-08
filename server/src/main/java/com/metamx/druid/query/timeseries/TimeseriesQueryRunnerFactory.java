/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.timeseries;

import com.google.common.base.Function;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.TimeseriesResultBuilder;
import com.metamx.druid.aggregation.Aggregator;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.index.brita.Filters;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.query.ChainedExecutionQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerHelper;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeseriesResultValue;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;

/**
 */
public class TimeseriesQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private static final TimeseriesQueryQueryToolChest toolChest = new TimeseriesQueryQueryToolChest();

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> createRunner(final StorageAdapter adapter)
  {
    return new QueryRunner<Result<TimeseriesResultValue>>()
    {
      @Override
      public Sequence<Result<TimeseriesResultValue>> run(Query<Result<TimeseriesResultValue>> input)
      {
        if (!(input instanceof TimeseriesQuery)) {
          throw new ISE("Got a [%s] which isn't a %s", input.getClass(), GroupByQuery.class);
        }

        final TimeseriesQuery query = (TimeseriesQuery) input;

        return new BaseSequence<Result<TimeseriesResultValue>, Iterator<Result<TimeseriesResultValue>>>(
            new BaseSequence.IteratorMaker<Result<TimeseriesResultValue>, Iterator<Result<TimeseriesResultValue>>>()
            {
              @Override
              public Iterator<Result<TimeseriesResultValue>> make()
              {
                return QueryRunnerHelper.makeCursorBasedQuery(
                    adapter,
                    query.getQuerySegmentSpec().getIntervals(),
                    Filters.convertDimensionFilters(query.getDimensionsFilter()),
                    query.getGranularity(),
                    new Function<Cursor, Result<TimeseriesResultValue>>()
                    {
                      private final List<AggregatorFactory> aggregatorSpecs = query.getAggregatorSpecs();
                      private final List<PostAggregator> postAggregatorSpecs = query.getPostAggregatorSpecs();

                      @Override
                      public Result<TimeseriesResultValue> apply(Cursor cursor)
                      {
                        Aggregator[] aggregators = QueryRunnerHelper.makeAggregators(cursor, aggregatorSpecs);

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

                        for (PostAggregator postAgg : postAggregatorSpecs) {
                          bob.addMetric(postAgg);
                        }

                        return bob.build();
                      }
                    }
                ).iterator();
              }

              @Override
              public void cleanup(Iterator<Result<TimeseriesResultValue>> toClean)
              {

              }
            }
        );
      }
    };
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TimeseriesResultValue>>> queryRunners
  )
  {
    return new ChainedExecutionQueryRunner<Result<TimeseriesResultValue>>(
        queryExecutor, toolChest.getOrdering(), queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> getToolchest()
  {
    return toolChest;
  }
}
