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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import io.druid.data.input.MapBasedRow;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.ChainedExecutionQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryWatcher;
import io.druid.query.Result;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.groupby.RowBasedColumnSelectorFactory;
import io.druid.segment.Segment;
import io.druid.segment.StorageAdapter;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

/**
 */
public class TimeseriesQueryRunnerFactory
    implements QueryRunnerFactory<Result<TimeseriesResultValue>, TimeseriesQuery>
{
  private final TimeseriesQueryQueryToolChest toolChest;
  private final TimeseriesQueryEngine engine;
  private final QueryWatcher queryWatcher;

  @Inject
  public TimeseriesQueryRunnerFactory(
      TimeseriesQueryQueryToolChest toolChest,
      TimeseriesQueryEngine engine,
      QueryWatcher queryWatcher
  )
  {
    this.toolChest = toolChest;
    this.engine = engine;
    this.queryWatcher = queryWatcher;
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> createRunner(final Segment segment)
  {
    return new TimeseriesQueryRunner(engine, segment.asStorageAdapter());
  }

  @Override
  public QueryRunner<Result<TimeseriesResultValue>> mergeRunners(
      ExecutorService queryExecutor, Iterable<QueryRunner<Result<TimeseriesResultValue>>> queryRunners
  )
  {
    if (!queryRunners.iterator().hasNext()) {
      return new EmptyQueryRunner();
    }
    return new ChainedExecutionQueryRunner<>(
        queryExecutor, queryWatcher, queryRunners
    );
  }

  @Override
  public QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> getToolchest()
  {
    return toolChest;
  }

  private static class TimeseriesQueryRunner implements QueryRunner<Result<TimeseriesResultValue>>
  {
    private final TimeseriesQueryEngine engine;
    private final StorageAdapter adapter;

    private TimeseriesQueryRunner(TimeseriesQueryEngine engine, StorageAdapter adapter)
    {
      this.engine = engine;
      this.adapter = adapter;
    }

    @Override
    public Sequence<Result<TimeseriesResultValue>> run(
        QueryPlus<Result<TimeseriesResultValue>> queryPlus,
        Map<String, Object> responseContext
    )
    {
      Query<Result<TimeseriesResultValue>> input = queryPlus.getQuery();
      if (!(input instanceof TimeseriesQuery)) {
        throw new ISE("Got a [%s] which isn't a %s", input.getClass(), TimeseriesQuery.class);
      }

      return engine.process((TimeseriesQuery) input, adapter);
    }
  }

  public static class EmptyQueryRunner implements QueryRunner<Result<TimeseriesResultValue>>
  {
    @Override
    public Sequence<Result<TimeseriesResultValue>> run(QueryPlus<Result<TimeseriesResultValue>> queryPlus, Map<String, Object> responseContext)
    {
      if (queryPlus.getQuery() instanceof TimeseriesQuery) {
        final TimeseriesQuery ts = (TimeseriesQuery) queryPlus.getQuery();
        if (!ts.isSkipEmptyBuckets()) {
          Iterable<Interval> iterable = Iterables.concat(ts.getIntervals()
                                                           .stream()
                                                           .map(i -> ts.getGranularity().getIterable(i))
                                                           .collect(Collectors.toList()));
          if (ts.isDescending()) {
            iterable = Lists.reverse(ImmutableList.copyOf(iterable));
          }
          Function fn = new Function<Interval, Result<TimeseriesResultValue>>()
          {
            private final List<AggregatorFactory> aggregatorSpecs = ts.getAggregatorSpecs();
            @Override
            public Result<TimeseriesResultValue> apply(Interval interval)
            {

              Aggregator[] aggregators = new Aggregator[aggregatorSpecs.size()];
              String[] aggregatorNames = new String[aggregatorSpecs.size()];
              for (int i = 0; i < aggregatorSpecs.size(); i++) {
                aggregators[i] = aggregatorSpecs.get(i)
                                                .factorize(RowBasedColumnSelectorFactory.create(() -> new MapBasedRow(
                                                    null,
                                                    null
                                                ), null));
                aggregatorNames[i] = aggregatorSpecs.get(i).getName();
              }
              TimeseriesResultBuilder bob = new TimeseriesResultBuilder(interval.getStart());
              for (int i = 0; i < aggregatorSpecs.size(); i++) {
                bob.addMetric(aggregatorNames[i], aggregators[i]);
                aggregators[i].close();
              }
              Result<TimeseriesResultValue> retVal = bob.build();
              return retVal;
            }
          };

          return Sequences.map(Sequences.simple(iterable), fn);

        }
        return Sequences.empty();
      }
      return Sequences.empty();
    }
  }
}
