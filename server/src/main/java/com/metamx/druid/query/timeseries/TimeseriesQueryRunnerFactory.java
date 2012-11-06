package com.metamx.druid.query.timeseries;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.Query;
import com.metamx.druid.SearchResultBuilder;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.TimeseriesResultBuilder;
import com.metamx.druid.aggregation.Aggregator;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.index.brita.Filters;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.query.ChainedExecutionQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerFactories;
import com.metamx.druid.query.QueryRunnerFactory;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.query.timeboundary.TimeBoundaryQuery;
import com.metamx.druid.query.timeboundary.TimeBoundaryQueryQueryToolChest;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeBoundaryResultValue;
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
                return QueryRunnerFactories.makeCursorBasedQuery(
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
                        Aggregator[] aggregators = QueryRunnerFactories.makeAggregators(cursor, aggregatorSpecs);

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
