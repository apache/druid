package com.metamx.druid.query;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.StorageAdapter;
import com.metamx.druid.aggregation.Aggregator;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.index.brita.Filter;
import com.metamx.druid.index.v1.processing.Cursor;
import com.metamx.druid.result.Result;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.List;

/**
 */
public class QueryRunnerFactories
{
  private static final Logger log = new Logger(QueryRunnerFactories.class);

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

  public static <T> Iterable<Result<T>> makeCursorBasedQuery(
      final StorageAdapter adapter,
      List<Interval> queryIntervals,
      Filter filter,
      QueryGranularity granularity,
      Function<Cursor, Result<T>> mapFn
  )
  {
    Preconditions.checkArgument(
        queryIntervals.size() == 1, "Can only handle a single interval, got[%s]", queryIntervals
    );

    return FunctionalIterable
        .create(adapter.makeCursors(filter, queryIntervals.get(0), granularity))
        .transform(
            new Function<Cursor, Cursor>()
            {
              @Override
              public Cursor apply(@Nullable Cursor input)
              {
                log.debug("Running over cursor[%s]", adapter.getInterval(), input.getTime());
                return input;
              }
            }
        )
        .keep(mapFn);
  }
}
