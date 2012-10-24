package com.metamx.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Query;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.result.BySegmentResultValueClass;
import com.metamx.druid.result.Result;

import javax.annotation.Nullable;

/**
 */
public class FinalizeResultsQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;
  private final QueryToolChest<T, Query<T>> toolChest;

  public FinalizeResultsQueryRunner(
      QueryRunner<T> baseRunner,
      QueryToolChest<T, Query<T>> toolChest
  )
  {
    this.baseRunner = baseRunner;
    this.toolChest = toolChest;
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final boolean isBySegment = Boolean.parseBoolean(query.getContextValue("bySegment"));
    final boolean shouldFinalize = Boolean.parseBoolean(query.getContextValue("finalize", "true"));
    if (shouldFinalize) {
      Function<T, T> finalizerFn;
      if (isBySegment) {
        finalizerFn = new Function<T, T>()
        {
          final Function<T, T> baseFinalizer = toolChest.makeMetricManipulatorFn(
              query,
              new MetricManipulationFn()
              {
                @Override
                public Object manipulate(AggregatorFactory factory, Object object)
                {
                  return factory.finalizeComputation(factory.deserialize(object));
                }
              }
          );

          @Override
          @SuppressWarnings("unchecked")
          public T apply(@Nullable T input)
          {
            Result<BySegmentResultValueClass<T>> result = (Result<BySegmentResultValueClass<T>>) input;
            BySegmentResultValueClass<T> resultsClass = result.getValue();

            return (T) new Result<BySegmentResultValueClass>(
                result.getTimestamp(),
                new BySegmentResultValueClass(
                    Lists.transform(resultsClass.getResults(), baseFinalizer),
                    resultsClass.getSegmentId(),
                    resultsClass.getIntervalString()
                )
            );
          }
        };
      }
      else {
        finalizerFn = toolChest.makeMetricManipulatorFn(
            query,
            new MetricManipulationFn()
            {
              @Override
              public Object manipulate(AggregatorFactory factory, Object object)
              {
                return factory.finalizeComputation(object);
              }
            }
        );
      }

      return Sequences.map(
          baseRunner.run(query.withOverriddenContext(ImmutableMap.of("finalize", "false"))),
          finalizerFn
      );
    }
    return baseRunner.run(query);
  }
}
