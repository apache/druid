package com.metamx.druid.query.timeboundary;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.metamx.common.guava.MergeSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Query;
import com.metamx.druid.collect.OrderedMergeSequence;
import com.metamx.druid.query.BySegmentSkippingQueryRunner;
import com.metamx.druid.query.CacheStrategy;
import com.metamx.druid.query.MetricManipulationFn;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeBoundaryResultValue;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.List;

/**
 */
public class TimeBoundaryQueryQueryToolChest
    implements QueryToolChest<Result<TimeBoundaryResultValue>, TimeBoundaryQuery>
{
  private static final byte TIMEBOUNDARY_QUERY = 0x3;

  private static final TypeReference<Result<TimeBoundaryResultValue>> TYPE_REFERENCE = new TypeReference<Result<TimeBoundaryResultValue>>()
  {
  };

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> mergeResults(
      final QueryRunner<Result<TimeBoundaryResultValue>> runner
  )
  {
    return new BySegmentSkippingQueryRunner<Result<TimeBoundaryResultValue>>(runner)
    {
      @Override
      protected Sequence<Result<TimeBoundaryResultValue>> doRun(
          QueryRunner<Result<TimeBoundaryResultValue>> baseRunner, Query<Result<TimeBoundaryResultValue>> input
      )
      {
        TimeBoundaryQuery query = (TimeBoundaryQuery) input;
        return Sequences.simple(
            query.mergeResults(
                Sequences.toList(baseRunner.run(query), Lists.<Result<TimeBoundaryResultValue>>newArrayList())
            )
        );
      }
    };
  }

  @Override
  public Sequence<Result<TimeBoundaryResultValue>> mergeSequences(Sequence<Sequence<Result<TimeBoundaryResultValue>>> seqOfSequences)
  {
    return new OrderedMergeSequence<Result<TimeBoundaryResultValue>>(getOrdering(), seqOfSequences);
  }

  @Override
  public ServiceMetricEvent.Builder makeMetricBuilder(TimeBoundaryQuery query)
  {
    return new ServiceMetricEvent.Builder()
        .setUser2(query.getDataSource())
        .setUser4(query.getType())
        .setUser6("false");
  }

  @Override
  public Function<Result<TimeBoundaryResultValue>, Result<TimeBoundaryResultValue>> makeMetricManipulatorFn(
      TimeBoundaryQuery query, MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<TimeBoundaryResultValue>> getResultTypeReference()
  {
    return TYPE_REFERENCE;
  }

  @Override
  public CacheStrategy<Result<TimeBoundaryResultValue>, TimeBoundaryQuery> getCacheStrategy(TimeBoundaryQuery query)
  {
    return new CacheStrategy<Result<TimeBoundaryResultValue>, TimeBoundaryQuery>()
    {
      @Override
      public byte[] computeCacheKey(TimeBoundaryQuery query)
      {
        return ByteBuffer.allocate(2)
                         .put(TIMEBOUNDARY_QUERY)
                         .put(query.getCacheKey())
                         .array();
      }

      @Override
      public Function<Result<TimeBoundaryResultValue>, Object> prepareForCache()
      {
        return new Function<Result<TimeBoundaryResultValue>, Object>()
        {
          @Override
          public Object apply(Result<TimeBoundaryResultValue> input)
          {
            return Lists.newArrayList(input.getTimestamp().getMillis(), input.getValue());
          }
        };
      }

      @Override
      public Function<Object, Result<TimeBoundaryResultValue>> pullFromCache()
      {
        return new Function<Object, Result<TimeBoundaryResultValue>>()
        {
          @Override
          @SuppressWarnings("unchecked")
          public Result<TimeBoundaryResultValue> apply(@Nullable Object input)
          {
            List<Object> result = (List<Object>) input;

            return new Result<TimeBoundaryResultValue>(
                new DateTime(result.get(0)),
                new TimeBoundaryResultValue(result.get(1))
            );
          }
        };
      }

      @Override
      public Sequence<Result<TimeBoundaryResultValue>> mergeSequences(Sequence<Sequence<Result<TimeBoundaryResultValue>>> seqOfSequences)
      {
        return new MergeSequence<Result<TimeBoundaryResultValue>>(getOrdering(), seqOfSequences);
      }
    };
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> preMergeQueryDecoration(QueryRunner<Result<TimeBoundaryResultValue>> runner)
  {
    return runner;
  }

  @Override
  public QueryRunner<Result<TimeBoundaryResultValue>> postMergeQueryDecoration(QueryRunner<Result<TimeBoundaryResultValue>> runner)
  {
    return runner;
  }

  public Ordering<Result<TimeBoundaryResultValue>> getOrdering()
  {
    return Ordering.natural();
  }
}
