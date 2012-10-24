package com.metamx.druid.query;

import com.google.common.collect.Lists;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.Yielders;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.druid.Query;
import com.metamx.druid.result.BySegmentResultValueClass;
import com.metamx.druid.result.Result;
import org.joda.time.DateTime;

import java.util.List;

/**
 */
public class BySegmentQueryRunner<T> implements QueryRunner<T>
{
  private final String segmentIdentifier;
  private final DateTime timestamp;
  private final QueryRunner<T> base;

  public BySegmentQueryRunner(
      String segmentIdentifier,
      DateTime timestamp,
      QueryRunner<T> base
  )
  {
    this.segmentIdentifier = segmentIdentifier;
    this.timestamp = timestamp;
    this.base = base;
  }

  @Override
  @SuppressWarnings("unchecked")
  public Sequence<T> run(final Query<T> query)
  {
    if (Boolean.parseBoolean(query.getContextValue("bySegment"))) {
      final Sequence<T> baseSequence = base.run(query);
      return new Sequence<T>()
      {
        @Override
        public <OutType> OutType accumulate(OutType initValue, Accumulator<OutType, T> accumulator)
        {
          List<T> results = Sequences.toList(baseSequence, Lists.<T>newArrayList());

          return accumulator.accumulate(
              initValue,
              (T) new Result<BySegmentResultValueClass<T>>(
                  timestamp,
                  new BySegmentResultValueClass<T>(
                      results,
                      segmentIdentifier,
                      query.getIntervals().get(0).toString()
                  )
              )
          );
        }

        @Override
        public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
        {
          List<T> results = Sequences.toList(baseSequence, Lists.<T>newArrayList());

          final OutType retVal = accumulator.accumulate(
              initValue,
              (T) new Result<BySegmentResultValueClass<T>>(
                  timestamp,
                  new BySegmentResultValueClass<T>(
                      results,
                      segmentIdentifier,
                      query.getIntervals().get(0).toString()
                  )
              )
          );

          return Yielders.done(retVal, null);
        }
      };
    }

    return base.run(query);
  }
}
