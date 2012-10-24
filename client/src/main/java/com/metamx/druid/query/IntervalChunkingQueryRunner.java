package com.metamx.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.PeriodGranularity;
import com.metamx.druid.Query;
import com.metamx.druid.query.segment.MultipleIntervalSegmentSpec;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 */
public class IntervalChunkingQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> baseRunner;
  private final Period period;

  public IntervalChunkingQueryRunner(QueryRunner<T> baseRunner, Period period)
  {
    this.baseRunner = baseRunner;
    this.period = period;
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    return Sequences.concat(
        FunctionalIterable
            .create(query.getIntervals())
            .transformCat(
                new Function<Interval, Iterable<Interval>>()
                {
                  @Override
                  public Iterable<Interval> apply(@Nullable Interval input)
                  {
                    return splitInterval(input);
                  }
                }
            )
            .transform(
                new Function<Interval, Sequence<T>>()
                {
                  @Override
                  public Sequence<T> apply(Interval singleInterval)
                  {
                    return baseRunner.run(
                        query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Arrays.asList(singleInterval)))
                    );
                  }
                }
            )
    );
  }

  private Iterable<Interval> splitInterval(Interval interval)
  {
    if (interval.getEndMillis() == interval.getStartMillis()) {
      return Lists.newArrayList(interval);
    }

    List<Interval> intervals = Lists.newArrayList();
    Iterator<Long> timestamps = new PeriodGranularity(period, null, null).iterable(
        interval.getStartMillis(),
        interval.getEndMillis()
    ).iterator();

    long start = Math.max(timestamps.next(), interval.getStartMillis());
    while (timestamps.hasNext()) {
      long end = timestamps.next();
      intervals.add(new Interval(start, end));
      start = end;
    }

    if (start < interval.getEndMillis()) {
      intervals.add(new Interval(start, interval.getEndMillis()));
    }

    return intervals;
  }
}
