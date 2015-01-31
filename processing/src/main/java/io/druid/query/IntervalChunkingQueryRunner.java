/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.granularity.PeriodGranularity;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    if (period.getMillis() == 0) {
      return baseRunner.run(query, responseContext);
    }

    return Sequences.concat(
        FunctionalIterable
            .create(query.getIntervals())
            .transformCat(
                new Function<Interval, Iterable<Interval>>()
                {
                  @Override
                  public Iterable<Interval> apply(Interval input)
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
                        query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Arrays.asList(singleInterval))),
                        responseContext
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
