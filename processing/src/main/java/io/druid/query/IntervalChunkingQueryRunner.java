/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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
