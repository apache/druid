/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.ISE;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;

import java.util.Arrays;

public class TimewarpOperator<T> implements PostProcessingOperator<T>
{
  private final Interval dataInterval;
  private final Period period;
  private final DateTime origin;

  @JsonCreator
  public TimewarpOperator(
      @JsonProperty("dataInterval") Interval dataInterval,
      @JsonProperty("period") Period period,
      @JsonProperty("origin") DateTime origin
  )
  {
    this.origin = origin;
    this.dataInterval = dataInterval;
    this.period = period;
  }


  @Override
  public QueryRunner<T> postProcess(final QueryRunner<T> baseRunner)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(Query<T> query)
      {
        final long t = DateTime.now().getMillis();
        final long originMillis = origin.getMillis();

        // this will fail for periods that do not map to millis (e.g. P1M)
        final long periodMillis = period.toStandardDuration().getMillis();

        // map time t into the last `period` fully contained within dataInterval
        long startMillis = dataInterval.getEnd().minus(period).getMillis();
        startMillis -= startMillis % periodMillis - originMillis % periodMillis;
        final long offset = startMillis + (t % periodMillis) - (originMillis % periodMillis) - t;

        final Interval interval = query.getIntervals().get(0);
        final Interval modifiedInterval = new Interval(
            interval.getStartMillis() + offset,
            Math.min(interval.getEndMillis() + offset, t)
        );
        return Sequences.map(
            baseRunner.run(
                query.withQuerySegmentSpec(new MultipleIntervalSegmentSpec(Arrays.asList(modifiedInterval)))
            ),
            new Function<T, T>()
            {
              @Override
              public T apply(T input)
              {
                if (input instanceof Result) {
                  Result res = (Result) input;
                  Object value = res.getValue();
                  if (value instanceof TimeBoundaryResultValue) {
                    TimeBoundaryResultValue boundary = (TimeBoundaryResultValue) value;
                    value = new TimeBoundaryResultValue(
                        ImmutableMap.of(
                            TimeBoundaryQuery.MIN_TIME, boundary.getMinTime().minus(offset),
                            TimeBoundaryQuery.MAX_TIME, new DateTime(Math.min(boundary.getMaxTime().getMillis() - offset, t))
                        )
                    );
                  }
                  return (T) new Result(res.getTimestamp().minus(offset), value);
                } else if (input instanceof MapBasedRow) {
                  MapBasedRow row = (MapBasedRow) input;
                  return (T) new MapBasedRow(row.getTimestamp().minus(offset), row.getEvent());
                }
                throw new ISE("Don't know how to timewarp results of type[%s]", input.getClass());
              }
            }
        );
      }
    };
  }
}
