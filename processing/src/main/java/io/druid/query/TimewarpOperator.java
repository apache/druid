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

/**
 * TimewarpOperator is an example post-processing operator that maps current time
 * to the latest period ending withing the specified data interval and truncates
 * the query interval to discard data that would be mapped to the future.
 *
 */
public class TimewarpOperator<T> implements PostProcessingOperator<T>
{
  private final Interval dataInterval;
  private final long periodMillis;
  private final long originMillis;

  /**
   *
   * @param dataInterval interval containing the actual data
   * @param period time will be offset by a multiple of the given period
   *               until there is at least a full period ending within the data interval
   * @param origin origin to be used to align time periods
   *               (e.g. to determine on what day of the week a weekly period starts)
   */
  @JsonCreator
  public TimewarpOperator(
      @JsonProperty("dataInterval") Interval dataInterval,
      @JsonProperty("period") Period period,
      @JsonProperty("origin") DateTime origin
  )
  {
    this.originMillis = origin.getMillis();
    this.dataInterval = dataInterval;
    // this will fail for periods that do not map to millis (e.g. P1M)
    this.periodMillis = period.toStandardDuration().getMillis();
  }


  @Override
  public QueryRunner<T> postProcess(QueryRunner<T> baseQueryRunner)
  {
    return postProcess(baseQueryRunner, DateTime.now().getMillis());
  }

  public QueryRunner<T> postProcess(final QueryRunner<T> baseRunner, final long now)
  {
    return new QueryRunner<T>()
    {
      @Override
      public Sequence<T> run(final Query<T> query)
      {
        final long offset = computeOffset(now);

        final Interval interval = query.getIntervals().get(0);
        final Interval modifiedInterval = new Interval(
            Math.min(interval.getStartMillis() + offset, now + offset),
            Math.min(interval.getEndMillis() + offset, now + offset)
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

                    DateTime minTime = null;
                    try{
                      minTime = boundary.getMinTime();
                    } catch(IllegalArgumentException e) {}

                    final DateTime maxTime = boundary.getMaxTime();

                    return (T) ((TimeBoundaryQuery) query).buildResult(
                        new DateTime(Math.min(res.getTimestamp().getMillis() - offset, now)),
                        minTime != null ? minTime.minus(offset) : null,
                        maxTime != null ? new DateTime(Math.min(maxTime.getMillis() - offset, now)) : null
                    ).iterator().next();
                  }
                  return (T) new Result(res.getTimestamp().minus(offset), value);
                } else if (input instanceof MapBasedRow) {
                  MapBasedRow row = (MapBasedRow) input;
                  return (T) new MapBasedRow(row.getTimestamp().minus(offset), row.getEvent());
                }

                // default to noop for unknown result types
                return input;
              }
            }
        );
      }
    };
  }

  /**
   * Map time t into the last `period` ending within `dataInterval`
   *
   * @param t the current time to be mapped into `dataInterval`
   * @return the offset between the mapped time and time t
   */
  protected long computeOffset(final long t)
  {
    // start is the beginning of the last period ending within dataInterval
    long start = dataInterval.getEndMillis() - periodMillis;
    long startOffset = start % periodMillis - originMillis % periodMillis;
    if(startOffset < 0) {
      startOffset += periodMillis;
    };
    start -= startOffset;

    // tOffset is the offset time t within the last period
    long tOffset = t % periodMillis - originMillis % periodMillis;
    if(tOffset < 0) {
      tOffset += periodMillis;
    }
    tOffset += start;
    return tOffset - t;
  }
}
