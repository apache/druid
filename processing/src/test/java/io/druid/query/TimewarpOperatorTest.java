/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import io.druid.query.timeseries.TimeseriesResultValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TimewarpOperatorTest
{
  TimewarpOperator<Result<TimeseriesResultValue>> testOperator = new TimewarpOperator<>(
      new Interval(new DateTime("2014-01-01"), new DateTime("2014-01-15")),
      new Period("P1W"),
      new DateTime("2014-01-06") // align on Monday
  );

  @Test
  public void testComputeOffset() throws Exception
  {
    {
      final DateTime t = new DateTime("2014-01-23");
      final DateTime tOffset = new DateTime("2014-01-09");

      Assert.assertEquals(
          new DateTime(tOffset),
          t.plus(testOperator.computeOffset(t.getMillis()))
      );
    }

    {
      final DateTime t = new DateTime("2014-08-02");
      final DateTime tOffset = new DateTime("2014-01-11");

      Assert.assertEquals(
          new DateTime(tOffset),
          t.plus(testOperator.computeOffset(t.getMillis()))
      );
    }
  }

  @Test
  public void testPostProcess() throws Exception
  {
    QueryRunner<Result<TimeseriesResultValue>> queryRunner = testOperator.postProcess(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(Query<Result<TimeseriesResultValue>> query)
          {
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        new DateTime(new DateTime("2014-01-09")),
                        new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 2))
                    ),
                    new Result<>(
                        new DateTime(new DateTime("2014-01-11")),
                        new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 3))
                    ),
                    new Result<>(
                        query.getIntervals().get(0).getEnd(),
                        new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 5))
                    )
                )
            );
          }
        },
        new DateTime("2014-08-02").getMillis()
    );

    final Query<Result<TimeseriesResultValue>> query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2014-07-31/2014-08-05")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new Result<>(
                new DateTime("2014-07-31"),
                new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 2))
            ),
            new Result<>(
                new DateTime("2014-08-02"),
                new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 3))
            ),
            new Result<>(
                new DateTime("2014-08-02"),
                new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 5))
            )
        ),
        Sequences.toList(queryRunner.run(query), Lists.<Result<TimeseriesResultValue>>newArrayList())
    );


    TimewarpOperator<Result<TimeBoundaryResultValue>> timeBoundaryOperator = new TimewarpOperator<>(
        new Interval(new DateTime("2014-01-01"), new DateTime("2014-01-15")),
        new Period("P1W"),
        new DateTime("2014-01-06") // align on Monday
    );

    QueryRunner<Result<TimeBoundaryResultValue>> timeBoundaryRunner = timeBoundaryOperator.postProcess(
        new QueryRunner<Result<TimeBoundaryResultValue>>()
        {
          @Override
          public Sequence<Result<TimeBoundaryResultValue>> run(Query<Result<TimeBoundaryResultValue>> query)
          {
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        new DateTime("2014-01-12"),
                        new TimeBoundaryResultValue(ImmutableMap.<String, Object>of("maxTime", new DateTime("2014-01-12")))
                    )
                )
            );
          }
        },
        new DateTime("2014-08-02").getMillis()
    );

    final Query<Result<TimeBoundaryResultValue>> timeBoundaryQuery =
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource("dummy")
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new Result<>(
                new DateTime("2014-08-02"),
                new TimeBoundaryResultValue(ImmutableMap.<String, Object>of("maxTime", new DateTime("2014-08-02")))
            )
        ),
        Sequences.toList(timeBoundaryRunner.run(timeBoundaryQuery), Lists.<Result<TimeBoundaryResultValue>>newArrayList())
    );

  }

  @Test
  public void testEmptyFutureInterval() throws Exception
  {
    QueryRunner<Result<TimeseriesResultValue>> queryRunner = testOperator.postProcess(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(Query<Result<TimeseriesResultValue>> query)
          {
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        query.getIntervals().get(0).getStart(),
                        new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 2))
                    ),
                    new Result<>(
                        query.getIntervals().get(0).getEnd(),
                        new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 3))
                    )
                )
            );
          }
        },
        new DateTime("2014-08-02").getMillis()
    );

    final Query<Result<TimeseriesResultValue>> query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2014-08-06/2014-08-08")
              .aggregators(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("count")))
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new Result<>(
                new DateTime("2014-08-02"),
                new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 2))
            ),
            new Result<>(
                new DateTime("2014-08-02"),
                new TimeseriesResultValue(ImmutableMap.<String, Object>of("metric", 3))
            )
        ),
        Sequences.toList(queryRunner.run(query), Lists.<Result<TimeseriesResultValue>>newArrayList())
    );
  }
}
