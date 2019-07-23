/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.timeboundary.TimeBoundaryResultValue;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;


public class TimewarpOperatorTest
{
  TimewarpOperator<Result<TimeseriesResultValue>> testOperator = new TimewarpOperator<>(
      new Interval(DateTimes.of("2014-01-01"), DateTimes.of("2014-01-15")),
      new Period("P1W"),
      DateTimes.of("2014-01-06") // align on Monday
  );

  @Test
  public void testComputeOffset()
  {
    {
      final DateTime t = DateTimes.of("2014-01-23");
      final DateTime tOffset = DateTimes.of("2014-01-09");

      Assert.assertEquals(tOffset, t.plus(testOperator.computeOffset(t.getMillis(), DateTimeZone.UTC)));
    }

    {
      final DateTime t = DateTimes.of("2014-08-02");
      final DateTime tOffset = DateTimes.of("2014-01-11");

      Assert.assertEquals(tOffset, t.plus(testOperator.computeOffset(t.getMillis(), DateTimeZone.UTC)));
    }

    {
      final DateTime t = DateTimes.of("2014-08-02T-07");
      final DateTime tOffset = DateTimes.of("2014-01-11T-08");

      Assert.assertEquals(
          tOffset,
          t.plus(testOperator.computeOffset(t.getMillis(), DateTimes.inferTzFromString("America/Los_Angeles")))
      );
    }
  }

  @Test
  public void testPostProcess()
  {
    QueryRunner<Result<TimeseriesResultValue>> queryRunner = testOperator.postProcess(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              ResponseContext responseContext
          )
          {
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        DateTimes.of("2014-01-09"),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 2))
                    ),
                    new Result<>(
                        DateTimes.of("2014-01-11"),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 3))
                    ),
                    new Result<>(
                        queryPlus.getQuery().getIntervals().get(0).getEnd(),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 5))
                    )
                )
            );
          }
        },
        DateTimes.of("2014-08-02").getMillis()
    );

    final Query<Result<TimeseriesResultValue>> query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2014-07-31/2014-08-05")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new Result<>(
                DateTimes.of("2014-07-31"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 2))
            ),
            new Result<>(
                DateTimes.of("2014-08-02"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 3))
            ),
            new Result<>(
                DateTimes.of("2014-08-02"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 5))
            )
        ),
        queryRunner.run(QueryPlus.wrap(query)).toList()
    );


    TimewarpOperator<Result<TimeBoundaryResultValue>> timeBoundaryOperator = new TimewarpOperator<>(
        new Interval(DateTimes.of("2014-01-01"), DateTimes.of("2014-01-15")),
        new Period("P1W"),
        DateTimes.of("2014-01-06") // align on Monday
    );

    QueryRunner<Result<TimeBoundaryResultValue>> timeBoundaryRunner = timeBoundaryOperator.postProcess(
        new QueryRunner<Result<TimeBoundaryResultValue>>()
        {
          @Override
          public Sequence<Result<TimeBoundaryResultValue>> run(
              QueryPlus<Result<TimeBoundaryResultValue>> queryPlus,
              ResponseContext responseContext
          )
          {
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        DateTimes.of("2014-01-12"),
                        new TimeBoundaryResultValue(
                            ImmutableMap.<String, Object>of(
                                "maxTime",
                                DateTimes.of("2014-01-12")
                            )
                        )
                    )
                )
            );
          }
        },
        DateTimes.of("2014-08-02").getMillis()
    );

    final Query<Result<TimeBoundaryResultValue>> timeBoundaryQuery =
        Druids.newTimeBoundaryQueryBuilder()
              .dataSource("dummy")
              .build();

    Assert.assertEquals(
        Collections.singletonList(
            new Result<>(
                DateTimes.of("2014-08-02"),
                new TimeBoundaryResultValue(ImmutableMap.<String, Object>of("maxTime", DateTimes.of("2014-08-02")))
            )
        ),
        timeBoundaryRunner.run(QueryPlus.wrap(timeBoundaryQuery)).toList()
    );

  }

  @Test
  public void testPostProcessWithTimezonesAndDstShift()
  {
    QueryRunner<Result<TimeseriesResultValue>> queryRunner = testOperator.postProcess(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              ResponseContext responseContext
          )
          {
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        DateTimes.of("2014-01-09T-08"),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 2))
                    ),
                    new Result<>(
                        DateTimes.of("2014-01-11T-08"),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 3))
                    ),
                    new Result<>(
                        queryPlus.getQuery().getIntervals().get(0).getEnd(),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 5))
                    )
                )
            );
          }
        },
        DateTimes.of("2014-08-02T-07").getMillis()
    );

    final Query<Result<TimeseriesResultValue>> query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2014-07-31T-07/2014-08-05T-07")
              .granularity(new PeriodGranularity(new Period("P1D"), null, DateTimes.inferTzFromString("America/Los_Angeles")))
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new Result<>(
                DateTimes.of("2014-07-31T-07"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 2))
            ),
            new Result<>(
                DateTimes.of("2014-08-02T-07"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 3))
            ),
            new Result<>(
                DateTimes.of("2014-08-02T-07"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 5))
            )
        ),
        queryRunner.run(QueryPlus.wrap(query)).toList()
    );
  }

  @Test
  public void testPostProcessWithTimezonesAndNoDstShift()
  {
    QueryRunner<Result<TimeseriesResultValue>> queryRunner = testOperator.postProcess(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              ResponseContext responseContext
          )
          {
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        DateTimes.of("2014-01-09T-07"),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 2))
                    ),
                    new Result<>(
                        DateTimes.of("2014-01-11T-07"),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 3))
                    ),
                    new Result<>(
                        queryPlus.getQuery().getIntervals().get(0).getEnd(),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 5))
                    )
                )
            );
          }
        },
        DateTimes.of("2014-08-02T-07").getMillis()
    );

    final Query<Result<TimeseriesResultValue>> query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2014-07-31T-07/2014-08-05T-07")
              .granularity(new PeriodGranularity(new Period("P1D"), null, DateTimes.inferTzFromString("America/Phoenix")))
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new Result<>(
                DateTimes.of("2014-07-31T-07"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 2))
            ),
            new Result<>(
                DateTimes.of("2014-08-02T-07"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 3))
            ),
            new Result<>(
                DateTimes.of("2014-08-02T-07"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 5))
            )
        ),
        queryRunner.run(QueryPlus.wrap(query)).toList()
    );
  }

  @Test
  public void testEmptyFutureInterval()
  {
    QueryRunner<Result<TimeseriesResultValue>> queryRunner = testOperator.postProcess(
        new QueryRunner<Result<TimeseriesResultValue>>()
        {
          @Override
          public Sequence<Result<TimeseriesResultValue>> run(
              QueryPlus<Result<TimeseriesResultValue>> queryPlus,
              ResponseContext responseContext
          )
          {
            final Query<Result<TimeseriesResultValue>> query = queryPlus.getQuery();
            return Sequences.simple(
                ImmutableList.of(
                    new Result<>(
                        query.getIntervals().get(0).getStart(),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 2))
                    ),
                    new Result<>(
                        query.getIntervals().get(0).getEnd(),
                        new TimeseriesResultValue(ImmutableMap.of("metric", 3))
                    )
                )
            );
          }
        },
        DateTimes.of("2014-08-02").getMillis()
    );

    final Query<Result<TimeseriesResultValue>> query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2014-08-06/2014-08-08")
              .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
              .build();

    Assert.assertEquals(
        Lists.newArrayList(
            new Result<>(
                DateTimes.of("2014-08-02"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 2))
            ),
            new Result<>(
                DateTimes.of("2014-08-02"),
                new TimeseriesResultValue(ImmutableMap.of("metric", 3))
            )
        ),
        queryRunner.run(QueryPlus.wrap(query)).toList()
    );
  }
}
