/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.query.timeseries;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.Druids;
import com.metamx.druid.PeriodGranularity;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.TestHelper;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.LongSumAggregatorFactory;
import com.metamx.druid.aggregation.MaxAggregatorFactory;
import com.metamx.druid.aggregation.MinAggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryRunnerTestHelper;
import com.metamx.druid.query.filter.AndDimFilter;
import com.metamx.druid.query.filter.DimFilter;
import com.metamx.druid.query.filter.RegexDimFilter;
import com.metamx.druid.query.segment.MultipleIntervalSegmentSpec;
import com.metamx.druid.result.Result;
import com.metamx.druid.result.TimeseriesResultValue;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TimeseriesQueryRunnerTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.makeQueryRunners(new TimeseriesQueryRunnerFactory());
  }

  private final QueryRunner runner;

  public TimeseriesQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testFullOnTimeseries()
  {
    QueryGranularity gran = QueryGranularity.DAY;
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      Arrays.asList(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    DateTime expectedEarliest = new DateTime("2011-01-12");
    DateTime expectedLast = new DateTime("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    int count = 0;
    Result lastResult = null;
    for (Result<TimeseriesResultValue> result : results) {
      lastResult = result;
      Assert.assertEquals(expectedEarliest, result.getTimestamp());
      Assert.assertFalse(
          String.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast),
          result.getTimestamp().isAfter(expectedLast)
      );

      final TimeseriesResultValue value = result.getValue();

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0L : 13L,
          value.getLongMetric("rows").longValue()
      );
      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.expectedFullOnIndexValues[count],
          String.valueOf(value.getDoubleMetric("index"))
      );
      Assert.assertEquals(
          result.toString(),
          new Double(QueryRunnerTestHelper.expectedFullOnIndexValues[count]) +
          (QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0L : 13L) + 1L,
          value.getDoubleMetric("addRowsIndexConstant"),
          0.0
      );

      expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
      ++count;
    }

    Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
  }

  @Test
  public void testFullOnTimeseriesMaxMin()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryGranularity.ALL)
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(
                                      Arrays.asList(
                                          new MaxAggregatorFactory("maxIndex", "index"),
                                          new MinAggregatorFactory("minIndex", "index")
                                      )
                                  )
                                  .build();

    DateTime expectedEarliest = new DateTime("2011-01-12");
    DateTime expectedLast = new DateTime("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Result<TimeseriesResultValue> result = results.iterator().next();

    Assert.assertEquals(expectedEarliest, result.getTimestamp());
    Assert.assertFalse(
        String.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast),
        result.getTimestamp().isAfter(expectedLast)
    );

    final TimeseriesResultValue value = result.getValue();

    Assert.assertEquals(result.toString(), 1870.06103515625, value.getDoubleMetric("maxIndex"), 0.0);
    Assert.assertEquals(result.toString(), 59.02102279663086, value.getDoubleMetric("minIndex"), 0.0);
  }

  @Test
  public void testFullOnTimeseriesWithFilter()
  {

    QueryGranularity gran = QueryGranularity.DAY;
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.providerDimension, "upfront")
                                  .intervals(QueryRunnerTestHelper.fullOnInterval)
                                  .aggregators(Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.rowsCount))
                                  .build();

    Assert.assertEquals(
        Druids.newSelectorDimFilterBuilder()
              .dimension(QueryRunnerTestHelper.providerDimension)
              .value("upfront")
              .build(),
        query.getDimensionsFilter()
    );

    DateTime expectedEarliest = new DateTime("2011-01-12");
    DateTime expectedLast = new DateTime("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    for (Result<TimeseriesResultValue> result : results) {
      Assert.assertEquals(result.toString(), expectedEarliest, result.getTimestamp());
      Assert.assertFalse(
          String.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast),
          result.getTimestamp().isAfter(expectedLast)
      );

      final TimeseriesResultValue value = result.getValue();

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0L : 2L,
          value.getLongMetric("rows").longValue()
      );

      expectedEarliest = gran.toDateTime(gran.next(expectedEarliest.getMillis()));
    }
  }

  @Test
  public void testTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 6619L)
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithTimeZone()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .intervals("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .granularity(new PeriodGranularity(new Period("P1D"), null, DateTimeZone.forID("America/Los_Angeles")))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-03-31", DateTimeZone.forID("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 6619L)
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01T", DateTimeZone.forID("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );

    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithVaryingGran()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"
                                           )
                                       )
                                   )
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = Sequences.toList(
        runner.run(query1),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .granularity("DAY")
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"
                                           )
                                       )
                                   )
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults2 = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results2 = Sequences.toList(
        runner.run(query2),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesWithVaryingGranWithFilter()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .filters(QueryRunnerTestHelper.providerDimension, "spot", "upfront", "total_market")
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"
                                           )
                                       )
                                   )
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = Sequences.toList(
        runner.run(query1),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .filters(QueryRunnerTestHelper.providerDimension, "spot", "upfront", "total_market")
                                   .granularity("DAY")
                                   .intervals(
                                       Arrays.asList(
                                           new Interval(
                                               "2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z"
                                           )
                                       )
                                   )
                                   .aggregators(
                                       Arrays.<AggregatorFactory>asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults2 = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results2 = Sequences.toList(
        runner.run(query2),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesQueryBeyondTimeRangeOfData()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(
                                      new MultipleIntervalSegmentSpec(
                                          Arrays.asList(
                                              new Interval(
                                                  "2015-01-01/2015-01-10"
                                              )
                                          )
                                      )
                                  )
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList();

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithOrFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.providerDimension, "spot", "upfront", "total_market")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 6619L,
                    "addRowsIndexConstant", 6633.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 5827L,
                    "addRowsIndexConstant", 5841.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithRegexFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
        .filters(new RegexDimFilter(QueryRunnerTestHelper.providerDimension, "^.p.*$")) // spot and upfront
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexLongSum))
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 11L,
                    "index", 3783L,
                    "addRowsIndexConstant", 3795.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 11L,
                    "index", 3313L,
                    "addRowsIndexConstant", 3325.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.providerDimension, "spot")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 9L,
                    "index", 1102L,
                    "addRowsIndexConstant", 1112.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 9L,
                    "index", 1120L,
                    "addRowsIndexConstant", 1130.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter2()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.providerDimension, "upfront")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 2681L,
                    "addRowsIndexConstant", 2684.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 2193L,
                    "addRowsIndexConstant", 2196.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter3()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.providerDimension, "total_market")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 2836L,
                    "addRowsIndexConstant", 2839.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 2514L,
                    "addRowsIndexConstant", 2517.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilterAndOr()
  {
    AndDimFilter andDimFilter = Druids
        .newAndDimFilterBuilder()
        .fields(
            Arrays.<DimFilter>asList(
                Druids.newSelectorDimFilterBuilder()
                      .dimension(QueryRunnerTestHelper.providerDimension)
                      .value("spot")
                      .build(),
                Druids.newOrDimFilterBuilder()
                      .fields(QueryRunnerTestHelper.qualityDimension, "automotive", "business")
                      .build()
            )
        )
        .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 254.4554443359375D,
                    "addRowsIndexConstant", 257.4554443359375D
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 260.4129638671875D,
                    "addRowsIndexConstant", 263.4129638671875D
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilter()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.providerDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.qualityDimension)
                                                    .value("automotive")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(135.885094).doubleValue(),
                    "addRowsIndexConstant", new Float(137.885094).doubleValue()
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(147.425935).doubleValue(),
                    "addRowsIndexConstant", new Float(149.425935).doubleValue()
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithOtherMultiDimFilter()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.providerDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.qualityDimension)
                                                    .value("business")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(118.570340).doubleValue(),
                    "addRowsIndexConstant", new Float(120.570340).doubleValue()
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 1L,
                    "index", new Float(112.987027).doubleValue(),
                    "addRowsIndexConstant", new Float(114.987027).doubleValue()
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterInOr()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(
                                      QueryRunnerTestHelper.providerDimension,
                                      "spot",
                                      "upfront",
                                      "total_market",
                                      "billyblank"
                                  )
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.<AggregatorFactory>asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 6619L,
                    "addRowsIndexConstant", 6633.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 13L,
                    "index", 5827L,
                    "addRowsIndexConstant", 5841.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDimAndOr()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.providerDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newOrDimFilterBuilder()
                                                    .fields(
                                                        QueryRunnerTestHelper.qualityDimension,
                                                        "automotive",
                                                        "business",
                                                        "billyblank"
                                                    )
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 254.4554443359375D,
                    "addRowsIndexConstant", 257.4554443359375D
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 2L,
                    "index", 260.4129638671875D,
                    "addRowsIndexConstant", 263.4129638671875D
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.providerDimension, "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDim()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.providerDimension)
                                                    .value("billy")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.qualityDimension)
                                                    .value("business")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0
                )
            )
        ),
        new Result<TimeseriesResultValue>(
            new DateTime("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>of(
                    "rows", 0L,
                    "index", 0.0,
                    "addRowsIndexConstant", 1.0
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.placementishDimension, "preferred")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = Sequences.toList(
        runner.run(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(QueryRunnerTestHelper.dataSource)
                  .granularity(QueryRunnerTestHelper.dayGran)
                  .intervals(QueryRunnerTestHelper.firstToThird)
                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                  .build()
        ),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter2()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.placementishDimension, "a")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = Sequences.toList(
        runner.run(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(QueryRunnerTestHelper.dataSource)
                  .granularity(QueryRunnerTestHelper.dayGran)
                  .filters(QueryRunnerTestHelper.qualityDimension, "automotive")
                  .intervals(QueryRunnerTestHelper.firstToThird)
                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                  .build()
        ),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr1()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.providerDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.placementishDimension)
                                                    .value("a")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    AndDimFilter andDimFilter2 = Druids.newAndDimFilterBuilder()
                                       .fields(
                                           Arrays.<DimFilter>asList(
                                               Druids.newSelectorDimFilterBuilder()
                                                     .dimension(QueryRunnerTestHelper.providerDimension)
                                                     .value("spot")
                                                     .build(),
                                               Druids.newSelectorDimFilterBuilder()
                                                     .dimension(QueryRunnerTestHelper.qualityDimension)
                                                     .value("automotive")
                                                     .build()
                                           )
                                       )
                                       .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = Sequences.toList(
        runner.run(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(QueryRunnerTestHelper.dataSource)
                  .granularity(QueryRunnerTestHelper.dayGran)
                  .filters(andDimFilter2)
                  .intervals(QueryRunnerTestHelper.firstToThird)
                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                  .build()
        ),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr2()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Arrays.<DimFilter>asList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.providerDimension)
                                                    .value("spot")
                                                    .build(),
                                              Druids.newOrDimFilterBuilder()
                                                    .fields(QueryRunnerTestHelper.placementishDimension, "a", "b")
                                                    .build()
                                          )
                                      )
                                      .build();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                  .build();

    AndDimFilter andDimFilter2 = Druids.newAndDimFilterBuilder()
                                       .fields(
                                           Arrays.<DimFilter>asList(
                                               Druids.newSelectorDimFilterBuilder()
                                                     .dimension(QueryRunnerTestHelper.providerDimension)
                                                     .value("spot")
                                                     .build(),
                                               Druids.newOrDimFilterBuilder()
                                                     .fields(QueryRunnerTestHelper.qualityDimension, "automotive", "business")
                                                     .build()
                                           )
                                       )
                                       .build();


    Iterable<Result<TimeseriesResultValue>> expectedResults = Sequences.toList(
        runner.run(
            Druids.newTimeseriesQueryBuilder()
                  .dataSource(QueryRunnerTestHelper.dataSource)
                  .granularity(QueryRunnerTestHelper.dayGran)
                  .filters(andDimFilter2)
                  .intervals(QueryRunnerTestHelper.firstToThird)
                  .aggregators(QueryRunnerTestHelper.commonAggregators)
                  .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                  .build()
        ),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }
}
