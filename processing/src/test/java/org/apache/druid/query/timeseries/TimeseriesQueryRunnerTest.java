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

package org.apache.druid.query.timeseries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TimeseriesQueryRunnerTest
{

  public static final Map<String, Object> CONTEXT = ImmutableMap.of();

  @Parameterized.Parameters(name = "{0}:descending={1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(
        // runners
        QueryRunnerTestHelper.makeQueryRunners(
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
                new TimeseriesQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        ),
        // descending?
        Arrays.asList(false, true),
        Arrays.asList(QueryRunnerTestHelper.commonDoubleAggregators, QueryRunnerTestHelper.commonFloatAggregators)
    );
  }

  private <T> void assertExpectedResults(Iterable<Result<T>> expectedResults, Iterable<Result<T>> results)
  {
    if (descending) {
      expectedResults = TestHelper.revert(expectedResults);
    }
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  protected final QueryRunner runner;
  protected final boolean descending;
  private final List<AggregatorFactory> aggregatorFactoryList;

  public TimeseriesQueryRunnerTest(
      QueryRunner runner,
      boolean descending,
      List<AggregatorFactory> aggregatorFactoryList
  )
  {
    this.runner = runner;
    this.descending = descending;
    this.aggregatorFactoryList = aggregatorFactoryList;
  }

  @Test
  public void testEmptyTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .intervals(QueryRunnerTestHelper.emptyInterval)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexDoubleSum,
                                          new DoubleFirstAggregatorFactory("first", "index")

                                      )
                                  )
                                  .descending(descending)
                                  .build();
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put("rows", 0L);
    resultMap.put("index", NullHandling.defaultDoubleValue());
    resultMap.put("first", NullHandling.defaultDoubleValue());
    List<Result<TimeseriesResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            DateTimes.of("2020-04-02"),
            new TimeseriesResultValue(
                resultMap
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testFullOnTimeseries()
  {
    Granularity gran = Granularities.DAY;
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(gran)
                                  .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexDoubleSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    final String[] expectedIndex = descending ?
                                   QueryRunnerTestHelper.expectedFullOnIndexValuesDesc :
                                   QueryRunnerTestHelper.expectedFullOnIndexValues;

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.earliest :
                                  QueryRunnerTestHelper.last;

    int count = 0;
    Result lastResult = null;
    for (Result<TimeseriesResultValue> result : results) {
      DateTime current = result.getTimestamp();
      Assert.assertFalse(
          StringUtils.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current.isBefore(expectedLast) : current.isAfter(expectedLast)
      );

      final TimeseriesResultValue value = result.getValue();

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(current) ? 0L : 13L,
          value.getLongMetric("rows").longValue()
      );

      if (!QueryRunnerTestHelper.skippedDay.equals(current)) {
        Assert.assertEquals(
            result.toString(),
            Doubles.tryParse(expectedIndex[count]).doubleValue(),
            value.getDoubleMetric("index").doubleValue(),
            value.getDoubleMetric("index").doubleValue() * 1e-6
        );
        Assert.assertEquals(
            result.toString(),
            new Double(expectedIndex[count]) +
            13L + 1L,
            value.getDoubleMetric("addRowsIndexConstant"),
            value.getDoubleMetric("addRowsIndexConstant") * 1e-6
        );
        Assert.assertEquals(
            value.getDoubleMetric("uniques"),
            9.0d,
            0.02
        );
      } else {
        if (NullHandling.replaceWithDefault()) {
          Assert.assertEquals(
              result.toString(),
              0.0D,
              value.getDoubleMetric("index").doubleValue(),
              value.getDoubleMetric("index").doubleValue() * 1e-6
          );
          Assert.assertEquals(
              result.toString(),
              new Double(expectedIndex[count]) + 1L,
              value.getDoubleMetric("addRowsIndexConstant"),
              value.getDoubleMetric("addRowsIndexConstant") * 1e-6
          );
          Assert.assertEquals(
              0.0D,
              value.getDoubleMetric("uniques"),
              0.02
          );
        } else {
          Assert.assertNull(
              result.toString(),
              value.getDoubleMetric("index")
          );
          Assert.assertNull(
              result.toString(),
              value.getDoubleMetric("addRowsIndexConstant")
          );
          Assert.assertEquals(
              value.getDoubleMetric("uniques"),
              0.0d,
              0.02
          );
        }
      }

      lastResult = result;
      ++count;
    }

    Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
  }

  @Test
  public void testTimeseriesNoAggregators()
  {
    Granularity gran = Granularities.DAY;
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(gran)
                                  .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                  .descending(descending)
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.earliest :
                                  QueryRunnerTestHelper.last;

    Result lastResult = null;
    for (Result<TimeseriesResultValue> result : results) {
      DateTime current = result.getTimestamp();
      Assert.assertFalse(
          StringUtils.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current.isBefore(expectedLast) : current.isAfter(expectedLast)
      );
      Assert.assertEquals(ImmutableMap.of(), result.getValue().getBaseObject());
      lastResult = result;
    }

    Assert.assertEquals(lastResult.toString(), expectedLast, lastResult.getTimestamp());
  }

  @Test
  public void testFullOnTimeseriesMaxMin()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                  .aggregators(
                                      Arrays.asList(
                                          new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                          new DoubleMinAggregatorFactory("minIndex", "index")
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    DateTime expectedEarliest = DateTimes.of("2011-01-12");
    DateTime expectedLast = DateTimes.of("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    Result<TimeseriesResultValue> result = results.iterator().next();

    Assert.assertEquals(expectedEarliest, result.getTimestamp());
    Assert.assertFalse(
        StringUtils.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast),
        result.getTimestamp().isAfter(expectedLast)
    );

    final TimeseriesResultValue value = result.getValue();

    Assert.assertEquals(result.toString(), 1870.061029, value.getDoubleMetric("maxIndex"), 1870.061029 * 1e-6);
    Assert.assertEquals(result.toString(), 59.021022, value.getDoubleMetric("minIndex"), 59.021022 * 1e-6);
  }

  @Test
  public void testFullOnTimeseriesWithFilter()
  {

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.marketDimension, "upfront")
                                  .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    Assert.assertEquals(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "upfront", null),
        query.getDimensionsFilter()
    );

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.earliest :
                                  QueryRunnerTestHelper.last;

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    for (Result<TimeseriesResultValue> result : results) {
      DateTime current = result.getTimestamp();
      Assert.assertFalse(
          StringUtils.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current.isBefore(expectedLast) : current.isAfter(expectedLast)
      );

      final TimeseriesResultValue value = result.getValue();

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0L : 2L,
          value.getLongMetric("rows").longValue()
      );
      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.skippedDay.equals(result.getTimestamp()) ? 0.0d : 2.0d,
          value.getDoubleMetric(
              "uniques"
          ),
          0.01
      );
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
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          ),
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 6619L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesGrandTotal()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, true))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = new ArrayList<>();

    expectedResults.add(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows",
                    13L,
                    "index",
                    6619L,
                    "uniques",
                    QueryRunnerTestHelper.UNIQUES_9,
                    QueryRunnerTestHelper.addRowsIndexConstantMetric,
                    6633.0
                )
            )
        )
    );

    expectedResults.add(
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows",
                    13L,
                    "index",
                    5827L,
                    "uniques",
                    QueryRunnerTestHelper.UNIQUES_9,
                    QueryRunnerTestHelper.addRowsIndexConstantMetric,
                    5841.0
                )
            )
        )
    );

    if (descending) {
      Collections.reverse(expectedResults);
    }

    expectedResults.add(
        new Result<>(
            null,
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows",
                    26L,
                    "index",
                    12446L,
                    "uniques",
                    QueryRunnerTestHelper.UNIQUES_9,
                    QueryRunnerTestHelper.addRowsIndexConstantMetric,
                    12473.0
                )
            )
        )
    );

    // Must create a toolChest so we can run mergeResults (which applies grand totals).
    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );

    // Must wrapped in a results finalizer to stop the runner's builtin finalizer from being called.
    final FinalizeResultsQueryRunner finalRunner = new FinalizeResultsQueryRunner(
        toolChest.mergeResults(runner),
        toolChest
    );

    final List results = finalRunner.run(QueryPlus.wrap(query), CONTEXT).toList();

    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesIntervalOutOfRanges()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .intervals(QueryRunnerTestHelper.emptyInterval)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.indexLongSum
                                      )
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .context(ImmutableMap.of(TimeseriesQuery.SKIP_EMPTY_BUCKETS, false))
                                  .build();
    List<Result<TimeseriesResultValue>> expectedResults = new ArrayList<>();

    expectedResults.add(
        new Result<>(
            QueryRunnerTestHelper.emptyInterval.getIntervals().get(0).getStart(),
            new TimeseriesResultValue(
                TestHelper.createExpectedMap(
                    "rows",
                    0L,
                    "index",
                    NullHandling.defaultLongValue(),
                    QueryRunnerTestHelper.addRowsIndexConstantMetric,
                    NullHandling.sqlCompatible() ? null : 1.0
                )
            )
        )
    );

    // Must create a toolChest so we can run mergeResults (which creates the zeroed-out row).
    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );

    // Must wrapped in a results finalizer to stop the runner's builtin finalizer from being called.
    final FinalizeResultsQueryRunner finalRunner = new FinalizeResultsQueryRunner(
        toolChest.mergeResults(runner),
        toolChest
    );

    final List results = finalRunner.run(QueryPlus.wrap(query), CONTEXT).toList();
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithVirtualColumn()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory("idx", "expr"),
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .descending(descending)
                                  .virtualColumns(
                                      new ExpressionVirtualColumn(
                                          "expr",
                                          "index",
                                          ValueType.FLOAT,
                                          TestExprMacroTable.INSTANCE
                                      )
                                  )
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 6619L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithTimeZone()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .intervals("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .granularity(
                                      new PeriodGranularity(
                                          new Period("P1D"),
                                          null,
                                          DateTimes.inferTzFromString("America/Los_Angeles")
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-03-31", DateTimes.inferTzFromString("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 6619L)
            )
        ),
        new Result<>(
            new DateTime("2011-04-01T", DateTimes.inferTzFromString("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithVaryingGran()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.qualityUniques
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1), CONTEXT).toList();
    assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .granularity("DAY")
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.qualityUniques
                                       )
                                   )
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults2 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results2 = runner.run(QueryPlus.wrap(query2), CONTEXT).toList();
    assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesGranularityNotAlignedOnSegmentBoundariesWithFilter()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity(
                                       new PeriodGranularity(
                                           new Period("P7D"),
                                           null,
                                           DateTimes.inferTzFromString("America/Los_Angeles")
                                       )
                                   )
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-01-12T00:00:00.000-08:00/2011-01-20T00:00:00.000-08:00")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-06T00:00:00.000-08:00", DateTimes.inferTzFromString("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 6071L)
            )
        ),
        new Result<>(
            new DateTime("2011-01-13T00:00:00.000-08:00", DateTimes.inferTzFromString("America/Los_Angeles")),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 91L, "idx", 33382L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1), CONTEXT).toList();
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesQueryZeroFilling()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity(Granularities.HOUR)
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-14T00:00:00.000Z/2011-05-01T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Result<TimeseriesResultValue>> lotsOfZeroes = new ArrayList<>();
    final Iterable<Interval> iterable = Granularities.HOUR.getIterable(
        new Interval(DateTimes.of("2011-04-14T01"), DateTimes.of("2011-04-15"))
    );
    Map noRowsResult = new HashMap<>();
    noRowsResult.put("rows", 0L);
    noRowsResult.put("idx", NullHandling.defaultLongValue());
    for (Interval interval : iterable) {
      lotsOfZeroes.add(
          new Result<>(
              interval.getStart(),
              new TimeseriesResultValue(noRowsResult)
          )
      );
    }

    List<Result<TimeseriesResultValue>> expectedResults1 = Lists.newArrayList(
        Iterables.concat(
            Collections.singletonList(
                new Result<>(
                    DateTimes.of("2011-04-14T00"),
                    new TimeseriesResultValue(
                        ImmutableMap.of("rows", 13L, "idx", 4907L)
                    )
                )
            ),
            lotsOfZeroes,
            Collections.singletonList(
                new Result<>(
                    DateTimes.of("2011-04-15T00"),
                    new TimeseriesResultValue(
                        ImmutableMap.of("rows", 13L, "idx", 4717L)
                    )
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1), CONTEXT).toList();
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesQueryGranularityNotAlignedWithRollupGranularity()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity(
                                       new PeriodGranularity(
                                           new Period("PT1H"),
                                           DateTimes.utc(60000),
                                           DateTimeZone.UTC
                                       )
                                   )
                                   .intervals(Collections.singletonList(Intervals.of("2011-04-15T00:00:00.000Z/2012")))
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-14T23:01Z"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 4717L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1), CONTEXT).toList();
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesWithVaryingGranWithFilter()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.qualityUniques
                                       )
                                   )
                                   .descending(descending)
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );
    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1), CONTEXT).toList();
    assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.dataSource)
                                   .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                   .granularity("DAY")
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.rowsCount,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.qualityUniques
                                       )
                                   )
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults2 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results2 = runner.run(QueryPlus.wrap(query2), CONTEXT).toList();
    assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesQueryBeyondTimeRangeOfData()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(
                                      new MultipleIntervalSegmentSpec(
                                          Collections.singletonList(Intervals.of("2015-01-01/2015-01-10"))
                                      )
                                  )
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.emptyList();

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithOrFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.marketDimension, "spot", "upfront", "total_market")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexLongSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 6619L,
                    "addRowsIndexConstant", 6633.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 5827L,
                    "addRowsIndexConstant", 5841.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithRegexFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
        .filters(new RegexDimFilter(QueryRunnerTestHelper.marketDimension, "^.p.*$", null)) // spot and upfront
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            QueryRunnerTestHelper.rowsCount,
            QueryRunnerTestHelper.indexLongSum,
            QueryRunnerTestHelper.qualityUniques
        )
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 11L,
                    "index", 3783L,
                    "addRowsIndexConstant", 3795.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 11L,
                    "index", 3313L,
                    "addRowsIndexConstant", 3325.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.marketDimension, "spot")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexLongSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 9L,
                    "index", 1102L,
                    "addRowsIndexConstant", 1112.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 9L,
                    "index", 1120L,
                    "addRowsIndexConstant", 1130.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter2()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.marketDimension, "upfront")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexLongSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 2L,
                    "index", 2681L,
                    "addRowsIndexConstant", 2684.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 2L,
                    "index", 2193L,
                    "addRowsIndexConstant", 2196.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter3()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.marketDimension, "total_market")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexLongSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 2L,
                    "index", 2836L,
                    "addRowsIndexConstant", 2839.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 2L,
                    "index", 2514L,
                    "addRowsIndexConstant", 2517.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilterAndOr()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new OrDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", "business")
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 2L,
                    "index", 254.4554443359375D,
                    "addRowsIndexConstant", 257.4554443359375D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 2L,
                    "index", 260.4129638671875D,
                    "addRowsIndexConstant", 263.4129638671875D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilter()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", null)
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 1L,
                    "index", new Float(135.885094).doubleValue(),
                    "addRowsIndexConstant", new Float(137.885094).doubleValue(),
                    "uniques", QueryRunnerTestHelper.UNIQUES_1
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 1L,
                    "index", new Float(147.425935).doubleValue(),
                    "addRowsIndexConstant", new Float(149.425935).doubleValue(),
                    "uniques", QueryRunnerTestHelper.UNIQUES_1
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithOtherMultiDimFilter()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "business", null)
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(QueryRunnerTestHelper.commonDoubleAggregators)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 1L,
                    "index", new Float(118.570340).doubleValue(),
                    "addRowsIndexConstant", new Float(120.570340).doubleValue(),
                    "uniques", QueryRunnerTestHelper.UNIQUES_1
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 1L,
                    "index", new Float(112.987027).doubleValue(),
                    "addRowsIndexConstant", new Float(114.987027).doubleValue(),
                    "uniques", QueryRunnerTestHelper.UNIQUES_1
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterInOr()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(
                                      QueryRunnerTestHelper.marketDimension,
                                      "spot",
                                      "upfront",
                                      "total_market",
                                      "billyblank"
                                  )
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexLongSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 6619L,
                    "addRowsIndexConstant", 6633.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 5827L,
                    "addRowsIndexConstant", 5841.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }


  @Test
  public void testTimeseriesWithInFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(
                                      new InDimFilter(
                                          QueryRunnerTestHelper.marketDimension,
                                          Arrays.asList(
                                              "spot",
                                              "upfront",
                                              "total_market",
                                              "billyblank"
                                          ),
                                          null
                                      )
                                  )
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexLongSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 6619L,
                    "addRowsIndexConstant", 6633.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 5827L,
                    "addRowsIndexConstant", 5841.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDimAndOr()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new OrDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", "business", "billyblank")
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 2L,
                    "index", 254.4554443359375D,
                    "addRowsIndexConstant", 257.4554443359375D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 2L,
                    "index", 260.4129638671875D,
                    "addRowsIndexConstant", 263.4129638671875D,
                    "uniques", QueryRunnerTestHelper.UNIQUES_2
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put("rows", 0L);
    resultMap.put("index", NullHandling.defaultDoubleValue());
    resultMap.put("addRowsIndexConstant", NullHandling.replaceWithDefault() ? 1.0 : null);
    resultMap.put("uniques", 0.0);

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                resultMap
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                resultMap
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimensionSkipBuckets()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .context(ImmutableMap.of("skipEmptyBuckets", "true"))
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.emptyList();

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), new HashMap<String, Object>()).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNullFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters("bobby", null)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 6626.151596069336,
                    "addRowsIndexConstant", 6640.151596069336,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 5833.2095947265625,
                    "addRowsIndexConstant", 5847.2095947265625,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), new HashMap<String, Object>()).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithInvertedFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(new NotDimFilter(new SelectorDimFilter("bobby", "sally", null)))
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 6626.151596069336,
                    "addRowsIndexConstant", 6640.151596069336,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 13L,
                    "index", 5833.2095947265625,
                    "addRowsIndexConstant", 5847.2095947265625,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), new HashMap<String, Object>()).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(QueryRunnerTestHelper.marketDimension, "billy")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put("rows", 0L);
    resultMap.put("index", NullHandling.defaultDoubleValue());
    resultMap.put("addRowsIndexConstant", NullHandling.replaceWithDefault() ? 1.0 : null);
    resultMap.put("uniques", 0.0);

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                resultMap
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                resultMap
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDim()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "billy", null),
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "business", null)
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .descending(descending)
                                  .build();
    Map<String, Object> resultMap = new HashMap<>();
    resultMap.put("rows", 0L);
    resultMap.put("index", NullHandling.defaultDoubleValue());
    resultMap.put("addRowsIndexConstant", NullHandling.replaceWithDefault() ? 1.0 : null);
    resultMap.put("uniques", 0.0);

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                resultMap
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                resultMap
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiValueFilteringJavascriptAggregator()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      ImmutableList.of(
                                          QueryRunnerTestHelper.indexDoubleSum,
                                          QueryRunnerTestHelper.jsIndexSumIfPlacementishA,
                                          QueryRunnerTestHelper.jsPlacementishCount
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            QueryRunnerTestHelper.firstToThird.getIntervals().get(0).getStart(),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "index", 12459.361190795898d,
                    "nindex", 283.31103515625d,
                    "pishcount", 52d
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueFilteringJavascriptAggregatorAndAlsoRegularFilters()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .filters(QueryRunnerTestHelper.placementishDimension, "a")
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      ImmutableList.of(
                                          QueryRunnerTestHelper.indexDoubleSum,
                                          QueryRunnerTestHelper.jsIndexSumIfPlacementishA,
                                          QueryRunnerTestHelper.jsPlacementishCount
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            QueryRunnerTestHelper.firstToThird.getIntervals().get(0).getStart(),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "index", 283.31103515625d,
                    "nindex", 283.31103515625d,
                    "pishcount", 4d
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithFirstLastAggregator()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.monthGran)
                                  .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                  .aggregators(
                                      ImmutableList.of(
                                          new DoubleFirstAggregatorFactory("first", "index"),
                                          new DoubleLastAggregatorFactory("last", "index")
                                      )
                                  )
                                  .descending(descending)
                                  .build();

    // There's a difference between ascending and descending results since granularity of druid.sample.tsv is days,
    // with multiple first and last times. The traversal order difference cause the first and last aggregator
    // to select different value from the list of first and last dates
    List<Result<TimeseriesResultValue>> expectedAscendingResults = ImmutableList.of(
        new Result<>(
            DateTimes.of("2011-01-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "first", new Float(100.000000).doubleValue(),
                    "last", new Float(943.497198).doubleValue()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-02-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "first", new Float(132.123776).doubleValue(),
                    "last", new Float(1101.918270).doubleValue()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-03-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "first", new Float(153.059937).doubleValue(),
                    "last", new Float(1063.201156).doubleValue()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "first", new Float(135.885094).doubleValue(),
                    "last", new Float(780.271977).doubleValue()
                )
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedDescendingResults = ImmutableList.of(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "first", new Float(1234.247546).doubleValue(),
                    "last", new Float(106.793700).doubleValue()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-03-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "first", new Float(1004.940887).doubleValue(),
                    "last", new Float(151.752485).doubleValue()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-02-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "first", new Float(913.561076).doubleValue(),
                    "last", new Float(122.258195).doubleValue()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-01-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "first", new Float(800.000000).doubleValue(),
                    "last", new Float(133.740047).doubleValue()
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    if (descending) {
      TestHelper.assertExpectedResults(expectedDescendingResults, actualResults);
    } else {
      TestHelper.assertExpectedResults(expectedAscendingResults, actualResults);
    }
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter1()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "preferred")
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    TimeseriesQuery query1 = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();
    Iterable<Result<TimeseriesResultValue>> expectedResults = runner.run(QueryPlus.wrap(query1), CONTEXT).toList();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter2()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "a")
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    TimeseriesQuery query1 = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .filters(QueryRunnerTestHelper.qualityDimension, "automotive")
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();
    Iterable<Result<TimeseriesResultValue>> expectedResults = runner.run(QueryPlus.wrap(query1), CONTEXT).toList();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr1()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.placementishDimension, "a", null)
    );
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .filters(andDimFilter)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    AndDimFilter andDimFilter2 = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", null)
    );

    TimeseriesQuery query2 = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .filters(andDimFilter2)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();
    Iterable<Result<TimeseriesResultValue>> expectedResults = runner.run(QueryPlus.wrap(query2), CONTEXT).toList();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr2()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new OrDimFilter(QueryRunnerTestHelper.placementishDimension, "a", "b")
    );
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .filters(andDimFilter)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    AndDimFilter andDimFilter2 = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null),
        new OrDimFilter(QueryRunnerTestHelper.qualityDimension, "automotive", "business")
    );

    TimeseriesQuery query2 = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.dayGran)
        .filters(andDimFilter2)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();
    Iterable<Result<TimeseriesResultValue>> expectedResults = runner.run(QueryPlus.wrap(query2), CONTEXT).toList();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAgg()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    Collections.singletonList(new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "spot", null)
                        ))
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "filteredAgg", 18L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggDimensionNotPresentNotNullValue()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    Collections.singletonList(
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new SelectorDimFilter("abraKaDabra", "Lol", null)
                        )
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "filteredAgg", 0L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggDimensionNotPresentNullValue()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    Collections.singletonList(
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new SelectorDimFilter("abraKaDabra", null, null)
                        )
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "filteredAgg", 26L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggValueNotPresent()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    Collections.singletonList(
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new NotDimFilter(
                                new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "LolLol", null)
                            )
                        )
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "filteredAgg", 26L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggInvertedNullValue()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    Collections.singletonList(
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new NotDimFilter(new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, null, null))
                        )
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
        .descending(descending)
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "filteredAgg", 26L,
                    "addRowsIndexConstant", 12486.361190795898d,
                    "index", 12459.361190795898d,
                    "uniques", 9.019833517963864d,
                    "rows", 26L
                )
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithTimeColumn()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
                                      QueryRunnerTestHelper.__timeLongSum
                                  )
                                  .granularity(QueryRunnerTestHelper.allGran)
                                  .descending(descending)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows",
                    26L,
                    "ntimestamps",
                    13.0,
                    "sumtime",
                    33843139200000L
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), CONTEXT).toList();

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithBoundFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(
                                      new AndDimFilter(
                                          Arrays.asList(
                                              new BoundDimFilter(
                                                  QueryRunnerTestHelper.marketDimension,
                                                  "spa",
                                                  "spot",
                                                  true,
                                                  null,
                                                  null,
                                                  null,
                                                  StringComparators.LEXICOGRAPHIC
                                              ),
                                              new BoundDimFilter(
                                                  QueryRunnerTestHelper.marketDimension,
                                                  "spot",
                                                  "spotify",
                                                  null,
                                                  true,
                                                  null,
                                                  null,
                                                  StringComparators.LEXICOGRAPHIC
                                              ),
                                              new BoundDimFilter(
                                                  QueryRunnerTestHelper.marketDimension,
                                                  "SPOT",
                                                  "spot",
                                                  null,
                                                  null,
                                                  null,
                                                  null,
                                                  StringComparators.LEXICOGRAPHIC
                                              )
                                          )
                                      )
                                  )
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexLongSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 9L,
                    "index", 1102L,
                    "addRowsIndexConstant", 1112.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 9L,
                    "index", 1120L,
                    "addRowsIndexConstant", 1130.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeSeriesWithSelectionFilterLookupExtractionFn()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("spot", "upfront");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, true, null, true, true);
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .filters(
                                      new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "upfront", lookupExtractionFn)
                                  )
                                  .intervals(QueryRunnerTestHelper.firstToThird)
                                  .aggregators(
                                      QueryRunnerTestHelper.rowsCount,
                                      QueryRunnerTestHelper.indexLongSum,
                                      QueryRunnerTestHelper.qualityUniques
                                  )
                                  .postAggregators(QueryRunnerTestHelper.addRowsIndexConstant)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 11L,
                    "index", 3783L,
                    "addRowsIndexConstant", 3795.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 11L,
                    "index", 3313L,
                    "addRowsIndexConstant", 3325.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query), CONTEXT).toList();
    TestHelper.assertExpectedResults(expectedResults, results);

    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );
    QueryRunner<Result<TimeseriesResultValue>> optimizedRunner = toolChest.postMergeQueryDecoration(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)));
    Iterable<Result<TimeseriesResultValue>> results2 = new FinalizeResultsQueryRunner(optimizedRunner, toolChest)
        .run(QueryPlus.wrap(query), CONTEXT)
        .toList();
    TestHelper.assertExpectedResults(expectedResults, results2);

  }

  @Test
  public void testTimeseriesWithLimit()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.dataSource)
                                  .granularity(QueryRunnerTestHelper.dayGran)
                                  .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.rowsCount,
                                          QueryRunnerTestHelper.qualityUniques
                                      )
                                  )
                                  .descending(descending)
                                  .limit(10)
                                  .build();

    // Must create a toolChest so we can run mergeResults.
    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest(
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );

    // Must wrapped in a results finalizer to stop the runner's builtin finalizer from being called.
    final FinalizeResultsQueryRunner finalRunner = new FinalizeResultsQueryRunner(
        toolChest.mergeResults(runner),
        toolChest
    );

    final List list = finalRunner.run(QueryPlus.wrap(query), CONTEXT).toList();
    Assert.assertEquals(10, list.size());
  }
}
