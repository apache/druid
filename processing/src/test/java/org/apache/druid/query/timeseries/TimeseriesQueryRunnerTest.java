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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Doubles;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.DoubleLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
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
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 */
@RunWith(Parameterized.class)
public class TimeseriesQueryRunnerTest extends InitializedNullHandlingTest
{
  private static final String TIMESTAMP_RESULT_FIELD_NAME = "d0";
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Parameterized.Parameters(name = "{0}:descending={1},vectorize={2}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final Iterable<Object[]> baseConstructors = QueryRunnerTestHelper.cartesian(
        // runners
        QueryRunnerTestHelper.makeQueryRunners(
            new TimeseriesQueryRunnerFactory(
                new TimeseriesQueryQueryToolChest(),
                new TimeseriesQueryEngine(),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        ),
        // descending?
        Arrays.asList(false, true),
        // vectorize?
        Arrays.asList(false, true),
        // double vs. float
        Arrays.asList(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS, QueryRunnerTestHelper.COMMON_FLOAT_AGGREGATORS)
    );

    // Add vectorization tests for any indexes that support it.
    return StreamSupport
        .stream(baseConstructors.spliterator(), false)
        .filter(
            constructor -> {
              boolean canVectorize =
                  QueryRunnerTestHelper.isTestRunnerVectorizable((QueryRunner) constructor[0])
                  && !(boolean) constructor[1] /* descending */;
              final boolean vectorize = (boolean) constructor[2]; /* vectorize */
              return !vectorize || canVectorize;
            }
        )
        .collect(Collectors.toList());
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
  protected final boolean vectorize;
  private final List<AggregatorFactory> aggregatorFactoryList;

  public TimeseriesQueryRunnerTest(
      QueryRunner runner,
      boolean descending,
      boolean vectorize,
      List<AggregatorFactory> aggregatorFactoryList
  )
  {
    this.runner = runner;
    this.descending = descending;
    this.vectorize = vectorize;
    this.aggregatorFactoryList = aggregatorFactoryList;
  }

  @Test
  public void testEmptyTimeseries()
  {
    // Cannot vectorize due to "doubleFirst" aggregator.
    cannotVectorize();

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.EMPTY_INTERVAL)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          QueryRunnerTestHelper.INDEX_DOUBLE_SUM,
                                          new DoubleFirstAggregatorFactory("first", "index", null)

                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testFullOnTimeseries()
  {
    Granularity gran = Granularities.DAY;
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(gran)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_DOUBLE_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    final String[] expectedIndex = descending ?
                                   QueryRunnerTestHelper.EXPECTED_FULL_ON_INDEX_VALUES_DESC :
                                   QueryRunnerTestHelper.EXPECTED_FULL_ON_INDEX_VALUES;

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.EARLIEST :
                                  QueryRunnerTestHelper.LAST;

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
          QueryRunnerTestHelper.SKIPPED_DAY.equals(current) ? 0L : 13L,
          value.getLongMetric("rows").longValue()
      );

      if (!QueryRunnerTestHelper.SKIPPED_DAY.equals(current)) {
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
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(gran)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.EARLIEST :
                                  QueryRunnerTestHelper.LAST;

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
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      Arrays.asList(
                                          new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                          new DoubleMinAggregatorFactory("minIndex", "index")
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    DateTime expectedEarliest = DateTimes.of("2011-01-12");
    DateTime expectedLast = DateTimes.of("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
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
  public void testFullOnTimeseriesMinMaxAggregators()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(Granularities.ALL)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      QueryRunnerTestHelper.INDEX_LONG_MIN,
                                      QueryRunnerTestHelper.INDEX_LONG_MAX,
                                      QueryRunnerTestHelper.INDEX_DOUBLE_MIN,
                                      QueryRunnerTestHelper.INDEX_DOUBLE_MAX,
                                      QueryRunnerTestHelper.INDEX_FLOAT_MIN,
                                      QueryRunnerTestHelper.INDEX_FLOAT_MAX
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    DateTime expectedEarliest = DateTimes.of("2011-01-12");
    DateTime expectedLast = DateTimes.of("2011-04-15");

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    Result<TimeseriesResultValue> result = results.iterator().next();
    Assert.assertEquals(expectedEarliest, result.getTimestamp());
    Assert.assertFalse(
        StringUtils.format("Timestamp[%s] > expectedLast[%s]", result.getTimestamp(), expectedLast),
        result.getTimestamp().isAfter(expectedLast)
    );

    Assert.assertEquals(59L, (long) result.getValue().getLongMetric(QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC));
    Assert.assertEquals(1870, (long) result.getValue().getLongMetric(QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC));
    Assert.assertEquals(59.021022D, result.getValue().getDoubleMetric(QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC), 0);
    Assert.assertEquals(1870.061029D, result.getValue().getDoubleMetric(QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC), 0);
    Assert.assertEquals(59.021023F, result.getValue().getFloatMetric(QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC), 0);
    Assert.assertEquals(1870.061F, result.getValue().getFloatMetric(QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC), 0);
  }

  @Test
  public void testFullOnTimeseriesWithFilter()
  {

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          QueryRunnerTestHelper.QUALITY_UNIQUES
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    Assert.assertEquals(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront", null),
        query.getDimensionsFilter()
    );

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.EARLIEST :
                                  QueryRunnerTestHelper.LAST;

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    for (Result<TimeseriesResultValue> result : results) {
      DateTime current = result.getTimestamp();
      Assert.assertFalse(
          StringUtils.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current.isBefore(expectedLast) : current.isAfter(expectedLast)
      );

      final TimeseriesResultValue value = result.getValue();

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.SKIPPED_DAY.equals(result.getTimestamp()) ? 0L : 2L,
          value.getLongMetric("rows").longValue()
      );
      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.SKIPPED_DAY.equals(result.getTimestamp()) ? 0.0d : 2.0d,
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
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          ),
                                          QueryRunnerTestHelper.QUALITY_UNIQUES,
                                          QueryRunnerTestHelper.INDEX_LONG_MIN,
                                          QueryRunnerTestHelper.INDEX_FLOAT_MAX
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 6619L, "uniques", QueryRunnerTestHelper.UNIQUES_9,
                                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC, 78L,
                                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC, 1522.043701171875)
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9,
                                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC, 97L,
                                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC, 1321.375F)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesGrandTotal()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          QueryRunnerTestHelper.INDEX_LONG_SUM,
                                          QueryRunnerTestHelper.QUALITY_UNIQUES,
                                          QueryRunnerTestHelper.INDEX_LONG_MIN,
                                          QueryRunnerTestHelper.INDEX_DOUBLE_MAX,
                                          QueryRunnerTestHelper.INDEX_FLOAT_MIN
                                      )
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(ImmutableMap.of(TimeseriesQuery.CTX_GRAND_TOTAL, true))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = new ArrayList<>();
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    expectedResults.add(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                builder
                    .put("rows", 13L)
                    .put("index", 6619L)
                    .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                    .put(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC, 6633.0)
                    .put(QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC, 78L)
                    .put(QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC, 1522.043733D)
                    .put(QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC, 78.62254333496094F)
                    .build()
            )
        )
    );

    ImmutableMap.Builder<String, Object> builder2 = ImmutableMap.builder();
    expectedResults.add(
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                builder2
                    .put("rows", 13L)
                    .put("index", 5827L)
                    .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                    .put(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC, 5841.0)
                    .put(QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC, 97L)
                    .put(QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC, 1321.375057D)
                    .put(QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC, 97.38743591308594F)
                    .build()
            )
        )
    );

    if (descending) {
      Collections.reverse(expectedResults);
    }

    ImmutableMap.Builder<String, Object> builder3 = ImmutableMap.builder();
    expectedResults.add(
        new Result<>(
            null,
            new TimeseriesResultValue(
                builder3
                    .put("rows", 26L)
                    .put("index", 12446L)
                    .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                    .put(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC, 12473.0)
                    .put(QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC, 78L)
                    .put(QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC, 1522.043733D)
                    .put(QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC, 78.62254333496094F)
                    .build()
            )
        )
    );

    // Must create a toolChest so we can run mergeResults (which applies grand totals).
    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest();

    // Must wrapped in a results finalizer to stop the runner's builtin finalizer from being called.
    final FinalizeResultsQueryRunner finalRunner = new FinalizeResultsQueryRunner(
        toolChest.mergeResults(runner),
        toolChest
    );

    final List results = finalRunner.run(QueryPlus.wrap(query)).toList();

    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesIntervalOutOfRanges()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.EMPTY_INTERVAL)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          QueryRunnerTestHelper.INDEX_LONG_SUM,
                                          QueryRunnerTestHelper.INDEX_LONG_MIN,
                                          QueryRunnerTestHelper.INDEX_LONG_MAX,
                                          QueryRunnerTestHelper.INDEX_DOUBLE_MIN,
                                          QueryRunnerTestHelper.INDEX_DOUBLE_MAX,
                                          QueryRunnerTestHelper.INDEX_FLOAT_MIN,
                                          QueryRunnerTestHelper.INDEX_FLOAT_MAX
                                      )
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(ImmutableMap.of(TimeseriesQuery.SKIP_EMPTY_BUCKETS, false))
                                  .build();
    List<Result<TimeseriesResultValue>> expectedResults = new ArrayList<>();

    expectedResults.add(
        new Result<>(
            QueryRunnerTestHelper.EMPTY_INTERVAL.getIntervals().get(0).getStart(),
            new TimeseriesResultValue(
                TestHelper.createExpectedMap(
                    "rows",
                    0L,
                    "index",
                    NullHandling.defaultLongValue(),
                    QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC,
                    NullHandling.sqlCompatible() ? null : 1.0,
                    QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                    NullHandling.sqlCompatible() ? null : Long.MAX_VALUE,
                    QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                    NullHandling.sqlCompatible() ? null : Long.MIN_VALUE,
                    QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                    NullHandling.sqlCompatible() ? null : Double.POSITIVE_INFINITY,
                    QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                    NullHandling.sqlCompatible() ? null : Double.NEGATIVE_INFINITY,
                    QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                    NullHandling.sqlCompatible() ? null : Float.POSITIVE_INFINITY,
                    QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                    NullHandling.sqlCompatible() ? null : Float.NEGATIVE_INFINITY
                )
            )
        )
    );

    // Must create a toolChest so we can run mergeResults (which creates the zeroed-out row).
    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest();

    // Must wrapped in a results finalizer to stop the runner's builtin finalizer from being called.
    final FinalizeResultsQueryRunner finalRunner = new FinalizeResultsQueryRunner(
        toolChest.mergeResults(runner),
        toolChest
    );

    final List results = finalRunner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithVirtualColumn()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new LongSumAggregatorFactory("idx", "expr"),
                                          QueryRunnerTestHelper.QUALITY_UNIQUES
                                      )
                                  )
                                  .descending(descending)
                                  .virtualColumns(
                                      new ExpressionVirtualColumn(
                                          "expr",
                                          "index",
                                          ColumnType.FLOAT,
                                          TestExprMacroTable.INSTANCE
                                      )
                                  )
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithTimeZone()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .intervals("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
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
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithVaryingGran()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.ROWS_COUNT,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.QUALITY_UNIQUES
                                       )
                                   )
                                   .descending(descending)
                                   .context(makeContext())
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1)).toList();
    assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                   .granularity("DAY")
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.ROWS_COUNT,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.QUALITY_UNIQUES
                                       )
                                   )
                                   .context(makeContext())
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults2 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results2 = runner.run(QueryPlus.wrap(query2)).toList();
    assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesGranularityNotAlignedOnSegmentBoundariesWithFilter()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                   .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", "upfront", "total_market")
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
                                           QueryRunnerTestHelper.ROWS_COUNT,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1)).toList();
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesQueryZeroFilling()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                   .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", "upfront", "total_market")
                                   .granularity(Granularities.HOUR)
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-14T00:00:00.000Z/2011-05-01T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.ROWS_COUNT,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1)).toList();
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesQueryGranularityNotAlignedWithRollupGranularity()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                   .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", "upfront", "total_market")
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
                                           QueryRunnerTestHelper.ROWS_COUNT,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           )
                                       )
                                   )
                                   .descending(descending)
                                   .context(makeContext())
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-14T23:01Z"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 4717L)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1)).toList();
    assertExpectedResults(expectedResults1, results1);
  }

  @Test
  public void testTimeseriesWithVaryingGranWithFilter()
  {
    TimeseriesQuery query1 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                   .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", "upfront", "total_market")
                                   .granularity(new PeriodGranularity(new Period("P1M"), null, null))
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.ROWS_COUNT,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.QUALITY_UNIQUES
                                       )
                                   )
                                   .descending(descending)
                                   .context(makeContext())
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults1 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );
    Iterable<Result<TimeseriesResultValue>> results1 = runner.run(QueryPlus.wrap(query1)).toList();
    assertExpectedResults(expectedResults1, results1);

    TimeseriesQuery query2 = Druids.newTimeseriesQueryBuilder()
                                   .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                   .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", "upfront", "total_market")
                                   .granularity("DAY")
                                   .intervals(
                                       Collections.singletonList(
                                           Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                                       )
                                   )
                                   .aggregators(
                                       Arrays.asList(
                                           QueryRunnerTestHelper.ROWS_COUNT,
                                           new LongSumAggregatorFactory(
                                               "idx",
                                               "index"
                                           ),
                                           QueryRunnerTestHelper.QUALITY_UNIQUES
                                       )
                                   )
                                   .context(makeContext())
                                   .build();

    List<Result<TimeseriesResultValue>> expectedResults2 = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results2 = runner.run(QueryPlus.wrap(query2)).toList();
    assertExpectedResults(expectedResults2, results2);
  }

  @Test
  public void testTimeseriesQueryBeyondTimeRangeOfData()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .intervals(
                                      new MultipleIntervalSegmentSpec(
                                          Collections.singletonList(Intervals.of("2015-01-01/2015-01-10"))
                                      )
                                  )
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          new LongSumAggregatorFactory(
                                              "idx",
                                              "index"
                                          )
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.emptyList();

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithOrFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", "upfront", "total_market")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithRegexFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(new RegexDimFilter(
                                      QueryRunnerTestHelper.MARKET_DIMENSION,
                                      "^.p.*$",
                                      null
                                  )) // spot and upfront
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "spot")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES,
                                      QueryRunnerTestHelper.INDEX_LONG_MIN
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows", 9L,
                    "index", 1102L,
                    "addRowsIndexConstant", 1112.0,
                    "uniques", QueryRunnerTestHelper.UNIQUES_9,
                    QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC, 78L
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
                    "uniques", QueryRunnerTestHelper.UNIQUES_9,
                    QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC, 97L
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter2()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilter3()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilterAndOr()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new OrDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", "business")
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiDimFilter()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", null)
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithOtherMultiDimFilter()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "business", null)
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterInOr()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(
                                      QueryRunnerTestHelper.MARKET_DIMENSION,
                                      "spot",
                                      "upfront",
                                      "total_market",
                                      "billyblank"
                                  )
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }


  @Test
  public void testTimeseriesWithInFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(
                                      new InDimFilter(
                                          QueryRunnerTestHelper.MARKET_DIMENSION,
                                          Arrays.asList(
                                              "spot",
                                              "upfront",
                                              "total_market",
                                              "billyblank"
                                          ),
                                          null
                                      )
                                  )
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDimAndOr()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new OrDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", "business", "billyblank")
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithFilterOnNonExistentDimensionSkipBuckets()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters("bobby", "billy")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext(ImmutableMap.of(TimeseriesQuery.SKIP_EMPTY_BUCKETS, "true")))
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.emptyList();

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query))
                                                            .toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNullFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters("bobby", null)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query))
                                                            .toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithInvertedFilterOnNonExistentDimension()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(new NotDimFilter(new SelectorDimFilter("bobby", "sally", null)))
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query))
                                                            .toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilter()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "billy")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithNonExistentFilterAndMultiDim()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "billy", null),
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "business", null)
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(andDimFilter)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(aggregatorFactoryList)
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithMultiValueFilteringJavascriptAggregator()
  {
    // Cannot vectorize due to JavaScript aggregators.
    cannotVectorize();

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      ImmutableList.of(
                                          QueryRunnerTestHelper.INDEX_DOUBLE_SUM,
                                          QueryRunnerTestHelper.JS_INDEX_SUM_IF_PLACEMENTISH_A,
                                          QueryRunnerTestHelper.JS_PLACEMENTISH_COUNT
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    Iterable<Result<TimeseriesResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            QueryRunnerTestHelper.FIRST_TO_THIRD.getIntervals().get(0).getStart(),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "index", 12459.361190795898d,
                    "nindex", 283.31103515625d,
                    "pishcount", 52d
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueFilteringJavascriptAggregatorAndAlsoRegularFilters()
  {
    // Cannot vectorize due to JavaScript aggregators.
    cannotVectorize();

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .filters(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      ImmutableList.of(
                                          QueryRunnerTestHelper.INDEX_DOUBLE_SUM,
                                          QueryRunnerTestHelper.JS_INDEX_SUM_IF_PLACEMENTISH_A,
                                          QueryRunnerTestHelper.JS_PLACEMENTISH_COUNT
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            QueryRunnerTestHelper.FIRST_TO_THIRD.getIntervals().get(0).getStart(),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "index", 283.31103515625d,
                    "nindex", 283.31103515625d,
                    "pishcount", 4d
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithFirstLastAggregator()
  {
    // Cannot vectorize due to "doubleFirst", "doubleLast" aggregators.
    cannotVectorize();

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.MONTH_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      ImmutableList.of(
                                          new DoubleFirstAggregatorFactory("first", "index", null),
                                          new DoubleLastAggregatorFactory("last", "index", null)
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
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
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .filters(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "preferred")
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    TimeseriesQuery query1 = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();
    Iterable<Result<TimeseriesResultValue>> expectedResults = runner.run(QueryPlus.wrap(query1)).toList();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilter2()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .filters(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a")
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    TimeseriesQuery query1 = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .filters(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive")
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();
    Iterable<Result<TimeseriesResultValue>> expectedResults = runner.run(QueryPlus.wrap(query1)).toList();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr1()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a", null)
    );
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .filters(andDimFilter)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    AndDimFilter andDimFilter2 = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", null)
    );

    TimeseriesQuery query2 = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .filters(andDimFilter2)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();
    Iterable<Result<TimeseriesResultValue>> expectedResults = runner.run(QueryPlus.wrap(query2)).toList();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithMultiValueDimFilterAndOr2()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new OrDimFilter(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a", "b")
    );
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .filters(andDimFilter)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    AndDimFilter andDimFilter2 = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null),
        new OrDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "automotive", "business")
    );

    TimeseriesQuery query2 = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.DAY_GRAN)
        .filters(andDimFilter2)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(aggregatorFactoryList)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();
    Iterable<Result<TimeseriesResultValue>> expectedResults = runner.run(QueryPlus.wrap(query2)).toList();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAgg()
  {
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    ImmutableList.of(
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null)
                        )
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("filteredAgg", 18L)
                            .put("addRowsIndexConstant", 12486.361190795898d)
                            .put("index", 12459.361190795898d)
                            .put("uniques", 9.019833517963864d)
                            .put("rows", 26L)
                            .build()
            )
        )
    );

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeSeriesWithFilteredAggAndExpressionFilteredAgg()
  {
    // can't vectorize if expression
    cannotVectorize();
    TimeseriesQuery query = Druids
        .newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    ImmutableList.of(
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "spot", null)
                        ),
                        new LongSumAggregatorFactory(
                            "altLongCount",
                            null,
                            "if (market == 'spot', 1, 0)",
                            TestExprMacroTable.INSTANCE
                        ),
                        new DoubleSumAggregatorFactory(
                            "altDoubleCount",
                            null,
                            "if (market == 'spot', 1, 0)",
                            TestExprMacroTable.INSTANCE
                        ),
                        new FloatSumAggregatorFactory(
                            "altFloatCount",
                            null,
                            "if (market == 'spot', 1, 0)",
                            TestExprMacroTable.INSTANCE
                        )
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("filteredAgg", 18L)
                            .put("addRowsIndexConstant", 12486.361190795898d)
                            .put("index", 12459.361190795898d)
                            .put("uniques", 9.019833517963864d)
                            .put("rows", 26L)
                            .put("altLongCount", 18L)
                            .put("altDoubleCount", 18.0)
                            .put("altFloatCount", 18.0f)
                            .build()
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
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
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
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();

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
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
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
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();

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
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    Collections.singletonList(
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new NotDimFilter(
                                new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "LolLol", null)
                            )
                        )
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
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
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    aggregatorFactoryList,
                    Collections.singletonList(
                        new FilteredAggregatorFactory(
                            new CountAggregatorFactory("filteredAgg"),
                            new NotDimFilter(new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, null, null))
                        )
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .descending(descending)
        .context(makeContext())
        .build();

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();
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
    // Cannot vectorize due to JavaScript aggregators.
    cannotVectorize();

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.JS_COUNT_IF_TIME_GREATER_THAN,
                                      QueryRunnerTestHelper.TIME_LONG_SUM
                                  )
                                  .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                  .descending(descending)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query)).toList();

    assertExpectedResults(expectedResults, actualResults);
  }

  @Test
  public void testTimeseriesWithBoundFilter1()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(
                                      new AndDimFilter(
                                          Arrays.asList(
                                              new BoundDimFilter(
                                                  QueryRunnerTestHelper.MARKET_DIMENSION,
                                                  "spa",
                                                  "spot",
                                                  true,
                                                  null,
                                                  null,
                                                  null,
                                                  StringComparators.LEXICOGRAPHIC
                                              ),
                                              new BoundDimFilter(
                                                  QueryRunnerTestHelper.MARKET_DIMENSION,
                                                  "spot",
                                                  "spotify",
                                                  null,
                                                  true,
                                                  null,
                                                  null,
                                                  StringComparators.LEXICOGRAPHIC
                                              ),
                                              new BoundDimFilter(
                                                  QueryRunnerTestHelper.MARKET_DIMENSION,
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
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithTimestampResultFieldContextForArrayResponse()
  {
    Granularity gran = Granularities.DAY;
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(gran)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_DOUBLE_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(
                                      makeContext(
                                          ImmutableMap.of(
                                              TimeseriesQuery.CTX_TIMESTAMP_RESULT_FIELD, TIMESTAMP_RESULT_FIELD_NAME,
                                              TimeseriesQuery.SKIP_EMPTY_BUCKETS, true
                                          )
                                      )
                                  )
                                  .build();

    Assert.assertEquals(TIMESTAMP_RESULT_FIELD_NAME, query.getTimestampResultField());

    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest();

    RowSignature rowSignature = toolChest.resultArraySignature(query);
    Assert.assertNotNull(rowSignature);
    List<String> columnNames = rowSignature.getColumnNames();
    Assert.assertNotNull(columnNames);
    Assert.assertEquals(6, columnNames.size());
    Assert.assertEquals("__time", columnNames.get(0));
    Assert.assertEquals(TIMESTAMP_RESULT_FIELD_NAME, columnNames.get(1));
    Assert.assertEquals("rows", columnNames.get(2));
    Assert.assertEquals("index", columnNames.get(3));
    Assert.assertEquals("uniques", columnNames.get(4));
    Assert.assertEquals("addRowsIndexConstant", columnNames.get(5));

    Sequence<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query));
    Sequence<Object[]> resultsAsArrays = toolChest.resultsAsArrays(query, results);

    Assert.assertNotNull(resultsAsArrays);

    final String[] expectedIndex = descending ?
                                   QueryRunnerTestHelper.EXPECTED_FULL_ON_INDEX_VALUES_DESC :
                                   QueryRunnerTestHelper.EXPECTED_FULL_ON_INDEX_VALUES;
    final String[] expectedIndexToUse = Arrays.stream(expectedIndex)
                                              .filter(eachIndex -> !"0.0".equals(eachIndex))
                                              .toArray(String[]::new);

    final Long expectedLast = descending ?
                                  QueryRunnerTestHelper.EARLIEST.getMillis() :
                                  QueryRunnerTestHelper.LAST.getMillis();

    int count = 0;
    Object[] lastResult = null;
    for (Object[] result : resultsAsArrays.toList()) {
      Long current = (Long) result[0];
      Assert.assertFalse(
          StringUtils.format("Timestamp[%s] > expectedLast[%s]", current, expectedLast),
          descending ? current < expectedLast : current > expectedLast
      );

      Assert.assertEquals(
          (Long) result[1],
          current,
          0
      );

      Assert.assertEquals(
          QueryRunnerTestHelper.SKIPPED_DAY.getMillis() == current ? (Long) 0L : (Long) 13L,
          result[2]
      );

      if (QueryRunnerTestHelper.SKIPPED_DAY.getMillis() != current) {
        Assert.assertEquals(
            Doubles.tryParse(expectedIndexToUse[count]).doubleValue(),
            (Double) result[3],
            (Double) result[3] * 1e-6
        );
        Assert.assertEquals(
            (Double) result[4],
            9.0d,
            0.02
        );
        Assert.assertEquals(
            new Double(expectedIndexToUse[count]) + 13L + 1L,
            (Double) result[5],
            (Double) result[5] * 1e-6
        );
      } else {
        if (NullHandling.replaceWithDefault()) {
          Assert.assertEquals(
              0.0D,
              (Double) result[3],
              (Double) result[3] * 1e-6
          );
          Assert.assertEquals(
              0.0D,
              (Double) result[4],
              0.02
          );
          Assert.assertEquals(
              new Double(expectedIndexToUse[count]) + 1L,
              (Double) result[5],
              (Double) result[5] * 1e-6
          );
        } else {
          Assert.assertNull(
              result[3]
          );
          Assert.assertEquals(
              (Double) result[4],
              0.0,
              0.02
          );
          Assert.assertNull(
              result[5]
          );
        }
      }

      lastResult = result;
      ++count;
    }
    Assert.assertEquals(expectedLast, lastResult[0]);
  }

  @Test
  public void testTimeseriesWithTimestampResultFieldContextForMapResponse()
  {
    Granularity gran = Granularities.DAY;
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(gran)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_DOUBLE_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .descending(descending)
                                  .context(
                                      makeContext(
                                          ImmutableMap.of(
                                              TimeseriesQuery.CTX_TIMESTAMP_RESULT_FIELD, TIMESTAMP_RESULT_FIELD_NAME,
                                              TimeseriesQuery.SKIP_EMPTY_BUCKETS, true
                                          )
                                      )
                                  )
                                  .build();

    Assert.assertEquals(TIMESTAMP_RESULT_FIELD_NAME, query.getTimestampResultField());

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();

    final String[] expectedIndex = descending ?
                                   QueryRunnerTestHelper.EXPECTED_FULL_ON_INDEX_VALUES_DESC :
                                   QueryRunnerTestHelper.EXPECTED_FULL_ON_INDEX_VALUES;
    final String[] expectedIndexToUse = Arrays.stream(expectedIndex)
                                              .filter(eachIndex -> !"0.0".equals(eachIndex))
                                              .toArray(String[]::new);

    final DateTime expectedLast = descending ?
                                  QueryRunnerTestHelper.EARLIEST :
                                  QueryRunnerTestHelper.LAST;

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
          value.getLongMetric(TIMESTAMP_RESULT_FIELD_NAME),
          current.getMillis(),
          0
      );

      Assert.assertEquals(
          result.toString(),
          QueryRunnerTestHelper.SKIPPED_DAY.equals(current) ? 0L : 13L,
          value.getLongMetric("rows").longValue()
      );

      if (!QueryRunnerTestHelper.SKIPPED_DAY.equals(current)) {
        Assert.assertEquals(
            result.toString(),
            Doubles.tryParse(expectedIndexToUse[count]).doubleValue(),
            value.getDoubleMetric("index").doubleValue(),
            value.getDoubleMetric("index").doubleValue() * 1e-6
        );
        Assert.assertEquals(
            result.toString(),
            new Double(expectedIndexToUse[count]) +
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
              new Double(expectedIndexToUse[count]) + 1L,
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
  public void testTimeSeriesWithSelectionFilterLookupExtractionFn()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("spot", "upfront");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, true, null, true, true);
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(
                                      new SelectorDimFilter(
                                          QueryRunnerTestHelper.MARKET_DIMENSION,
                                          "upfront",
                                          lookupExtractionFn
                                      )
                                  )
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      QueryRunnerTestHelper.ROWS_COUNT,
                                      QueryRunnerTestHelper.INDEX_LONG_SUM,
                                      QueryRunnerTestHelper.QUALITY_UNIQUES
                                  )
                                  .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                  .context(makeContext())
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

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    TestHelper.assertExpectedResults(expectedResults, results);

    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest();
    QueryRunner<Result<TimeseriesResultValue>> optimizedRunner = toolChest.postMergeQueryDecoration(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)));
    Iterable<Result<TimeseriesResultValue>> results2 = new FinalizeResultsQueryRunner(optimizedRunner, toolChest)
        .run(QueryPlus.wrap(query))
        .toList();
    TestHelper.assertExpectedResults(expectedResults, results2);

  }

  @Test
  public void testTimeseriesWithLimit()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                  .aggregators(
                                      Arrays.asList(
                                          QueryRunnerTestHelper.ROWS_COUNT,
                                          QueryRunnerTestHelper.QUALITY_UNIQUES
                                      )
                                  )
                                  .descending(descending)
                                  .limit(10)
                                  .context(makeContext())
                                  .build();

    // Must create a toolChest so we can run mergeResults.
    QueryToolChest<Result<TimeseriesResultValue>, TimeseriesQuery> toolChest = new TimeseriesQueryQueryToolChest();

    // Must wrapped in a results finalizer to stop the runner's builtin finalizer from being called.
    final FinalizeResultsQueryRunner finalRunner = new FinalizeResultsQueryRunner(
        toolChest.mergeResults(runner),
        toolChest
    );

    final List list = finalRunner.run(QueryPlus.wrap(query)).toList();
    Assert.assertEquals(10, list.size());
  }

  @Test
  public void testTimeseriesWithPostAggregatorReferencingTimestampResultField()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "spot")
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .postAggregators(
                                      new FieldAccessPostAggregator("timestampInPostAgg", "myTimestamp")
                                  )
                                  .descending(descending)
                                  .context(
                                      makeContext(
                                          ImmutableMap.of(TimeseriesQuery.CTX_TIMESTAMP_RESULT_FIELD, "myTimestamp")
                                      )
                                  )
                                  .build();

    final DateTime aprilFirst = DateTimes.of("2011-04-01");
    final DateTime aprilSecond = DateTimes.of("2011-04-02");

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            aprilFirst,
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "myTimestamp", aprilFirst.getMillis(),
                    "timestampInPostAgg", aprilFirst.getMillis()
                )
            )
        ),
        new Result<>(
            aprilSecond,
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "myTimestamp", aprilSecond.getMillis(),
                    "timestampInPostAgg", aprilSecond.getMillis()
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithExpressionAggregator()
  {
    // expression agg cannot vectorize
    cannotVectorize();
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      Arrays.asList(
                                          new ExpressionLambdaAggregatorFactory(
                                              "diy_count",
                                              ImmutableSet.of(),
                                              null,
                                              "0",
                                              null,
                                              false,
                                              "__acc + 1",
                                              "__acc + diy_count",
                                              null,
                                              null,
                                              null,
                                              TestExprMacroTable.INSTANCE
                                          ),
                                          new ExpressionLambdaAggregatorFactory(
                                              "diy_sum",
                                              ImmutableSet.of("index"),
                                              null,
                                              "0.0",
                                              null,
                                              null,
                                              "__acc + index",
                                              null,
                                              null,
                                              null,
                                              null,
                                              TestExprMacroTable.INSTANCE
                                          ),
                                          new ExpressionLambdaAggregatorFactory(
                                              "diy_decomposed_sum",
                                              ImmutableSet.of("index"),
                                              null,
                                              "0.0",
                                              "<DOUBLE>[]",
                                              null,
                                              "__acc + index",
                                              "array_concat(__acc, diy_decomposed_sum)",
                                              null,
                                              "fold((x, acc) -> x + acc, o, 0.0)",
                                              null,
                                              TestExprMacroTable.INSTANCE
                                          ),
                                          new ExpressionLambdaAggregatorFactory(
                                              "array_agg_distinct",
                                              ImmutableSet.of(QueryRunnerTestHelper.MARKET_DIMENSION),
                                              "acc",
                                              "[]",
                                              null,
                                              null,
                                              "array_set_add(acc, market)",
                                              "array_set_add_all(acc, array_agg_distinct)",
                                              null,
                                              null,
                                              null,
                                              TestExprMacroTable.INSTANCE
                                          )
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "diy_count", 13L,
                    "diy_sum", 6626.151569,
                    "diy_decomposed_sum", 6626.151569,
                    "array_agg_distinct", new String[] {"upfront", "spot", "total_market"}
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-02"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "diy_count", 13L,
                    "diy_sum", 5833.209718,
                    "diy_decomposed_sum", 5833.209718,
                    "array_agg_distinct", new String[] {"upfront", "spot", "total_market"}
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesWithExpressionAggregatorTooBig()
  {
    // expression agg cannot vectorize
    cannotVectorize();
    if (!vectorize) {
      // size bytes when it overshoots varies slightly between algorithms
      expectedException.expectMessage("Exceeded memory usage when aggregating type [ARRAY<STRING>]");
    }
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                  .granularity(Granularities.DAY)
                                  .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                  .aggregators(
                                      Collections.singletonList(
                                          new ExpressionLambdaAggregatorFactory(
                                              "array_agg_distinct",
                                              ImmutableSet.of(QueryRunnerTestHelper.MARKET_DIMENSION),
                                              "acc",
                                              "[]",
                                              null,
                                              null,
                                              "array_set_add(acc, market)",
                                              "array_set_add_all(acc, array_agg_distinct)",
                                              null,
                                              null,
                                              HumanReadableBytes.valueOf(10),
                                              TestExprMacroTable.INSTANCE
                                          )
                                      )
                                  )
                                  .descending(descending)
                                  .context(makeContext())
                                  .build();

    runner.run(QueryPlus.wrap(query)).toList();
  }

  @Test
  public void testTimeseriesCardinalityAggOnMultiStringExpression()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .virtualColumns(
            new ExpressionVirtualColumn("v0", "concat(quality,market)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        )
        .aggregators(
            QueryRunnerTestHelper.ROWS_COUNT,
            new CardinalityAggregatorFactory(
                "numVals",
                ImmutableList.of(DefaultDimensionSpec.of("v0")),
                false
            )
        )
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows",
                    26L,
                    "numVals",
                    13.041435202975777d
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  @Test
  public void testTimeseriesCardinalityAggOnHyperUnique()
  {
    // Cardinality aggregator on complex columns (like hyperUnique) returns 0.

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(
            QueryRunnerTestHelper.ROWS_COUNT,
            new CardinalityAggregatorFactory(
                "cardinality",
                ImmutableList.of(DefaultDimensionSpec.of("quality_uniques")),
                false
            ),
            new HyperUniquesAggregatorFactory("hyperUnique", "quality_uniques", false, false)
        )
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TimeseriesResultValue(
                ImmutableMap.of(
                    "rows",
                    26L,
                    "cardinality",
                    NullHandling.replaceWithDefault() ? 1.0002442201269182 : 0.0d,
                    "hyperUnique",
                    9.019833517963864d
                )
            )
        )
    );

    Iterable<Result<TimeseriesResultValue>> results = runner.run(QueryPlus.wrap(query)).toList();
    assertExpectedResults(expectedResults, results);
  }

  protected Map<String, Object> makeContext()
  {
    return makeContext(ImmutableMap.of());
  }

  protected Map<String, Object> makeContext(final Map<String, Object> myContext)
  {
    final Map<String, Object> context = new HashMap<>();
    context.put(QueryContexts.VECTORIZE_KEY, vectorize ? "force" : "false");
    context.put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize ? "force" : "false");
    context.put(QueryContexts.VECTOR_SIZE_KEY, 16); // Small vector size to ensure we use more than one.
    context.putAll(myContext);
    return context;
  }

  protected void cannotVectorize()
  {
    if (vectorize) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("Cannot vectorize!");
    }
  }
}
