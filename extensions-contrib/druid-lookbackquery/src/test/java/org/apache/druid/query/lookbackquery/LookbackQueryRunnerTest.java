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

package org.apache.druid.query.lookbackquery;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.MapQueryToolChestWarehouse;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
import org.apache.druid.query.groupby.having.LessThanHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.server.log.RequestLogger;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IArgumentMatcher;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.Period;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.and;
import static org.easymock.EasyMock.anyObject;
import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.easymock.EasyMock.mock;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.reset;
import static org.easymock.EasyMock.strictMock;
import static org.easymock.EasyMock.verify;

public class LookbackQueryRunnerTest
{

  public static final DateTime measurementDateTime = DateTimes.of("2015-10-10");
  public static final List<Period> lookbackOffsets = Collections.singletonList(LookbackQueryTestResources.period);
  public static final DateTime cohortDateTime = measurementDateTime.plus(LookbackQueryTestResources.period);
  public static final String TIMESERIES = "timeseries";
  public static final String LOOKBACK = "lookback";
  private static final RequestLogger mockLogger = strictMock(RequestLogger.class);

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static final QueryToolChestWarehouse warehouse = new MapQueryToolChestWarehouse(
      new ImmutableMap.Builder()
          .put(TimeseriesQuery.class, new TimeseriesQueryQueryToolChest(null))
          .put(GroupByQuery.class, new GroupByQueryQueryToolChest(null, null))
          .build()
  );

  @SuppressWarnings("unchecked")
  @SafeVarargs
  public static List<Result<LookbackResultValue>> getActualResults(
      LookbackQuery query,
      List<Object> measurementResults,
      List<Object>... cohortResultsList
  )
  {

    Sequence<Object> measurementResultSeq = Sequences.simple(measurementResults);

    // Set up Mock Queryrunner
    QueryRunner<Object> mockQueryRunner = mock(QueryRunner.class);
    QuerySegmentWalker mockWalker = strictMock(QuerySegmentWalker.class);
    String queryId = (String) query.getContext().getOrDefault("queryId", "");
    List<Period> lookbackOffsets = query.getLookbackOffsets();
    int index = 0;

    expect(mockWalker.getQueryRunnerForIntervals(anyObject(Query.class), anyObject(List.class)))
        .andReturn(mockQueryRunner).anyTimes();
    for (List<Object> cohortResults : cohortResultsList) {
      Sequence<Object> cohortResultSeq = Sequences.simple(cohortResults);
      Period lookbackOffset = lookbackOffsets.get(index++);
      expect(mockQueryRunner.run(
          QueryPlus.wrap(queryIdMatches(queryId + "cohort_query" + lookbackOffset.toString())),
          isA(HashMap.class)
      )).andReturn(cohortResultSeq);
    }
    expect(mockQueryRunner.run(
        QueryPlus.wrap(queryIdMatches(queryId + "measurement_query")),
        isA(HashMap.class)
    )).andReturn(measurementResultSeq);
    replay(mockQueryRunner, mockWalker);

    QueryRunner<Result<LookbackResultValue>> runner = new LookbackQueryRunner(mockWalker, warehouse);
    Sequence<Result<LookbackResultValue>> actualResult = runner.run(QueryPlus.wrap(query), new HashMap<>());

    List<Result<LookbackResultValue>> results = new ArrayList<>();
    Sequences.toList(actualResult, results);
    Assert.assertFalse(results.isEmpty());

    verify(mockQueryRunner, mockWalker);
    reset(mockQueryRunner, mockWalker);

    return Sequences.toList(actualResult, new ArrayList<>());
  }

  @SuppressWarnings("unchecked")
  public static <T> Result<T> getResult(DateTime dateTime, String resultType, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = new HashMap<>();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    if (TIMESERIES.equals(resultType)) {
      return new Result<T>(dateTime, (T) new TimeseriesResultValue(theVals));
    } else {
      LookbackResultValue resultValue = new LookbackResultValue(theVals);
      return new Result<T>(dateTime, (T) resultValue);
    }
  }

  public static Row createExpectedRow(DateTime timestamp, Object... vals)
  {
    Preconditions.checkArgument(vals.length % 2 == 0);

    Map<String, Object> theVals = new HashMap<>();
    for (int i = 0; i < vals.length; i += 2) {
      theVals.put(vals[i].toString(), vals[i + 1]);
    }

    DateTime ts = DateTimes.of(timestamp.toString());
    return new MapBasedRow(ts, theVals);
  }

  //@Test
  public void testLookbackQueryWithNoPostAggs()
  {
    // Test a simple Lookback query with a Timeseries datasource containing only aggs.
    // The Lookback query has no Postaggs. Verify that the interval of the cohort query is correct.

    // Build lookback query
    LookbackQuery.LookbackQueryBuilder builder = LookbackQuery.builder();

    LookbackQuery query = builder
        .setDatasource(LookbackQueryTestResources.timeSeriesQueryDataSource)
        .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
        .setLookbackOffsets(lookbackOffsets).build();

    // Set up Mock Queryrunner
    QueryRunner<Object> mockQueryRunner = mock(QueryRunner.class);
    QuerySegmentWalker mockWalker = strictMock(QuerySegmentWalker.class);
    Capture<Query<Object>> capturedQuery = EasyMock.newCapture();

    // measurement results
    Result<TimeseriesResultValue> measurementResults = getResult(measurementDateTime, TIMESERIES, "pageViews", 2,
                                                                 "timeSpent", 3
    );

    // cohort results
    Result<TimeseriesResultValue> cohortResults = getResult(cohortDateTime,
                                                            TIMESERIES, "pageViews", 4, "timeSpent", 9
    );

    // Generate the results that should be obtained after running the mock query
    Sequence<Object> measurementResultSeq = Sequences.simple(Collections.singletonList(measurementResults));
    Sequence<Object> cohortResultSeq = Sequences.simple(Collections.singletonList(cohortResults));

    expect(mockWalker.getQueryRunnerForIntervals(anyObject(Query.class), anyObject(List.class)))
        .andReturn(mockQueryRunner).anyTimes();
    // Run the mock query runner
    expect(mockQueryRunner.run(
        QueryPlus.wrap(and(queryIdMatches("2cohort_queryP-1D"), capture(capturedQuery))),
        isA(HashMap.class)
    )).andReturn(cohortResultSeq);
    expect(mockQueryRunner.run(QueryPlus.wrap(queryIdMatches("2measurement_query")), isA(HashMap.class))).andReturn(
        measurementResultSeq);
    replay(mockQueryRunner, mockWalker);

    QueryRunner<Result<LookbackResultValue>> runner = new LookbackQueryRunner(mockWalker, warehouse);
    Sequence<Result<LookbackResultValue>> actualResult = runner.run(QueryPlus.wrap(query), new HashMap<>());
    List<Result<LookbackResultValue>> actualResultList =
        Sequences.toList(actualResult, new ArrayList<>());

    // Expected Result
    Result<LookbackResultValue> expectedResult = getResult(
        measurementDateTime,
        LOOKBACK,
        "timeSpent",
        3,
        "lookback_P-1D_timeSpent",
        9,
        "pageViews",
        2,
        "lookback_P-1D_pageViews",
        4,
        "pageViewsPerTimeSpent",
        2.0 / 3.0,
        "lookback_P-1D_pageViewsPerTimeSpent",
        4.0 / 9.0
    );
    List<Result<LookbackResultValue>> expectedList = new ArrayList<>();
    expectedList.add(expectedResult);

    Assert.assertEquals(expectedList, actualResultList);

    Interval measurementInterval = LookbackQueryTestResources.oneDayQuerySegmentSpec.getIntervals().get(0);
    Interval expectedCohortInterval =
        new Interval(
            measurementInterval.getStart().withPeriodAdded(LookbackQueryTestResources.period, 1),
            measurementInterval.getEnd().withPeriodAdded(LookbackQueryTestResources.period, 1)
        );

    verify(mockQueryRunner, mockWalker);
    reset(mockQueryRunner, mockWalker);

    Assert.assertEquals(expectedCohortInterval, capturedQuery.getValue().getIntervals().get(0));

  }

  //@Test
  public void testLookbackQueryWithUnionDataSource()
  {
    // Test a simple Lookback query with a Timeseries datasource containing only aggs.
    // The Lookback query has no Postaggs. Verify that the interval of the cohort query is correct.

    // Build lookback query
    LookbackQuery.LookbackQueryBuilder builder = LookbackQuery.builder();

    LookbackQuery query = builder
        .setDatasource(LookbackQueryTestResources.timeSeriesQueryUnionDataSource)
        .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
        .setLookbackOffsets(lookbackOffsets).build();

    // Set up Mock Queryrunner
    QueryRunner<Object> mockQueryRunner = mock(QueryRunner.class);
    QuerySegmentWalker mockWalker = strictMock(QuerySegmentWalker.class);
    Capture<Query<Object>> capturedQuery = EasyMock.newCapture();

    // measurement results
    Result<TimeseriesResultValue> measurementResults = getResult(measurementDateTime, TIMESERIES, "pageViews", 2,
                                                                 "timeSpent", 3
    );

    // cohort results
    Result<TimeseriesResultValue> cohortResults = getResult(cohortDateTime,
                                                            TIMESERIES, "pageViews", 4, "timeSpent", 9
    );

    // Generate the results that should be obtained after running the mock query
    Sequence<Object> measurementResultSeq = Sequences.simple(Collections.singletonList(measurementResults));
    Sequence<Object> cohortResultSeq = Sequences.simple(Collections.singletonList(cohortResults));

    expect(mockWalker.getQueryRunnerForIntervals(anyObject(Query.class), anyObject(List.class)))
        .andReturn(mockQueryRunner).anyTimes();
    // Run the mock query runner
    expect(mockQueryRunner.run(
        QueryPlus.wrap(and(queryIdMatches("2cohort_queryP-1D"), capture(capturedQuery))),
        isA(HashMap.class)
    )).andReturn(cohortResultSeq);
    expect(mockQueryRunner.run(QueryPlus.wrap(queryIdMatches("2measurement_query")), isA(HashMap.class))).andReturn(
        measurementResultSeq);
    replay(mockQueryRunner, mockWalker);

    QueryRunner<Result<LookbackResultValue>> runner = new LookbackQueryRunner(mockWalker, warehouse);
    Sequence<Result<LookbackResultValue>> actualResult = runner.run(QueryPlus.wrap(query), new HashMap<>());
    List<Result<LookbackResultValue>> actualResultList =
        Sequences.toList(actualResult, new ArrayList<>());

    // Expected Result
    Result<LookbackResultValue> expectedResult = getResult(
        measurementDateTime,
        LOOKBACK,
        "timeSpent",
        3,
        "lookback_P-1D_timeSpent",
        9,
        "pageViews",
        2,
        "lookback_P-1D_pageViews",
        4,
        "pageViewsPerTimeSpent",
        2.0 / 3.0,
        "lookback_P-1D_pageViewsPerTimeSpent",
        4.0 / 9.0
    );
    List<Result<LookbackResultValue>> expectedList = new ArrayList<>();
    expectedList.add(expectedResult);

    Assert.assertEquals(expectedList, actualResultList);

    Interval measurementInterval = LookbackQueryTestResources.oneDayQuerySegmentSpec.getIntervals().get(0);
    Interval expectedCohortInterval =
        new Interval(
            measurementInterval.getStart().withPeriodAdded(LookbackQueryTestResources.period, 1),
            measurementInterval.getEnd().withPeriodAdded(LookbackQueryTestResources.period, 1)
        );

    verify(mockQueryRunner, mockWalker);
    reset(mockQueryRunner, mockWalker);

    Assert.assertEquals(expectedCohortInterval, capturedQuery.getValue().getIntervals().get(0));

  }

  //@Test
  public void testGroupByQueryWithMuiltipDimsAndLookbackMetrics()
  {

    GroupByQuery groupbyQuery = GroupByQuery.builder()
                                            .setDataSource("slice1")
                                            .setQuerySegmentSpec(LookbackQueryTestResources.oneDayQuerySegmentSpec)
                                            .setGranularity(LookbackQueryTestResources.dayGran)
                                            .setAggregatorSpecs(Arrays.asList(
                                                LookbackQueryTestResources.pageViews,
                                                LookbackQueryTestResources.timeSpent
                                            ))
                                            .setDimensions(Arrays.asList(
                                                LookbackQueryTestResources.dim1,
                                                LookbackQueryTestResources.dim2
                                            ))
                                            .build();

    List<PostAggregator> lookbackPostAggs = Lists.<PostAggregator>newArrayList(
        LookbackQueryTestResources.totalPageViews,
        LookbackQueryTestResources.totalTimeSpent
    );

    LookbackQuery lookbackQuery = LookbackQuery.builder()
                                               .setLookbackOffsets(lookbackOffsets)
                                               .setDatasource(new QueryDataSource(groupbyQuery))
                                               .setPostAggregatorSpecs(lookbackPostAggs)
                                               .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                               .build();

    List<Object> measurementResults = Arrays.asList(
        createExpectedRow(measurementDateTime, "gender", "f", "country", "usa", "pageViews", 2,
                          "timeSpent", 3
        ),
        createExpectedRow(measurementDateTime, "gender", "m", "country", "usa", "pageViews", 2,
                          "timeSpent", 3
        ),
        createExpectedRow(measurementDateTime, "gender", "f", "country", "france", "pageViews", 2,
                          "timeSpent", 3
        ),
        createExpectedRow(measurementDateTime, "gender", "m", "country", "france", "pageViews", 2,
                          "timeSpent", 3
        )
    );

    List<Object> cohortResults = Arrays.asList(
        createExpectedRow(cohortDateTime, "gender", "f", "country", "usa", "pageViews", 4, "timeSpent",
                          5
        ),
        createExpectedRow(cohortDateTime, "gender", "m", "country", "usa", "pageViews", 4, "timeSpent",
                          5
        ),
        createExpectedRow(cohortDateTime, "gender", "f", "country", "france", "pageViews", 4,
                          "timeSpent", 5
        ),
        createExpectedRow(cohortDateTime, "gender", "m", "country", "france", "pageViews", 4,
                          "timeSpent", 5
        )
    );

    // Generate the results that should be obtained after running the mock query
    List<Result<LookbackResultValue>> actualResultList =
        getActualResults(lookbackQuery, measurementResults, cohortResults);

    // Expected Result
    List<Result<LookbackResultValue>> expectedResultList = Arrays.asList(
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "usa", "pageViews", 2, "timeSpent", 3, "totalPageViews", 6.0, "totalTimeSpent",
                  8.0, "lookback_P-1D_pageViews", 4, "lookback_P-1D_timeSpent", 5
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "usa", "pageViews", 2, "timeSpent", 3, "totalPageViews", 6.0, "totalTimeSpent",
                  8.0, "lookback_P-1D_pageViews", 4, "lookback_P-1D_timeSpent", 5
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "france", "pageViews", 2, "timeSpent", 3, "totalPageViews", 6.0,
                  "totalTimeSpent", 8.0, "lookback_P-1D_pageViews", 4, "lookback_P-1D_timeSpent", 5
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "france", "pageViews", 2, "timeSpent", 3, "totalPageViews", 6.0,
                  "totalTimeSpent", 8.0, "lookback_P-1D_pageViews", 4, "lookback_P-1D_timeSpent", 5
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);
  }

  //@Test
  public void testLookbackQueryHaving()
  {

    GroupByQuery groupbyQuery = GroupByQuery.builder().setDataSource("slice1")
                                            .setQuerySegmentSpec(LookbackQueryTestResources.oneDayQuerySegmentSpec)
                                            .setGranularity(LookbackQueryTestResources.dayGran)
                                            .setAggregatorSpecs(Arrays.asList(
                                                LookbackQueryTestResources.pageViews,
                                                LookbackQueryTestResources.timeSpent
                                            ))
                                            .setDimensions(Arrays.asList(
                                                LookbackQueryTestResources.dim1,
                                                LookbackQueryTestResources.dim2
                                            ))
                                            .build();

    List<PostAggregator> lookbackPostAggs = Lists.<PostAggregator>newArrayList(
        LookbackQueryTestResources.totalPageViews,
        LookbackQueryTestResources.totalTimeSpent
    );

    List<Object> measurementResults = Arrays.asList(
        createExpectedRow(measurementDateTime, "gender", "f", "country", "usa", "pageViews", 100,
                          "timeSpent", 15
        ),
        createExpectedRow(measurementDateTime, "gender", "m", "country", "usa", "pageViews", 200,
                          "timeSpent", 30
        ),
        createExpectedRow(measurementDateTime, "gender", "f", "country", "france", "pageViews", 300,
                          "timeSpent", 45
        ),
        createExpectedRow(measurementDateTime, "gender", "m", "country", "france", "pageViews", 400,
                          "timeSpent", 60
        )
    );

    List<Object> cohortResults = Arrays.asList(
        createExpectedRow(cohortDateTime, "gender", "f", "country", "usa", "pageViews", 1000,
                          "timeSpent", 75
        ),
        createExpectedRow(cohortDateTime, "gender", "m", "country", "usa", "pageViews", 2000,
                          "timeSpent", 90
        ),
        createExpectedRow(cohortDateTime, "gender", "f", "country", "france", "pageViews", 3000,
                          "timeSpent", 105
        ),
        createExpectedRow(cohortDateTime, "gender", "m", "country", "france", "pageViews", 4000,
                          "timeSpent", 120
        )
    );

    // Generate the results that should be obtained after running the mock query

    // metric filter
    LookbackQuery lookbackQuery = LookbackQuery.builder()
                                               .setLookbackOffsets(lookbackOffsets)
                                               .setDatasource(new QueryDataSource(groupbyQuery))
                                               .setPostAggregatorSpecs(lookbackPostAggs)
                                               .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                               .setHavingSpec(new GreaterThanHavingSpec("pageViews", 200L))
                                               .build();

    List<Result<LookbackResultValue>> actualResultList =
        getActualResults(lookbackQuery, measurementResults, cohortResults);

    List<Result<LookbackResultValue>> expectedResultList = Arrays.asList(
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "france", "pageViews", 300, "timeSpent", 45, "totalPageViews", 3300.0,
                  "totalTimeSpent", 150.0, "lookback_P-1D_pageViews", 3000, "lookback_P-1D_timeSpent", 105
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "france", "pageViews", 400, "timeSpent", 60, "totalPageViews", 4400.0,
                  "totalTimeSpent", 180.0, "lookback_P-1D_pageViews", 4000, "lookback_P-1D_timeSpent",
                  120
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);


    // lookback metric filter
    lookbackQuery = LookbackQuery.builder()
                                 .setLookbackOffsets(lookbackOffsets)
                                 .setDatasource(new QueryDataSource(groupbyQuery))
                                 .setPostAggregatorSpecs(lookbackPostAggs)
                                 .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                 .setHavingSpec(new GreaterThanHavingSpec("lookback_P-1D_timeSpent", 105L))
                                 .build();

    actualResultList = getActualResults(lookbackQuery, measurementResults, cohortResults);

    // Expected Result
    expectedResultList = Arrays.asList(getResult(
        measurementDateTime,
        LOOKBACK,
        "gender",
        "m",
        "country",
        "france",
        "pageViews",
        400,
        "timeSpent",
        60,
        "totalPageViews",
        4400.0,
        "totalTimeSpent",
        180.0,
        "lookback_P-1D_pageViews",
        4000,
        "lookback_P-1D_timeSpent",
        120
    ));

    Assert.assertEquals(expectedResultList, actualResultList);

    // postAgg metric filter
    lookbackQuery = LookbackQuery.builder()
                                 .setLookbackOffsets(lookbackOffsets)
                                 .setDatasource(new QueryDataSource(groupbyQuery))
                                 .setPostAggregatorSpecs(lookbackPostAggs)
                                 .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                 .setHavingSpec(new LessThanHavingSpec("totalPageViews", 3000L))
                                 .build();

    actualResultList = getActualResults(lookbackQuery, measurementResults, cohortResults);

    expectedResultList = Arrays.asList(
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "usa", "pageViews", 100, "timeSpent", 15, "totalPageViews", 1100.0,
                  "totalTimeSpent", 90.0, "lookback_P-1D_pageViews", 1000, "lookback_P-1D_timeSpent", 75
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "usa", "pageViews", 200, "timeSpent", 30, "totalPageViews", 2200.0,
                  "totalTimeSpent", 120.0, "lookback_P-1D_pageViews", 2000, "lookback_P-1D_timeSpent", 90
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);
  }

  // @Test
  public void testLookbackQueryOrderBy()
  {

    GroupByQuery groupbyQuery = GroupByQuery.builder().setDataSource("slice1")
                                            .setQuerySegmentSpec(LookbackQueryTestResources.oneDayQuerySegmentSpec)
                                            .setGranularity(LookbackQueryTestResources.dayGran)
                                            .setAggregatorSpecs(Arrays.asList(
                                                LookbackQueryTestResources.pageViews,
                                                LookbackQueryTestResources.timeSpent
                                            ))
                                            .setDimensions(Arrays.asList(
                                                LookbackQueryTestResources.dim1,
                                                LookbackQueryTestResources.dim2
                                            ))
                                            .build();

    List<PostAggregator> lookbackPostAggs = Lists.<PostAggregator>newArrayList(
        LookbackQueryTestResources.totalPageViews,
        LookbackQueryTestResources.totalTimeSpent
    );

    List<Object> measurementResults = Arrays.asList(
        createExpectedRow(measurementDateTime, "gender", "f", "country", "usa", "pageViews", 100,
                          "timeSpent", 15
        ),
        createExpectedRow(measurementDateTime, "gender", "m", "country", "usa", "pageViews", 20,
                          "timeSpent", 30
        ),
        createExpectedRow(measurementDateTime, "gender", "f", "country", "france", "pageViews", 300,
                          "timeSpent", 10
        ),
        createExpectedRow(measurementDateTime, "gender", "m", "country", "france", "pageViews", 80,
                          "timeSpent", 35
        )
    );

    List<Object> cohortResults = Arrays.asList(
        createExpectedRow(cohortDateTime, "gender", "f", "country", "usa", "pageViews", 50, "timeSpent",
                          75
        ),
        createExpectedRow(cohortDateTime, "gender", "m", "country", "usa", "pageViews", 2000,
                          "timeSpent", 90
        ),
        createExpectedRow(cohortDateTime, "gender", "f", "country", "france", "pageViews", 1000,
                          "timeSpent", 105
        ),
        createExpectedRow(cohortDateTime, "gender", "m", "country", "france", "pageViews", 4000,
                          "timeSpent", 120
        )
    );

    // Generate the results that should be obtained after running the mock query

    // order by metric
    LookbackQuery lookbackQuery = LookbackQuery.builder()
                                               .setLookbackOffsets(lookbackOffsets)
                                               .setDatasource(new QueryDataSource(groupbyQuery))
                                               .setPostAggregatorSpecs(lookbackPostAggs)
                                               .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                               .setLimitSpec(new DefaultLimitSpec(OrderByColumnSpec.descending(
                                                   "pageViews"), null))
                                               .build();

    List<Result<LookbackResultValue>> actualResultList =
        getActualResults(lookbackQuery, measurementResults, cohortResults);

    List<Result<LookbackResultValue>> expectedResultList = Arrays.asList(
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "france", "pageViews", 300, "timeSpent", 10, "totalPageViews", 1300.0,
                  "totalTimeSpent", 115.0, "lookback_P-1D_pageViews", 1000, "lookback_P-1D_timeSpent", 105
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "usa", "pageViews", 100, "timeSpent", 15, "totalPageViews", 150.0,
                  "totalTimeSpent", 90.0, "lookback_P-1D_pageViews", 50, "lookback_P-1D_timeSpent", 75
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "france", "pageViews", 80, "timeSpent", 35, "totalPageViews", 4080.0,
                  "totalTimeSpent", 155.0, "lookback_P-1D_pageViews", 4000, "lookback_P-1D_timeSpent", 120
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "usa", "pageViews", 20, "timeSpent", 30, "totalPageViews", 2020.0,
                  "totalTimeSpent", 120.0, "lookback_P-1D_pageViews", 2000, "lookback_P-1D_timeSpent", 90
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);

    // order by dimension
    lookbackQuery = LookbackQuery.builder()
                                 .setLookbackOffsets(lookbackOffsets)
                                 .setDatasource(new QueryDataSource(groupbyQuery))
                                 .setPostAggregatorSpecs(lookbackPostAggs)
                                 .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                 .setLimitSpec(new DefaultLimitSpec(
                                     OrderByColumnSpec.ascending("country", "gender"),
                                     null
                                 ))
                                 .build();

    actualResultList = getActualResults(lookbackQuery, measurementResults, cohortResults);

    expectedResultList = Arrays.asList(
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "france", "pageViews", 300, "timeSpent", 10, "totalPageViews", 1300.0,
                  "totalTimeSpent", 115.0, "lookback_P-1D_pageViews", 1000, "lookback_P-1D_timeSpent", 105
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "france", "pageViews", 80, "timeSpent", 35, "totalPageViews", 4080.0,
                  "totalTimeSpent", 155.0, "lookback_P-1D_pageViews", 4000, "lookback_P-1D_timeSpent", 120
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "usa", "pageViews", 100, "timeSpent", 15, "totalPageViews", 150.0,
                  "totalTimeSpent", 90.0, "lookback_P-1D_pageViews", 50, "lookback_P-1D_timeSpent", 75
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "usa", "pageViews", 20, "timeSpent", 30, "totalPageViews", 2020.0,
                  "totalTimeSpent", 120.0, "lookback_P-1D_pageViews", 2000, "lookback_P-1D_timeSpent", 90
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);

    // order by lookback metric
    lookbackQuery = LookbackQuery.builder()
                                 .setLookbackOffsets(lookbackOffsets)
                                 .setDatasource(new QueryDataSource(groupbyQuery))
                                 .setPostAggregatorSpecs(lookbackPostAggs)
                                 .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                 .setLimitSpec(new DefaultLimitSpec(OrderByColumnSpec.ascending(
                                     "lookback_P-1D_timeSpent"), null))
                                 .build();

    actualResultList = getActualResults(lookbackQuery, measurementResults, cohortResults);

    expectedResultList = Arrays.asList(
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "usa", "pageViews", 100, "timeSpent", 15, "totalPageViews", 150.0,
                  "totalTimeSpent", 90.0, "lookback_P-1D_pageViews", 50, "lookback_P-1D_timeSpent", 75
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "usa", "pageViews", 20, "timeSpent", 30, "totalPageViews", 2020.0,
                  "totalTimeSpent", 120.0, "lookback_P-1D_pageViews", 2000, "lookback_P-1D_timeSpent", 90
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "france", "pageViews", 300, "timeSpent", 10, "totalPageViews", 1300.0,
                  "totalTimeSpent", 115.0, "lookback_P-1D_pageViews", 1000, "lookback_P-1D_timeSpent", 105
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "france", "pageViews", 80, "timeSpent", 35, "totalPageViews", 4080.0,
                  "totalTimeSpent", 155.0, "lookback_P-1D_pageViews", 4000, "lookback_P-1D_timeSpent",
                  120
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);

    // order by postAgg
    lookbackQuery = LookbackQuery.builder()
                                 .setLookbackOffsets(lookbackOffsets)
                                 .setDatasource(new QueryDataSource(groupbyQuery))
                                 .setPostAggregatorSpecs(lookbackPostAggs)
                                 .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                 .setLimitSpec(new DefaultLimitSpec(
                                     OrderByColumnSpec.descending("totalPageViews"),
                                     null
                                 ))
                                 .build();

    actualResultList = getActualResults(lookbackQuery, measurementResults, cohortResults);

    expectedResultList = Arrays.asList(
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "france", "pageViews", 80, "timeSpent", 35, "totalPageViews", 4080.0,
                  "totalTimeSpent", 155.0, "lookback_P-1D_pageViews", 4000, "lookback_P-1D_timeSpent", 120
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "m", "country",
                  "usa", "pageViews", 20, "timeSpent", 30, "totalPageViews", 2020.0,
                  "totalTimeSpent", 120.0, "lookback_P-1D_pageViews", 2000, "lookback_P-1D_timeSpent", 90
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "france", "pageViews", 300, "timeSpent", 10, "totalPageViews", 1300.0,
                  "totalTimeSpent", 115.0, "lookback_P-1D_pageViews", 1000, "lookback_P-1D_timeSpent", 105
        ),
        getResult(measurementDateTime, LOOKBACK, "gender", "f", "country",
                  "usa", "pageViews", 100, "timeSpent", 15, "totalPageViews", 150.0,
                  "totalTimeSpent", 90.0, "lookback_P-1D_pageViews", 50, "lookback_P-1D_timeSpent", 75
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);
  }


  //@Test
  public void testTimeseriesQueryWithMissingCohortData()
  {

    Query<Result<TimeseriesResultValue>> timeSeriesQuery = new TimeseriesQuery(
        LookbackQueryTestResources.tableDataSource,
        LookbackQueryTestResources.twoDaysQuerySegmentSpec,
        false,
        null,
        null,
        null,
        Arrays.asList(LookbackQueryTestResources.pageViews, LookbackQueryTestResources.timeSpent),
        null,
        0,
        ImmutableMap.<String, Object>of("queryId", "2")
    );

    LookbackQuery lookbackQuery = LookbackQuery.builder()
                                               .setDatasource(new QueryDataSource(timeSeriesQuery))
                                               .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                               .setPostAggregatorSpecs(LookbackQueryTestResources.singleLookbackPostAggList)
                                               .setLookbackOffsets(lookbackOffsets).build();

    // measurement results
    List<Object> measurementResults = Arrays.asList(
        getResult(DateTimes.of("2015-10-10"), TIMESERIES, "pageViews", 2, "timeSpent", 3),
        getResult(DateTimes.of("2015-10-11"), TIMESERIES, "pageViews", 2, "timeSpent", 3)

    );

    // cohort results
    List<Object> cohortResults = Collections.singletonList(
        getResult(DateTimes.of("2015-10-10"), TIMESERIES, "pageViews", 4, "timeSpent", 9)

    );

    List<Result<LookbackResultValue>> actualResultList = getActualResults(
        lookbackQuery,
        measurementResults,
        cohortResults
    );

    List<Result<LookbackResultValue>> expectedResultList = Arrays.asList(
        getResult(
            DateTimes.of("2015-10-10"),
            LOOKBACK,
            "timeSpent",
            3,
            "lookback_P-1D_timeSpent",
            null,
            "pageViews",
            2,
            "lookback_P-1D_pageViews",
            null,
            "totalPageViews",
            null
        ),
        getResult(
            DateTimes.of("2015-10-11"),
            LOOKBACK,
            "timeSpent",
            3,
            "lookback_P-1D_timeSpent",
            9,
            "pageViews",
            2,
            "lookback_P-1D_pageViews",
            4,
            "totalPageViews",
            6.0
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);
  }

  //@Test
  public void testGroupByQueryWithMissingCohortDataAndNoMatchingResultRow()
  {
    //This test is for the interval 2015-10-10/2015-10-12
    //There is no data for 2015-10-09 and the available cohort data for 2015-10-10 has a different value of dim1
    //as compared to the measurement data from 2015-10-11. Therefore the join does not return any rows
    Query<Row> groupByQuery = GroupByQuery.builder()
                                          .setDataSource("slice1")
                                          .setQuerySegmentSpec(LookbackQueryTestResources.twoDaysQuerySegmentSpec)
                                          .setGranularity(LookbackQueryTestResources.dayGran)
                                          .setAggregatorSpecs(Arrays.asList(LookbackQueryTestResources.pageViews))
                                          .setDimensions(Arrays.asList(LookbackQueryTestResources.dim1))
                                          .build();

    LookbackQuery lookbackQuery = LookbackQuery.builder()
                                               .setDatasource(new QueryDataSource(groupByQuery))
                                               .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                               .setPostAggregatorSpecs(LookbackQueryTestResources.singleLookbackPostAggList)
                                               .setLookbackOffsets(lookbackOffsets).build();

    // measurement results
    List<Object> measurementResults = Arrays.asList(
        createExpectedRow(DateTimes.of("2015-10-10"), "gender", "m", "pageViews", 2),
        createExpectedRow(DateTimes.of("2015-10-10"), "gender", "f", "pageViews", 3),
        createExpectedRow(DateTimes.of("2015-10-11"), "gender", "f", "pageViews", 2)
    );

    // cohort results
    List<Object> cohortResults = Arrays.asList(
        createExpectedRow(DateTimes.of("2015-10-9"), "gender", "m", "pageViews", 2),
        createExpectedRow(DateTimes.of("2015-10-10"), "gender", "m", "pageViews", 2),
        createExpectedRow(DateTimes.of("2015-10-10"), "gender", "f", "pageViews", 3)
    );

    List<Result<LookbackResultValue>> actualResultList = getActualResults(
        lookbackQuery,
        measurementResults,
        cohortResults
    );

    List<Result<LookbackResultValue>> expectedResultList = Arrays.asList(
        getResult(
            DateTimes.of("2015-10-10"),
            LOOKBACK,
            "gender",
            "m",
            "pageViews",
            2,
            "lookback_P-1D_pageViews",
            2,
            "totalPageViews",
            4.0
        ),
        getResult(
            DateTimes.of("2015-10-10"),
            LOOKBACK,
            "gender",
            "f",
            "pageViews",
            3,
            "lookback_P-1D_pageViews",
            null,
            "totalPageViews",
            null
        ),
        getResult(
            DateTimes.of("2015-10-11"),
            LOOKBACK,
            "gender",
            "f",
            "pageViews",
            2,
            "lookback_P-1D_pageViews",
            3,
            "totalPageViews",
            5.0
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);
  }

  //@Test
  public void testComplexLookbackQueryOverMultipleDays()
  {
    //The inner query is a Timeseries Query over an interval of 2 days. It has aggs and post aggs
    //The outer query has a Postagg referring to the inner query postagg

    CountAggregatorFactory otherPageViews = new CountAggregatorFactory("otherPageViews");
    CountAggregatorFactory additivePageViews = new CountAggregatorFactory("additivePageViews");

    ArithmeticPostAggregator totalPageViews = new ArithmeticPostAggregator(
        "totalPageViews",
        "+",
        Lists.<PostAggregator>newArrayList(
            new FieldAccessPostAggregator("otherPageViews", "otherPageViews"),
            new FieldAccessPostAggregator("additivePageViews", "additivePageViews")
        )
    );

    ArithmeticPostAggregator retainedPageViews = new ArithmeticPostAggregator(
        "retainedPageViews",
        "+",
        Lists.<PostAggregator>newArrayList(
            new FieldAccessPostAggregator("foo_totalPageViews", "foo_totalPageViews"),
            new FieldAccessPostAggregator("bar_totalPageViews", "bar_totalPageViews"),
            new FieldAccessPostAggregator("totalPageViews", "totalPageViews")
        )
    );

    Query<Result<TimeseriesResultValue>> timeSeriesQuery = new TimeseriesQuery(
        LookbackQueryTestResources.tableDataSource,
        LookbackQueryTestResources.twoDaysQuerySegmentSpec,
        false,
        null,
        null,
        null,
        Arrays.asList(LookbackQueryTestResources.timeSpent, otherPageViews, additivePageViews),
        Arrays.asList(totalPageViews),
        0,
        ImmutableMap.<String, Object>of("queryId", "2")
    );

    LookbackQuery lookbackQuery = LookbackQuery.builder()
                                               .setDatasource(new QueryDataSource(timeSeriesQuery))
                                               .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
                                               .setLookbackPrefixes(Arrays.asList("foo_", "bar_"))
                                               .setPostAggregatorSpecs(Arrays.asList(retainedPageViews))
                                               .setLookbackOffsets(Arrays.asList(
                                                   LookbackQueryTestResources.period,
                                                   Period.days(-2)
                                               )).build();

    // measurement results
    List<Object> measurementResults = Arrays.asList(
        getResult(
            DateTimes.of("2015-10-10"),
            TIMESERIES,
            "otherPageViews",
            2,
            "additivePageViews",
            3,
            "timeSpent",
            4,
            "totalPageViews",
            5
        ),
        getResult(
            DateTimes.of("2015-10-11"),
            TIMESERIES,
            "otherPageViews",
            2,
            "additivePageViews",
            3,
            "timeSpent",
            4,
            "totalPageViews",
            5
        )

    );

    // cohort results
    List<Object> cohortResults = Arrays.asList(
        getResult(
            DateTimes.of("2015-10-09"),
            TIMESERIES,
            "otherPageViews",
            4,
            "additivePageViews",
            6,
            "timeSpent",
            4,
            "totalPageViews",
            10
        ),
        getResult(
            DateTimes.of("2015-10-10"),
            TIMESERIES,
            "otherPageViews",
            2,
            "additivePageViews",
            3,
            "timeSpent",
            4,
            "totalPageViews",
            5
        )

    );
    List<Object> cohortResults2 = Arrays.asList(
        getResult(
            DateTimes.of("2015-10-08"),
            TIMESERIES,
            "otherPageViews",
            4,
            "additivePageViews",
            6,
            "timeSpent",
            4,
            "totalPageViews",
            10
        ),
        getResult(
            DateTimes.of("2015-10-09"),
            TIMESERIES,
            "otherPageViews",
            2,
            "additivePageViews",
            3,
            "timeSpent",
            4,
            "totalPageViews",
            5
        )
    );

    List<Result<LookbackResultValue>> actualResultList = getActualResults(
        lookbackQuery,
        measurementResults,
        cohortResults,
        cohortResults2
    );

    List<Result<LookbackResultValue>> expectedResultList = Arrays.asList(
        getResult(
            DateTimes.of("2015-10-10"),
            LOOKBACK,
            "otherPageViews",
            2,
            "additivePageViews",
            3,
            "timeSpent",
            4,
            "totalPageViews",
            5.0,
            "foo_timeSpent",
            4,
            "foo_totalPageViews",
            10.0,
            "foo_additivePageViews",
            6,
            "foo_otherPageViews",
            4,
            "bar_timeSpent",
            4,
            "bar_totalPageViews",
            10.0,
            "bar_additivePageViews",
            6,
            "bar_otherPageViews",
            4,
            "retainedPageViews",
            25.0
        ),
        getResult(
            DateTimes.of("2015-10-11"),
            LOOKBACK,
            "otherPageViews",
            2,
            "additivePageViews",
            3,
            "timeSpent",
            4,
            "totalPageViews",
            5.0,
            "foo_timeSpent",
            4,
            "foo_totalPageViews",
            5.0,
            "foo_additivePageViews",
            3,
            "foo_otherPageViews",
            2,
            "bar_timeSpent",
            4,
            "bar_totalPageViews",
            5.0,
            "bar_additivePageViews",
            3,
            "bar_otherPageViews",
            2,
            "retainedPageViews",
            15.0
        )
    );

    Assert.assertEquals(expectedResultList, actualResultList);
  }

  @SuppressWarnings("unchecked")
  //@Test
  public void testCohortIntervalOfNestedQuery()
  {

    GroupByQuery nestedQuery = GroupByQuery.builder()
                                           .setDataSource("slice1")
                                           .setQuerySegmentSpec(LookbackQueryTestResources.oneDayQuerySegmentSpec)
                                           .setGranularity(LookbackQueryTestResources.dayGran)
                                           .setAggregatorSpecs(Collections.singletonList(LookbackQueryTestResources.pageViews))
                                           .setDimensions(Collections.singletonList(LookbackQueryTestResources.dim1))
                                           .build();

    GroupByQuery groupByQuery = GroupByQuery.builder()
                                            .setDataSource(new QueryDataSource(nestedQuery))
                                            .setQuerySegmentSpec(LookbackQueryTestResources.oneDayQuerySegmentSpec)
                                            .setGranularity(LookbackQueryTestResources.dayGran)
                                            .setAggregatorSpecs(Collections.singletonList(LookbackQueryTestResources.pageViews))
                                            .setDimensions(Collections.singletonList(LookbackQueryTestResources.dim1))
                                            .build();

    LookbackQuery.LookbackQueryBuilder builder = LookbackQuery.builder();

    LookbackQuery query = builder
        .setDatasource(new QueryDataSource(groupByQuery))
        .setContext(ImmutableMap.<String, Object>of("queryId", "2"))
        .setLookbackOffsets(lookbackOffsets).build();

    // Set up Mock Queryrunner
    QueryRunner<Object> mockQueryRunner = mock(QueryRunner.class);
    QuerySegmentWalker mockWalker = strictMock(QuerySegmentWalker.class);
    Capture<Query<Object>> capturedQuery = EasyMock.newCapture();
    String queryId = (String) query.getContext().getOrDefault("queryId", "");

    Sequence<Object> measurementResultSeq = Sequences.simple(Collections.EMPTY_LIST);
    Sequence<Object> cohortResultSeq = Sequences.simple(Collections.EMPTY_LIST);

    // Run the mock query runner
    expect(mockWalker.getQueryRunnerForIntervals(anyObject(Query.class), anyObject(List.class)))
        .andReturn(mockQueryRunner).anyTimes();
    expect(mockQueryRunner.run(
        QueryPlus.wrap(queryIdMatches(queryId + "measurement_query")),
        isA(HashMap.class)
    )).andReturn(measurementResultSeq);

    expect(mockQueryRunner.run(QueryPlus.wrap(and(
        queryIdMatches(queryId
                       + "cohort_query"
                       + LookbackQueryTestResources.period.toString()),
        capture(capturedQuery)
    )), isA(HashMap.class))).andReturn(cohortResultSeq);

    replay(mockQueryRunner, mockWalker);

    QueryRunner<Result<LookbackResultValue>> runner = new LookbackQueryRunner(mockWalker, warehouse);
    Sequence<Result<LookbackResultValue>> actualResult = runner.run(QueryPlus.wrap(query), new HashMap<>());

    List<Result<LookbackResultValue>> results = new ArrayList<>();
    Sequences.toList(actualResult, results);
    Assert.assertTrue(results.isEmpty());

    verify(mockQueryRunner, mockWalker);
    Query<Object> cohortQuery = capturedQuery.getValue();
    Query<Object> nestedCohortQuery = ((QueryDataSource) cohortQuery.getDataSource()).getQuery();

    Interval measurementInterval = LookbackQueryTestResources.oneDayQuerySegmentSpec.getIntervals().get(0);
    Interval expectedCohortInterval =
        new Interval(
            measurementInterval.getStart().withPeriodAdded(LookbackQueryTestResources.period, 1),
            measurementInterval.getEnd().withPeriodAdded(LookbackQueryTestResources.period, 1)
        );

    reset(mockQueryRunner, mockWalker);

    Assert.assertEquals(expectedCohortInterval, cohortQuery.getIntervals().get(0));
    Assert.assertEquals(expectedCohortInterval, nestedCohortQuery.getIntervals().get(0));
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  //@Test
  public void testCohortQueryHasCorrectContext()
  {

    Map<String, Object> contextMap = new HashMap<>();
    contextMap.put("timeout", 60000);
    contextMap.put("queryId", "lookbackQuery");
    contextMap.put("priority", 0);

    Map<String, Object> expectedCohortQueryContextMap = new HashMap<>();
    expectedCohortQueryContextMap.put("timeout", 60000);
    expectedCohortQueryContextMap.put("queryId", "lookbackQuerycohort_queryP-1D");
    expectedCohortQueryContextMap.put("priority", 0);
    expectedCohortQueryContextMap.put("finalize", false);

    // Build lookback query
    LookbackQuery.LookbackQueryBuilder builder = LookbackQuery.builder();

    LookbackQuery query = builder
        .setDatasource(LookbackQueryTestResources.timeSeriesQueryDataSource)
        .setContext(contextMap)
        .setLookbackOffsets(lookbackOffsets).build();

    // Set up Mock Queryrunner
    QueryRunner mockQueryRunner = mock(QueryRunner.class);
    QuerySegmentWalker mockWalker = strictMock(QuerySegmentWalker.class);
    Capture<Query<Object>> capturedQuery = EasyMock.newCapture();

    Sequence<Object> measurementResultSeq = Sequences.simple(Collections.EMPTY_LIST);
    Sequence<Object> cohortResultSeq = Sequences.simple(Collections.EMPTY_LIST);

    expect(mockWalker.getQueryRunnerForIntervals(anyObject(Query.class), anyObject(List.class)))
        .andReturn(mockQueryRunner).anyTimes();
    // Run the mock query runner
    expect(mockQueryRunner.run(QueryPlus.wrap(and(
        queryIdMatches("lookbackQuerycohort_queryP-1D"),
        capture(capturedQuery)
    )), isA(HashMap.class))).andReturn(
        cohortResultSeq);
    expect(mockQueryRunner.run(
        QueryPlus.wrap(queryIdMatches("lookbackQuerymeasurement_query")),
        isA(HashMap.class)
    )).andReturn(measurementResultSeq);
    replay(mockQueryRunner, mockWalker);

    QueryRunner<Result<LookbackResultValue>> runner = new LookbackQueryRunner(mockWalker, warehouse);
    Sequence<Result<LookbackResultValue>> actualResult = runner.run(QueryPlus.wrap(query), new HashMap<>());

    List<Result<LookbackResultValue>> results = new ArrayList<>();
    Sequences.toList(actualResult, results);
    Assert.assertTrue(results.isEmpty());

    verify(mockQueryRunner, mockWalker);
    reset(mockQueryRunner, mockWalker);

    Query<Object> cohortQuery = capturedQuery.getValue();

    Map<String, Object> actualCohortQueryContextMap = cohortQuery.getContext();

    Assert.assertEquals(expectedCohortQueryContextMap, actualCohortQueryContextMap);
  }

  private static Query queryIdMatches(String queryId)
  {
    EasyMock.reportMatcher(new IArgumentMatcher()
    {

      @Override
      public boolean matches(Object actual)
      {
        if (!(actual instanceof Query)) {
          return false;
        }
        Query<?> query = (Query) actual;
        String actualQueryId = (String) query.getContext().get("queryId");

        return queryId.equals(actualQueryId);
      }

      @Override
      public void appendTo(StringBuffer buffer)
      {
        buffer.append("queryIdMatches(");
        buffer.append(queryId);
        buffer.append(")");
      }
    });
    return null;
  }
}
