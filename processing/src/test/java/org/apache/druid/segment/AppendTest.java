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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TestQueryRunners;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.search.SearchHit;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryResultValue;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@Ignore
public class AppendTest
{
  private static final AggregatorFactory[] METRIC_AGGS = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory("index", "index"),
      new CountAggregatorFactory("count"),
      new HyperUniquesAggregatorFactory("quality_uniques", "quality")
  };
  private static final AggregatorFactory[] METRIC_AGGS_NO_UNIQ = new AggregatorFactory[]{
      new DoubleSumAggregatorFactory("index", "index"),
      new CountAggregatorFactory("count")
  };

  final String dataSource = "testing";
  final Granularity ALL_GRAN = Granularities.ALL;
  final String marketDimension = "market";
  final String qualityDimension = "quality";
  final String placementDimension = "placement";
  final String placementishDimension = "placementish";
  final String indexMetric = "index";
  final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  final DoubleSumAggregatorFactory indexDoubleSum = new DoubleSumAggregatorFactory("index", "index");
  final HyperUniquesAggregatorFactory uniques = new HyperUniquesAggregatorFactory("uniques", "quality_uniques");
  final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L);
  final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
  final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
  final ArithmeticPostAggregator addRowsIndexConstant =
      new ArithmeticPostAggregator(
          "addRowsIndexConstant", "+", Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
      );
  final List<AggregatorFactory> commonAggregators = Arrays.asList(rowsCount, indexDoubleSum, uniques);

  final QuerySegmentSpec fullOnInterval = new MultipleIntervalSegmentSpec(
      Collections.singletonList(Intervals.of("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"))
  );

  private Segment segment;
  private Segment segment2;
  private Segment segment3;

  @Before
  public void setUp()
  {
    SchemalessIndexTest schemalessIndexTest = new SchemalessIndexTest(OffHeapMemorySegmentWriteOutMediumFactory.instance());
    // (1, 2) cover overlapping segments of the form
    // |------|
    //     |--------|
    QueryableIndex appendedIndex = schemalessIndexTest.getAppendedIncrementalIndex(
        Arrays.asList(
            new Pair<String, AggregatorFactory[]>("append.json.1", METRIC_AGGS_NO_UNIQ),
            new Pair<String, AggregatorFactory[]>("append.json.2", METRIC_AGGS)
        ),
        Arrays.asList(
            Intervals.of("2011-01-12T00:00:00.000Z/2011-01-16T00:00:00.000Z"),
            Intervals.of("2011-01-14T22:00:00.000Z/2011-01-16T00:00:00.000Z")
        )
    );
    segment = new QueryableIndexSegment(appendedIndex, null);

    // (3, 4) cover overlapping segments of the form
    // |------------|
    //     |-----|
    QueryableIndex append2 = schemalessIndexTest.getAppendedIncrementalIndex(
        Arrays.asList(new Pair<>("append.json.3", METRIC_AGGS_NO_UNIQ), new Pair<>("append.json.4", METRIC_AGGS)),
        Arrays.asList(
            Intervals.of("2011-01-12T00:00:00.000Z/2011-01-16T00:00:00.000Z"),
            Intervals.of("2011-01-13T00:00:00.000Z/2011-01-14T00:00:00.000Z")
        )
    );
    segment2 = new QueryableIndexSegment(append2, null);

    // (5, 6, 7) test gaps that can be created in data because of rows being discounted
    // |-------------|
    //   |---|
    //          |---|
    QueryableIndex append3 = schemalessIndexTest.getAppendedIncrementalIndex(
        Arrays.asList(
            new Pair<>("append.json.5", METRIC_AGGS),
            new Pair<>("append.json.6", METRIC_AGGS),
            new Pair<>("append.json.7", METRIC_AGGS)
        ),
        Arrays.asList(
            Intervals.of("2011-01-12T00:00:00.000Z/2011-01-22T00:00:00.000Z"),
            Intervals.of("2011-01-13T00:00:00.000Z/2011-01-16T00:00:00.000Z"),
            Intervals.of("2011-01-18T00:00:00.000Z/2011-01-21T00:00:00.000Z")
        )
    );
    segment3 = new QueryableIndexSegment(append3, null);
  }

  @Test
  public void testTimeBoundary()
  {
    List<Result<TimeBoundaryResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-15T02:00:00.000Z")
                )
            )
        )
    );

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource(dataSource)
                                    .build();
    QueryRunner runner = TestQueryRunners.makeTimeBoundaryQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testTimeBoundary2()
  {
    List<Result<TimeBoundaryResultValue>> expectedResults = Collections.singletonList(
        new Result<TimeBoundaryResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-15T00:00:00.000Z")
                )
            )
        )
    );

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource(dataSource)
                                    .build();
    QueryRunner runner = TestQueryRunners.makeTimeBoundaryQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testTimeSeries()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<TimeseriesResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 8L)
                    .put("index", 700.0D)
                    .put("addRowsIndexConstant", 709.0D)
                    .put("uniques", 1.0002442201269182D)
                    .put("maxIndex", 100.0D)
                    .put("minIndex", 0.0D)
                    .build()
            )
        )
    );

    TimeseriesQuery query = makeTimeseriesQuery();
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testTimeSeries2()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<TimeseriesResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 7L)
                    .put("index", 500.0D)
                    .put("addRowsIndexConstant", 508.0D)
                    .put("uniques", 0.0D)
                    .put("maxIndex", 100.0D)
                    .put("minIndex", 0.0D)
                    .build()
            )
        )
    );

    TimeseriesQuery query = makeTimeseriesQuery();
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testFilteredTimeSeries()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<TimeseriesResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 5L)
                    .put("index", 500.0D)
                    .put("addRowsIndexConstant", 506.0D)
                    .put("uniques", 1.0002442201269182D)
                    .put("maxIndex", 100.0D)
                    .put("minIndex", 100.0D)
                    .build()
            )
        )
    );

    TimeseriesQuery query = makeFilteredTimeseriesQuery();
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testFilteredTimeSeries2()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<TimeseriesResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 4L)
                    .put("index", 400.0D)
                    .put("addRowsIndexConstant", 405.0D)
                    .put("uniques", 0.0D)
                    .put("maxIndex", 100.0D)
                    .put("minIndex", 100.0D)
                    .build()
            )
        )
    );

    TimeseriesQuery query = makeFilteredTimeseriesQuery();
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testTopNSeries()
  {
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("rows", 3L)
                        .put("index", 300.0D)
                        .put("addRowsIndexConstant", 304.0D)
                        .put("uniques", 0.0D)
                        .put("maxIndex", 100.0)
                        .put("minIndex", 100.0)
                        .build(),
                    QueryRunnerTestHelper.orderedMap(
                        "market", null,
                        "rows", 3L,
                        "index", 200.0D,
                        "addRowsIndexConstant", 204.0D,
                        "uniques", 0.0D,
                        "maxIndex", 100.0,
                        "minIndex", 0.0
                    ),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("rows", 2L)
                        .put("index", 200.0D)
                        .put("addRowsIndexConstant", 203.0D)
                        .put("uniques", 1.0002442201269182D)
                        .put("maxIndex", 100.0D)
                        .put("minIndex", 100.0D)
                        .build()
                )
            )
        )
    );

    TopNQuery query = makeTopNQuery();
    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment, pool);
      TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
    }
  }

  @Test
  public void testTopNSeries2()
  {
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("rows", 3L)
                        .put("index", 300.0D)
                        .put("addRowsIndexConstant", 304.0D)
                        .put("uniques", 0.0D)
                        .put("maxIndex", 100.0D)
                        .put("minIndex", 100.0D)
                        .build(),
                    QueryRunnerTestHelper.orderedMap(
                        "market", null,
                        "rows", 3L,
                        "index", 100.0D,
                        "addRowsIndexConstant", 104.0D,
                        "uniques", 0.0D,
                        "maxIndex", 100.0,
                        "minIndex", 0.0
                    ),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("rows", 1L)
                        .put("index", 100.0D)
                        .put("addRowsIndexConstant", 102.0D)
                        .put("uniques", 0.0D)
                        .put("maxIndex", 100.0)
                        .put("minIndex", 100.0)
                        .build()
                )
            )
        )
    );

    TopNQuery query = makeTopNQuery();
    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment2, pool);
      TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
    }
  }

  @Test
  public void testFilteredTopNSeries()
  {
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("rows", 1L)
                        .put("index", 100.0D)
                        .put("addRowsIndexConstant", 102.0D)
                        .put("uniques", 0.0D)
                        .put("maxIndex", 100.0)
                        .put("minIndex", 100.0)
                        .build()
                )
            )
        )
    );

    TopNQuery query = makeFilteredTopNQuery();
    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment, pool);
      TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
    }
  }

  @Test
  public void testFilteredTopNSeries2()
  {
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                new ArrayList<Map<String, Object>>()
            )
        )
    );

    TopNQuery query = makeFilteredTopNQuery();
    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment2, pool);
      TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
    }
  }

  @Test
  public void testSearch()
  {
    List<Result<SearchResultValue>> expectedResults = Collections.singletonList(
        new Result<SearchResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive"),
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(marketDimension, "total_market")
                )
            )
        )
    );

    SearchQuery query = makeSearchQuery();
    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testSearchWithOverlap()
  {
    List<Result<SearchResultValue>> expectedResults = Collections.singletonList(
        new Result<SearchResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(marketDimension, "total_market")
                )
            )
        )
    );

    SearchQuery query = makeSearchQuery();
    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testFilteredSearch()
  {
    List<Result<SearchResultValue>> expectedResults = Collections.singletonList(
        new Result<SearchResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.asList(
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(marketDimension, "total_market")
                )
            )
        )
    );

    SearchQuery query = makeFilteredSearchQuery();
    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testFilteredSearch2()
  {
    List<Result<SearchResultValue>> expectedResults = Collections.singletonList(
        new Result<SearchResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(marketDimension, "total_market")
                )
            )
        )
    );

    SearchQuery query = makeFilteredSearchQuery();
    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testRowFiltering()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result<TimeseriesResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", 5L)
                    .put("index", 500.0D)
                    .put("addRowsIndexConstant", 506.0D)
                    .put("uniques", 0.0D)
                    .put("maxIndex", 100.0D)
                    .put("minIndex", 100.0D)
                    .build()
            )
        )
    );

    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(ALL_GRAN)
                                  .intervals(fullOnInterval)
                                  .filters(marketDimension, "breakstuff")
                                  .aggregators(
                                      Lists.newArrayList(
                                          Iterables.concat(
                                              commonAggregators,
                                              Lists.newArrayList(
                                                  new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                                  new DoubleMinAggregatorFactory("minIndex", "index")
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(addRowsIndexConstant)
                                  .build();
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment3);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  private TimeseriesQuery makeTimeseriesQuery()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(dataSource)
                 .granularity(ALL_GRAN)
                 .intervals(fullOnInterval)
                 .aggregators(
                     Lists.newArrayList(
                         Iterables.concat(
                             commonAggregators,
                             Lists.newArrayList(
                                 new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                 new DoubleMinAggregatorFactory("minIndex", "index")
                             )
                         )
                     )
                 )
                 .postAggregators(addRowsIndexConstant)
                 .build();
  }

  private TimeseriesQuery makeFilteredTimeseriesQuery()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(dataSource)
                 .granularity(ALL_GRAN)
                 .intervals(fullOnInterval)
                 .filters(
                     new OrDimFilter(
                         new SelectorDimFilter(marketDimension, "spot", null),
                         new SelectorDimFilter(marketDimension, "total_market", null)
                     )
                 )
                 .aggregators(
                     Lists.newArrayList(
                         Iterables.concat(
                             commonAggregators,
                             Lists.newArrayList(
                                 new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                 new DoubleMinAggregatorFactory("minIndex", "index")
                             )
                         )
                     )
                 )
                 .postAggregators(addRowsIndexConstant)
                 .build();
  }

  private TopNQuery makeTopNQuery()
  {
    return new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(ALL_GRAN)
        .dimension(marketDimension)
        .metric(indexMetric)
        .threshold(3)
        .intervals(fullOnInterval)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(addRowsIndexConstant)
        .build();
  }

  private TopNQuery makeFilteredTopNQuery()
  {
    return new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(ALL_GRAN)
        .dimension(marketDimension)
        .metric(indexMetric)
        .threshold(3)
        .filters(
            new AndDimFilter(
                new SelectorDimFilter(marketDimension, "spot", null),
                new SelectorDimFilter(placementDimension, "preferred", null)
            )
        )
        .intervals(fullOnInterval)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(addRowsIndexConstant)
        .build();
  }

  private SearchQuery makeSearchQuery()
  {
    return Druids.newSearchQueryBuilder()
                 .dataSource(dataSource)
                 .granularity(ALL_GRAN)
                 .intervals(fullOnInterval)
                 .query("a")
                 .build();
  }

  private SearchQuery makeFilteredSearchQuery()
  {
    return Druids.newSearchQueryBuilder()
                 .dataSource(dataSource)
                 .filters(new NotDimFilter(new SelectorDimFilter(marketDimension, "spot", null)))
                 .granularity(ALL_GRAN)
                 .intervals(fullOnInterval)
                 .query("a")
                 .build();
  }
}
