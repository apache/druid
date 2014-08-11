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

package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.Pair;
import io.druid.granularity.QueryGranularity;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.filter.DimFilter;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.SearchHit;
import io.druid.query.search.search.SearchQuery;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
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
  final QueryGranularity allGran = QueryGranularity.ALL;
  final String dimensionValue = "dimension";
  final String valueValue = "value";
  final String providerDimension = "provider";
  final String qualityDimension = "quality";
  final String placementDimension = "placement";
  final String placementishDimension = "placementish";
  final String indexMetric = "index";
  final CountAggregatorFactory rowsCount = new CountAggregatorFactory("rows");
  final DoubleSumAggregatorFactory indexDoubleSum = new DoubleSumAggregatorFactory("index", "index");
  final HyperUniquesAggregatorFactory uniques = new HyperUniquesAggregatorFactory("uniques", "quality_uniques");
  final ConstantPostAggregator constant = new ConstantPostAggregator("const", 1L, null);
  final FieldAccessPostAggregator rowsPostAgg = new FieldAccessPostAggregator("rows", "rows");
  final FieldAccessPostAggregator indexPostAgg = new FieldAccessPostAggregator("index", "index");
  final ArithmeticPostAggregator addRowsIndexConstant =
      new ArithmeticPostAggregator(
          "addRowsIndexConstant", "+", Lists.newArrayList(constant, rowsPostAgg, indexPostAgg)
      );
  final List<AggregatorFactory> commonAggregators = Arrays.asList(rowsCount, indexDoubleSum, uniques);

  final QuerySegmentSpec fullOnInterval = new MultipleIntervalSegmentSpec(
      Arrays.asList(new Interval("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"))
  );

  private Segment segment;
  private Segment segment2;
  private Segment segment3;

  @Before
  public void setUp() throws Exception
  {
    // (1, 2) cover overlapping segments of the form
    // |------|
    //     |--------|
    QueryableIndex appendedIndex = SchemalessIndex.getAppendedIncrementalIndex(
        Arrays.asList(
            new Pair<String, AggregatorFactory[]>("append.json.1", METRIC_AGGS_NO_UNIQ),
            new Pair<String, AggregatorFactory[]>("append.json.2", METRIC_AGGS)
        ),
        Arrays.asList(
            new Interval("2011-01-12T00:00:00.000Z/2011-01-16T00:00:00.000Z"),
            new Interval("2011-01-14T22:00:00.000Z/2011-01-16T00:00:00.000Z")
        )
    );
    segment = new QueryableIndexSegment(null, appendedIndex);

    // (3, 4) cover overlapping segments of the form
    // |------------|
    //     |-----|
    QueryableIndex append2 = SchemalessIndex.getAppendedIncrementalIndex(
        Arrays.asList(
            new Pair<String, AggregatorFactory[]>("append.json.3", METRIC_AGGS_NO_UNIQ),
            new Pair<String, AggregatorFactory[]>("append.json.4", METRIC_AGGS)
        ),
        Arrays.asList(
            new Interval("2011-01-12T00:00:00.000Z/2011-01-16T00:00:00.000Z"),
            new Interval("2011-01-13T00:00:00.000Z/2011-01-14T00:00:00.000Z")
        )
    );
    segment2 = new QueryableIndexSegment(null, append2);

    // (5, 6, 7) test gaps that can be created in data because of rows being discounted
    // |-------------|
    //   |---|
    //          |---|
    QueryableIndex append3 = SchemalessIndex.getAppendedIncrementalIndex(
        Arrays.asList(
            new Pair<String, AggregatorFactory[]>("append.json.5", METRIC_AGGS),
            new Pair<String, AggregatorFactory[]>("append.json.6", METRIC_AGGS),
            new Pair<String, AggregatorFactory[]>("append.json.7", METRIC_AGGS)
        ),
        Arrays.asList(
            new Interval("2011-01-12T00:00:00.000Z/2011-01-22T00:00:00.000Z"),
            new Interval("2011-01-13T00:00:00.000Z/2011-01-16T00:00:00.000Z"),
            new Interval("2011-01-18T00:00:00.000Z/2011-01-21T00:00:00.000Z")
        )
    );
    segment3 = new QueryableIndexSegment(null, append3);
  }

  @Test
  public void testTimeBoundary()
  {
    List<Result<TimeBoundaryResultValue>> expectedResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-15T02:00:00.000Z")
                )
            )
        )
    );

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource(dataSource)
                                    .build();
    QueryRunner runner = TestQueryRunners.makeTimeBoundaryQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTimeBoundary2()
  {
    List<Result<TimeBoundaryResultValue>> expectedResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-15T00:00:00.000Z")
                )
            )
        )
    );

    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource(dataSource)
                                    .build();
    QueryRunner runner = TestQueryRunners.makeTimeBoundaryQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTimeSeries()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTimeSeries2()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testFilteredTimeSeries()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testFilteredTimeSeries2()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNSeries()
  {
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "spot")
                                .put("rows", 3L)
                                .put("index", 300.0D)
                                .put("addRowsIndexConstant", 304.0D)
                                .put("uniques", 0.0D)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    new HashMap<String, Object>()
                    {{
                        put("provider", null);
                        put("rows", 3L);
                        put("index", 200.0D);
                        put("addRowsIndexConstant", 204.0D);
                        put("uniques", 0.0D);
                        put("maxIndex", 100.0);
                        put("minIndex", 0.0);
                      }},
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "total_market")
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
    QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTopNSeries2()
  {
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "total_market")
                                .put("rows", 3L)
                                .put("index", 300.0D)
                                .put("addRowsIndexConstant", 304.0D)
                                .put("uniques", 0.0D)
                                .put("maxIndex", 100.0D)
                                .put("minIndex", 100.0D)
                                .build(),
                    new HashMap<String, Object>()
                    {{
                        put("provider", null);
                        put("rows", 3L);
                        put("index", 100.0D);
                        put("addRowsIndexConstant", 104.0D);
                        put("uniques", 0.0D);
                        put("maxIndex", 100.0);
                        put("minIndex", 0.0);
                      }},
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "spot")
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
    QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testFilteredTopNSeries()
  {
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("provider", "spot")
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
    QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testFilteredTopNSeries2()
  {
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Lists.<Map<String, Object>>newArrayList()
            )
        )
    );

    TopNQuery query = makeFilteredTopNQuery();
    QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testSearch()
  {
    List<Result<SearchResultValue>> expectedResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive"),
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(providerDimension, "total_market")
                )
            )
        )
    );

    SearchQuery query = makeSearchQuery();
    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testSearchWithOverlap()
  {
    List<Result<SearchResultValue>> expectedResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(providerDimension, "total_market")
                )
            )
        )
    );

    SearchQuery query = makeSearchQuery();
    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testFilteredSearch()
  {
    List<Result<SearchResultValue>> expectedResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(providerDimension, "total_market")
                )
            )
        )
    );

    SearchQuery query = makeFilteredSearchQuery();
    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testFilteredSearch2()
  {
    List<Result<SearchResultValue>> expectedResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(providerDimension, "total_market")
                )
            )
        )
    );

    SearchQuery query = makeFilteredSearchQuery();
    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment2);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testRowFiltering()
  {
    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
                                  .granularity(allGran)
                                  .intervals(fullOnInterval)
                                  .filters(providerDimension, "breakstuff")
                                  .aggregators(
                                      Lists.<AggregatorFactory>newArrayList(
                                          Iterables.concat(
                                              commonAggregators,
                                              Lists.newArrayList(
                                                  new MaxAggregatorFactory("maxIndex", "index"),
                                                  new MinAggregatorFactory("minIndex", "index")
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
                                  .build();
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment3);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  private TimeseriesQuery makeTimeseriesQuery()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(dataSource)
                 .granularity(allGran)
                 .intervals(fullOnInterval)
                 .aggregators(
                     Lists.<AggregatorFactory>newArrayList(
                         Iterables.concat(
                             commonAggregators,
                             Lists.newArrayList(
                                 new MaxAggregatorFactory("maxIndex", "index"),
                                 new MinAggregatorFactory("minIndex", "index")
                             )
                         )
                     )
                 )
                 .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
                 .build();
  }

  private TimeseriesQuery makeFilteredTimeseriesQuery()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(dataSource)
                 .granularity(allGran)
                 .intervals(fullOnInterval)
                 .filters(
                     Druids.newOrDimFilterBuilder()
                           .fields(
                               Arrays.<DimFilter>asList(
                                   Druids.newSelectorDimFilterBuilder()
                                         .dimension(providerDimension)
                                         .value("spot")
                                         .build(),
                                   Druids.newSelectorDimFilterBuilder()
                                         .dimension(providerDimension)
                                         .value("total_market")
                                         .build()
                               )
                           ).build()
                 )
                 .aggregators(
                     Lists.<AggregatorFactory>newArrayList(
                         Iterables.concat(
                             commonAggregators,
                             Lists.newArrayList(
                                 new MaxAggregatorFactory("maxIndex", "index"),
                                 new MinAggregatorFactory("minIndex", "index")
                             )
                         )
                     )
                 )
                 .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
                 .build();
  }

  private TopNQuery makeTopNQuery()
  {
    return new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(3)
        .intervals(fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();
  }

  private TopNQuery makeFilteredTopNQuery()
  {
    return new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(providerDimension)
        .metric(indexMetric)
        .threshold(3)
        .filters(
            Druids.newAndDimFilterBuilder()
                  .fields(
                      Arrays.<DimFilter>asList(
                          Druids.newSelectorDimFilterBuilder()
                                .dimension(providerDimension)
                                .value("spot")
                                .build(),
                          Druids.newSelectorDimFilterBuilder()
                                .dimension(placementDimension)
                                .value("preferred")
                                .build()
                      )
                  ).build()
        )
        .intervals(fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();
  }

  private SearchQuery makeSearchQuery()
  {
    return Druids.newSearchQueryBuilder()
                 .dataSource(dataSource)
                 .granularity(allGran)
                 .intervals(fullOnInterval)
                 .query("a")
                 .build();
  }

  private SearchQuery makeFilteredSearchQuery()
  {
    return Druids.newSearchQueryBuilder()
                 .dataSource(dataSource)
                 .filters(
                     Druids.newNotDimFilterBuilder()
                           .field(
                               Druids.newSelectorDimFilterBuilder()
                                     .dimension(providerDimension)
                                     .value("spot")
                                     .build()
                           ).build()
                 )
                 .granularity(allGran)
                 .intervals(fullOnInterval)
                 .query("a")
                 .build();
  }
}
