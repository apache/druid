/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.Druids;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.search.SearchHit;
import io.druid.query.search.SearchQuery;
import io.druid.query.search.SearchResultValue;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeboundary.TimeBoundaryQuery;
import io.druid.query.timeboundary.TimeBoundaryResultValue;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import io.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class SchemalessTestFullTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[] {TmpFileSegmentWriteOutMediumFactory.instance()},
        new Object[] {OffHeapMemorySegmentWriteOutMediumFactory.instance()}
    );
  }

  final double UNIQUES_2 = 2.000977198748901d;
  final double UNIQUES_1 = 1.0002442201269182d;

  final SchemalessIndexTest schemalessIndexTest;
  final String dataSource = "testing";
  final Granularity allGran = Granularities.ALL;
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
      Arrays.asList(Intervals.of("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z"))
  );

  public SchemalessTestFullTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    schemalessIndexTest = new SchemalessIndexTest(segmentWriteOutMediumFactory);
  }

  @Test
  public void testCompleteIntersectingSchemas()
  {
    List<Result<TimeseriesResultValue>> expectedTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 2L)
                            .put("index", 200.0D)
                            .put("addRowsIndexConstant", 203.0D)
                            .put("uniques", UNIQUES_2)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("index", 100.0D)
                            .put("addRowsIndexConstant", 102.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "total_market")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0D)
                                .put("minIndex", 100.0D)
                                .build()
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive"),
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(marketDimension, "total_market")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-13T00:00:00.000Z")
                )
            )
        )
    );

    testAll(
        1,
        5,
        expectedTimeSeriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults
    );
  }

  @Test
  public void testEmptyStrings()
  {
    List<Result<TimeseriesResultValue>> expectedTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 2L)
                            .put("index", 200.0D)
                            .put("addRowsIndexConstant", 203.0D)
                            .put("uniques", 0.0D)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("index", 100.0D)
                            .put("addRowsIndexConstant", 102.0D)
                            .put("uniques", 0.0D)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "")
                                .put("rows", 2L)
                                .put("index", 200.0D)
                                .put("addRowsIndexConstant", 203.0D)
                                .put("uniques", 0.0)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", 0.0)
                                .put("maxIndex", 100.0D)
                                .put("minIndex", 100.0D)
                                .build()
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", 0.0)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", 0.0)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    testAll(
        9,
        10,
        expectedTimeSeriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults
    );
  }


  @Test
  public void testNonIntersectingSchemas()
  {
    List<Result<TimeseriesResultValue>> expectedTimeseriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 2L)
                            .put("index", 100.0D)
                            .put("addRowsIndexConstant", 103.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 0.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("index", 100.0D)
                            .put("addRowsIndexConstant", 102.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    QueryRunnerTestHelper.orderedMap(
                        "market", null,
                        "rows", 1L,
                        "index", 0.0D,
                        "addRowsIndexConstant", 2.0D,
                        "uniques", 0.0D,
                        "maxIndex", 0.0,
                        "minIndex", 0.0
                    )
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    testAll(
        2,
        3,
        expectedTimeseriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults
    );
  }

  @Test
  public void testPartialIntersectingSchemas()
  {
    List<Result<TimeseriesResultValue>> expectedTimeseriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 2L)
                            .put("index", 200.0D)
                            .put("addRowsIndexConstant", 203.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("index", 100.0D)
                            .put("addRowsIndexConstant", 102.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "total_market")
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

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(qualityDimension, "automotive"),
                    new SearchHit(marketDimension, "total_market")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-13T00:00:00.000Z")
                )
            )
        )
    );

    testAll(
        2,
        4,
        expectedTimeseriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults
    );
  }

  @Test
  public void testSupersetSchemas()
  {
    List<Result<TimeseriesResultValue>> expectedTimeseriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 2L)
                            .put("index", 200.0D)
                            .put("addRowsIndexConstant", 203.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = expectedTimeseriesResults;

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 2L)
                                .put("index", 200.0D)
                                .put("addRowsIndexConstant", 203.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = expectedTopNResults;

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = expectedSearchResults;

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    testAll(
        1,
        2,
        expectedTimeseriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults
    );
  }

  @Test
  public void testValueAndEmptySchemas()
  {
    List<Result<TimeseriesResultValue>> expectedTimeseriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 2L)
                            .put("index", 100.0D)
                            .put("addRowsIndexConstant", 103.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 0.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("index", 100.0D)
                            .put("addRowsIndexConstant", 102.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    QueryRunnerTestHelper.orderedMap(
                        "market", null,
                        "rows", 1L,
                        "index", 0.0D,
                        "addRowsIndexConstant", 2.0D,
                        "uniques", 0.0D,
                        "maxIndex", 0.0,
                        "minIndex", 0.0
                    )
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = expectedSearchResults;

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    testAll(
        0,
        1,
        expectedTimeseriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults
    );
  }

  @Test
  public void testEmptySchemas()
  {
    List<Result<TimeseriesResultValue>> expectedTimeseriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("index", 0.0D)
                            .put("addRowsIndexConstant", 2.0D)
                            .put("uniques", 0.0D)
                            .put("maxIndex", 0.0D)
                            .put("minIndex", 0.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 0L)
                            .put("index", 0.0D)
                            .put("addRowsIndexConstant", 1.0D)
                            .put("uniques", 0.0D)
                            .put("maxIndex", Double.NEGATIVE_INFINITY)
                            .put("minIndex", Double.POSITIVE_INFINITY)
                            .build()
            )
        )
    );

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    QueryRunnerTestHelper.orderedMap(
                        "market", null,
                        "rows", 1L,
                        "index", 0.0D,
                        "addRowsIndexConstant", 2.0D,
                        "uniques", 0.0D,
                        "maxIndex", 0.0,
                        "minIndex", 0.0
                    )
                )
            )
        )
    );
    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<SearchHit>emptyList()
            )
        )
    );

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Collections.<SearchHit>emptyList()
            )
        )
    );
    List<Result<SearchResultValue>> expectedFilteredSearchResults = expectedSearchResults;

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    runTests(
        new QueryableIndexSegment(
            null, schemalessIndexTest.getMergedIncrementalIndex(0, 0)
        ),
        expectedTimeseriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults,
        "Failed: MII[0, 0]"
    );
  }

  @Test
  public void testExactSameSchemas()
  {
    List<Result<TimeseriesResultValue>> expectedTimeseriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("index", 200.0D)
                            .put("addRowsIndexConstant", 202.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 200.0D)
                            .put("minIndex", 200.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = expectedTimeseriesResults;

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 1L)
                                .put("index", 200.0D)
                                .put("addRowsIndexConstant", 202.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 200.0)
                                .put("minIndex", 200.0)
                                .build()
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = expectedTopNResults;

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = expectedSearchResults;

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    runTests(
        new QueryableIndexSegment(
            null, schemalessIndexTest.getMergedIncrementalIndex(1, 1)
        ),
        expectedTimeseriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults,
        "Failed: MII[1, 1]"
    );
  }

  @Test
  public void testMultiDimensionalValues()
  {
    List<Result<TimeseriesResultValue>> expectedTimeseriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 3L)
                            .put("index", 300.0D)
                            .put("addRowsIndexConstant", 304.0D)
                            .put("uniques", 0.0D)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 100.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 1L)
                            .put("index", 100.0D)
                            .put("addRowsIndexConstant", 102.0D)
                            .put("uniques", 0.0D)
                            .put("maxIndex", 100.0)
                            .put("minIndex", 100.0)
                            .build()
            )
        )
    );

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    QueryRunnerTestHelper.orderedMap(
                        "market", null,
                        "rows", 2L,
                        "index", 200.0D,
                        "addRowsIndexConstant", 203.0D,
                        "uniques", 0.0D,
                        "maxIndex", 100.0,
                        "minIndex", 100.0
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

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
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


    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementDimension, "mezzanine")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList()
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    runTests(
        new QueryableIndexSegment(null, schemalessIndexTest.getMergedIncrementalIndex(new int[]{6, 7, 8})),
        expectedTimeseriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults,
        "Failed: MII[6, 7]"
    );
  }

  @Test
  public void testDifferentMetrics()
  {
    List<Result<TimeseriesResultValue>> expectedTimeseriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 10L)
                            .put("index", 900.0D)
                            .put("addRowsIndexConstant", 911.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 0.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 4L)
                            .put("index", 400.0D)
                            .put("addRowsIndexConstant", 405.0D)
                            .put("uniques", 0.0D)
                            .put("maxIndex", 100.0)
                            .put("minIndex", 100.0)
                            .build()
            )
        )
    );

    /* Uncomment when Druid support for nulls/empty strings is actually consistent
    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "")
                                .put("rows", 6L)
                                .put("index", 400.0D)
                                .put("addRowsIndexConstant", 407.0D)
                                .put("uniques", 0.0)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 0.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 4L)
                                .put("index", 400.0D)
                                .put("addRowsIndexConstant", 405.0D)
                                .put("uniques", 0.0D)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "total_market")
                                .put("rows", 2L)
                                .put("index", 200.0D)
                                .put("addRowsIndexConstant", 203.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );
    */
    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 4L)
                                .put("index", 400.0D)
                                .put("addRowsIndexConstant", 405.0D)
                                .put("uniques", 0.0D)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "")
                                .put("rows", 3L)
                                .put("index", 200.0D)
                                .put("addRowsIndexConstant", 204.0D)
                                .put("uniques", 0.0)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 0.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "total_market")
                                .put("rows", 2L)
                                .put("index", 200.0D)
                                .put("addRowsIndexConstant", 203.0D)
                                .put("uniques", UNIQUES_1)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 4L)
                                .put("index", 400.0D)
                                .put("addRowsIndexConstant", 405.0D)
                                .put("uniques", 0.0D)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "")
                                .put("rows", 1L)
                                .put("index", 100.0D)
                                .put("addRowsIndexConstant", 102.0D)
                                .put("uniques", 0.0)
                                .put("maxIndex", 100.0)
                                .put("minIndex", 100.0)
                                .build()
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive"),
                    new SearchHit(placementDimension, "mezzanine"),
                    new SearchHit(marketDimension, "total_market")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    DateTimes.of("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    DateTimes.of("2011-01-13T00:00:00.000Z")
                )
            )
        )
    );

    runTests(
        new QueryableIndexSegment(null, schemalessIndexTest.getMergedIncrementalIndexDiffMetrics()),
        expectedTimeseriesResults,
        expectedFilteredTimeSeriesResults,
        expectedTopNResults,
        expectedFilteredTopNResults,
        expectedSearchResults,
        expectedFilteredSearchResults,
        expectedTimeBoundaryResults,
        "Failed: MIIDM"
    );
  }

  @SuppressWarnings("ArgumentParameterSwap")
  private List<Pair<QueryableIndex, String>> getIndexes(int index1, int index2)
  {
    return Arrays.asList(
        new Pair<>(
            SchemalessIndexTest.getIncrementalIndex(index1, index2),
            StringUtils.format("Failed: II[%,d, %,d]", index1, index2)
        ),
        new Pair<>(
            SchemalessIndexTest.getIncrementalIndex(index2, index1),
            StringUtils.format("Failed: II[%,d, %,d]", index2, index1)
        ),
        new Pair<>(
            schemalessIndexTest.getMergedIncrementalIndex(index1, index2),
            StringUtils.format("Failed: MII[%,d, %,d]", index1, index2)
        ),
        new Pair<>(
            schemalessIndexTest.getMergedIncrementalIndex(index2, index1),
            StringUtils.format("Failed: MII[%,d, %,d]", index2, index1)
        )
    );
  }

  private void testAll(
      int index1,
      int index2,
      List<Result<TimeseriesResultValue>> expectedTimeseriesResults,
      List<Result<TimeseriesResultValue>> expectedFilteredTimeseriesResults,
      List<Result<TopNResultValue>> expectedTopNResults,
      List<Result<TopNResultValue>> expectedFilteredTopNResults,
      List<Result<SearchResultValue>> expectedSearchResults,
      List<Result<SearchResultValue>> expectedFilteredSearchResults,
      List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults
  )
  {
    for (Pair<QueryableIndex, String> entry : getIndexes(index1, index2)) {
      runTests(
          new QueryableIndexSegment(null, entry.lhs),
          expectedTimeseriesResults,
          expectedFilteredTimeseriesResults,
          expectedTopNResults,
          expectedFilteredTopNResults,
          expectedSearchResults,
          expectedFilteredSearchResults,
          expectedTimeBoundaryResults,
          entry.rhs
      );
    }
  }

  private void runTests(
      Segment adapter,
      List<Result<TimeseriesResultValue>> expectedTimeseriesResults,
      List<Result<TimeseriesResultValue>> expectedFilteredTimeseriesResults,
      @SuppressWarnings("unused") // see below
      List<Result<TopNResultValue>> expectedTopNResults,
      @SuppressWarnings("unused") // see below
      List<Result<TopNResultValue>> expectedFilteredTopNResults,
      List<Result<SearchResultValue>> expectedSearchResults,
      List<Result<SearchResultValue>> expectedFilteredSearchResults,
      List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults,
      String failMsg
  )
  {
    testFullOnTimeseries(TestQueryRunners.makeTimeSeriesQueryRunner(adapter), expectedTimeseriesResults, failMsg);
    testFilteredTimeseries(
        TestQueryRunners.makeTimeSeriesQueryRunner(adapter),
        expectedFilteredTimeseriesResults,
        failMsg
    );
    /*
    TODO: Handling of null values is inconsistent right now, need to make it all consistent and re-enable test
    TODO: Complain to Eric when you see this.  It shouldn't be like this...
    testFullOnTopN(TestQueryRunners.makeTopNQueryRunner(adapter), expectedTopNResults, failMsg);
    testFilteredTopN(TestQueryRunners.makeTopNQueryRunner(adapter), expectedFilteredTopNResults, failMsg);*/
    testFullOnSearch(TestQueryRunners.makeSearchQueryRunner(adapter), expectedSearchResults, failMsg);
    testFilteredSearch(TestQueryRunners.makeSearchQueryRunner(adapter), expectedFilteredSearchResults, failMsg);
    testTimeBoundary(TestQueryRunners.makeTimeBoundaryQueryRunner(adapter), expectedTimeBoundaryResults, failMsg);
  }

  private void testFullOnTimeseries(
      QueryRunner runner,
      List<Result<TimeseriesResultValue>> expectedResults,
      String failMsg
  )
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(allGran)
                                  .intervals(fullOnInterval)
                                  .aggregators(
                                      Lists.<AggregatorFactory>newArrayList(
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

    failMsg += " timeseries ";
    HashMap<String, Object> context = new HashMap<>();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }

  private void testFilteredTimeseries(
      QueryRunner runner,
      List<Result<TimeseriesResultValue>> expectedResults,
      String failMsg
  )
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource(dataSource)
                                  .granularity(allGran)
                                  .intervals(fullOnInterval)
                                  .filters(marketDimension, "spot")
                                  .aggregators(
                                      Lists.<AggregatorFactory>newArrayList(
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

    failMsg += " filtered timeseries ";
    HashMap<String, Object> context = new HashMap<>();
    Iterable<Result<TimeseriesResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }

  /** See {@link #runTests} */
  @SuppressWarnings("unused")
  private void testFullOnTopN(QueryRunner runner, List<Result<TopNResultValue>> expectedResults, String failMsg)
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(marketDimension)
        .metric(indexMetric)
        .threshold(3)
        .intervals(fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    failMsg += " topN ";
    HashMap<String, Object> context = new HashMap<>();
    Iterable<Result<TopNResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();

    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }

  /** See {@link #runTests} */
  @SuppressWarnings("unused")
  private void testFilteredTopN(QueryRunner runner, List<Result<TopNResultValue>> expectedResults, String failMsg)
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(dataSource)
        .granularity(allGran)
        .dimension(marketDimension)
        .filters(marketDimension, "spot")
        .metric(indexMetric)
        .threshold(3)
        .intervals(fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    failMsg += " filtered topN ";
    HashMap<String, Object> context = new HashMap<>();
    Iterable<Result<TopNResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }

  private void testFullOnSearch(QueryRunner runner, List<Result<SearchResultValue>> expectedResults, String failMsg)
  {
    SearchQuery query = Druids.newSearchQueryBuilder()
                              .dataSource(dataSource)
                              .granularity(allGran)
                              .intervals(fullOnInterval)
                              .query("a")
                              .build();

    failMsg += " search ";
    HashMap<String, Object> context = new HashMap<>();
    Iterable<Result<SearchResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }

  private void testFilteredSearch(QueryRunner runner, List<Result<SearchResultValue>> expectedResults, String failMsg)
  {
    SearchQuery query = Druids.newSearchQueryBuilder()
                              .dataSource(dataSource)
                              .granularity(allGran)
                              .filters(marketDimension, "spot")
                              .intervals(fullOnInterval)
                              .query("a")
                              .build();

    failMsg += " filtered search ";
    HashMap<String, Object> context = new HashMap<>();
    Iterable<Result<SearchResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }

  private void testTimeBoundary(
      QueryRunner runner,
      List<Result<TimeBoundaryResultValue>> expectedResults,
      String failMsg
  )
  {
    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource("testing")
                                    .build();

    failMsg += " timeBoundary ";
    HashMap<String, Object> context = new HashMap<>();
    Iterable<Result<TimeBoundaryResultValue>> actualResults = runner.run(QueryPlus.wrap(query), context).toList();
    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }
}
