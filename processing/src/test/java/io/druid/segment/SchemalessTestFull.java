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
import com.metamx.common.guava.Sequences;
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
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
public class SchemalessTestFull
{
  final double UNIQUES_2 = 2.000977198748901d;
  final double UNIQUES_1 = 1.0002442201269182d;

  final String dataSource = "testing";
  final QueryGranularity allGran = QueryGranularity.ALL;
  final String dimensionValue = "dimension";
  final String valueValue = "value";
  final String marketDimension = "market";
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

  @Test
  public void testCompleteIntersectingSchemas()
  {
    List<Result<TimeseriesResultValue>> expectedTimeSeriesResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-13T00:00:00.000Z")
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z")
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
                    new HashMap<String, Object>()
                    {{
                        put("market", null);
                        put("rows", 1L);
                        put("index", 0.0D);
                        put("addRowsIndexConstant", 2.0D);
                        put("uniques", 0.0D);
                        put("maxIndex", 0.0);
                        put("minIndex", 0.0);
                      }}
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z")
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(qualityDimension, "automotive"),
                    new SearchHit(marketDimension, "total_market")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-13T00:00:00.000Z")
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z")
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
                    new HashMap<String, Object>()
                    {{
                        put("market", null);
                        put("rows", 1L);
                        put("index", 0.0D);
                        put("addRowsIndexConstant", 2.0D);
                        put("uniques", 0.0D);
                        put("maxIndex", 0.0);
                        put("minIndex", 0.0);
                      }}
                )
            )
        )
    );

    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z")
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList();
    List<Result<TopNResultValue>> expectedFilteredTopNResults = Arrays.asList();

    List<Result<SearchResultValue>> expectedSearchResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList()
            )
        )
    );
    List<Result<SearchResultValue>> expectedFilteredSearchResults = expectedSearchResults;

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    runTests(
        new QueryableIndexSegment(
            null, SchemalessIndex.getMergedIncrementalIndex(0, 0)
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    runTests(
        new QueryableIndexSegment(
            null, SchemalessIndex.getMergedIncrementalIndex(1, 1)
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new HashMap<String, Object>()
                    {{
                        put("market", null);
                        put("rows", 2L);
                        put("index", 200.0D);
                        put("addRowsIndexConstant", 203.0D);
                        put("uniques", 0.0D);
                        put("maxIndex", 100.0);
                        put("minIndex", 100.0);
                      }},
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementDimension, "mezzanine")
                )
            )
        )
    );

    List<Result<SearchResultValue>> expectedFilteredSearchResults = Arrays.asList(
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList()
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z")
                )
            )
        )
    );

    runTests(
        new QueryableIndexSegment(
            null, SchemalessIndex.getMergedIncrementalIndex(new int[]{6, 7, 8})
        )
        ,
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
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 11L)
                            .put("index", 900.0D)
                            .put("addRowsIndexConstant", 912.0D)
                            .put("uniques", UNIQUES_1)
                            .put("maxIndex", 100.0D)
                            .put("minIndex", 0.0D)
                            .build()
            )
        )
    );

    List<Result<TimeseriesResultValue>> expectedFilteredTimeSeriesResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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

    List<Result<TopNResultValue>> expectedTopNResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        new Result<SearchResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new SearchResultValue(
                Arrays.<SearchHit>asList(
                    new SearchHit(placementishDimension, "a"),
                    new SearchHit(qualityDimension, "automotive")
                )
            )
        )
    );

    List<Result<TimeBoundaryResultValue>> expectedTimeBoundaryResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeBoundaryResultValue(
                ImmutableMap.of(
                    TimeBoundaryQuery.MIN_TIME,
                    new DateTime("2011-01-12T00:00:00.000Z"),
                    TimeBoundaryQuery.MAX_TIME,
                    new DateTime("2011-01-13T00:00:00.000Z")
                )
            )
        )
    );

    runTests(
        new QueryableIndexSegment(null, SchemalessIndex.getMergedIncrementalIndexDiffMetrics()),
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

  private List<Pair<QueryableIndex, String>> getIndexes(int index1, int index2)
  {
    return Arrays.asList(
        new Pair<QueryableIndex, String>(
            SchemalessIndex.getIncrementalIndex(index1, index2),
            String.format("Failed: II[%,d, %,d]", index1, index2)
        ),
        new Pair<QueryableIndex, String>(
            SchemalessIndex.getIncrementalIndex(index2, index1),
            String.format("Failed: II[%,d, %,d]", index2, index1)
        ),
        new Pair<QueryableIndex, String>(
            SchemalessIndex.getMergedIncrementalIndex(index1, index2),
            String.format("Failed: MII[%,d, %,d]", index1, index2)
        ),
        new Pair<QueryableIndex, String>(
            SchemalessIndex.getMergedIncrementalIndex(index2, index1),
            String.format("Failed: MII[%,d, %,d]", index2, index1)
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
      List<Result<TopNResultValue>> expectedTopNResults,
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
    testFullOnTopN(TestQueryRunners.makeTopNQueryRunner(adapter), expectedTopNResults, failMsg);
    testFilteredTopN(TestQueryRunners.makeTopNQueryRunner(adapter), expectedFilteredTopNResults, failMsg);
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
                                                  new MaxAggregatorFactory("maxIndex", "index"),
                                                  new MinAggregatorFactory("minIndex", "index")
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
                                  .build();

    failMsg += " timeseries ";
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
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
                                                  new MaxAggregatorFactory("maxIndex", "index"),
                                                  new MinAggregatorFactory("minIndex", "index")
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
                                  .build();

    failMsg += " filtered timeseries ";
    Iterable<Result<TimeseriesResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeseriesResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }


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
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    failMsg += " topN ";
    Iterable<Result<TopNResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TopNResultValue>>newArrayList()
    );

    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }

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
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    failMsg += " filtered topN ";
    Iterable<Result<TopNResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TopNResultValue>>newArrayList()
    );
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
    Iterable<Result<SearchResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<SearchResultValue>>newArrayList()
    );
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
    Iterable<Result<SearchResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<SearchResultValue>>newArrayList()
    );
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
    Iterable<Result<TimeBoundaryResultValue>> actualResults = Sequences.toList(
        runner.run(query),
        Lists.<Result<TimeBoundaryResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, actualResults, failMsg);
  }
}
