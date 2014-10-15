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
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.incremental.IncrementalIndex;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class SchemalessTestSimple
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    final IncrementalIndex incrementalIndex = SchemalessIndex.getIncrementalIndex();
    final QueryableIndex persistedIncrementalIndex = TestIndex.persistRealtimeAndLoadMMapped(incrementalIndex);
    final QueryableIndex mergedIncrementalIndex = SchemalessIndex.getMergedIncrementalIndex();

    return Arrays.asList(
        new Object[][]{
            {
                new IncrementalIndexSegment(incrementalIndex, null)
            },
            {
                new QueryableIndexSegment(
                    null, persistedIncrementalIndex
                )
            },
            {
                new QueryableIndexSegment(
                    null, mergedIncrementalIndex
                )
            }
        }
    );
  }

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

  private Segment segment;

  public SchemalessTestSimple(
      Segment segment
  )
  {
    this.segment = segment;
  }

  @Test
  public void testFullOnTimeseries()
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

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", 11L)
                            .put("index", 900.0)
                            .put("addRowsIndexConstant", 912.0)
                            .put("uniques", 2.000977198748901D)
                            .put("maxIndex", 100.0)
                            .put("minIndex", 0.0)
                            .build()
            )
        )
    );
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }


  //  @Test TODO: Handling of null values is inconsistent right now, need to make it all consistent and re-enable test
  // TODO: Complain to Eric when you see this.  It shouldn't be like this...
  public void testFullOnTopN()
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

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<DimensionAndMetricValueExtractor>asList(
                    new DimensionAndMetricValueExtractor(
                        ImmutableMap.<String, Object>builder()
                                    .put("market", "spot")
                                    .put("rows", 4L)
                                    .put("index", 400.0D)
                                    .put("addRowsIndexConstant", 405.0D)
                                    .put("uniques", 1.0002442201269182D)
                                    .put("maxIndex", 100.0)
                                    .put("minIndex", 100.0)
                                    .build()
                    ),
                    new DimensionAndMetricValueExtractor(
                        ImmutableMap.<String, Object>builder()
                                    .put("market", "")
                                    .put("rows", 2L)
                                    .put("index", 200.0D)
                                    .put("addRowsIndexConstant", 203.0D)
                                    .put("uniques", 0.0)
                                    .put("maxIndex", 100.0D)
                                    .put("minIndex", 100.0D)
                                    .build()
                    ),
                    new DimensionAndMetricValueExtractor(
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
        )
    );

    QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testFullOnSearch()
  {
    SearchQuery query = Druids.newSearchQueryBuilder()
                              .dataSource(dataSource)
                              .granularity(allGran)
                              .intervals(fullOnInterval)
                              .query("a")
                              .build();

    List<Result<SearchResultValue>> expectedResults = Arrays.asList(
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

    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }

  @Test
  public void testTimeBoundary()
  {
    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource("testing")
                                    .build();

    List<Result<TimeBoundaryResultValue>> expectedResults = Arrays.asList(
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

    QueryRunner runner = TestQueryRunners.makeTimeBoundaryQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(query));
  }
}
