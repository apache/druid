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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.query.Druids;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
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
import io.druid.query.topn.DimensionAndMetricValueExtractor;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class SchemalessTestSimpleTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    List<Object[]> argumentArrays = new ArrayList<>();
    for (SegmentWriteOutMediumFactory segmentWriteOutMediumFactory : SegmentWriteOutMediumFactory.builtInFactories()) {
      SchemalessIndexTest schemalessIndexTest = new SchemalessIndexTest(segmentWriteOutMediumFactory);
      final IncrementalIndex incrementalIndex = SchemalessIndexTest.getIncrementalIndex();
      final QueryableIndex persistedIncrementalIndex = TestIndex.persistRealtimeAndLoadMMapped(incrementalIndex);
      final QueryableIndex mergedIncrementalIndex = schemalessIndexTest.getMergedIncrementalIndex();
      argumentArrays.add(new Object[] {new IncrementalIndexSegment(incrementalIndex, null), false});
      argumentArrays.add(new Object[] {new QueryableIndexSegment(null, persistedIncrementalIndex), false});
      argumentArrays.add(new Object[] {new QueryableIndexSegment(null, mergedIncrementalIndex), true});
    }
    return argumentArrays;
  }

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

  private final Segment segment;
  private final boolean coalesceAbsentAndEmptyDims;

  public SchemalessTestSimpleTest(Segment segment, boolean coalesceAbsentAndEmptyDims)
  {
    this.segment = segment;
    this.coalesceAbsentAndEmptyDims = coalesceAbsentAndEmptyDims;
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
                                                  new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                                  new DoubleMinAggregatorFactory("minIndex", "index")
                                              )
                                          )
                                      )
                                  )
                                  .postAggregators(addRowsIndexConstant)
                                  .build();

    List<Result<TimeseriesResultValue>> expectedResults = Arrays.asList(
        new Result<TimeseriesResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                            .put("rows", coalesceAbsentAndEmptyDims ? 10L : 11L)
                            .put("index", 900.0)
                            .put("addRowsIndexConstant", coalesceAbsentAndEmptyDims ? 911.0 : 912.0)
                            .put("uniques", 2.000977198748901D)
                            .put("maxIndex", 100.0)
                            .put("minIndex", 0.0)
                            .build()
            )
        )
    );
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment);
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query), context));
  }


  //  @Test TODO: Handling of null values is inconsistent right now, need to make it all consistent and re-enable test
  // TODO: Complain to Eric when you see this.  It shouldn't be like this...
  @SuppressWarnings("unused")
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
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query), context));
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

    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment);
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query), context));
  }

  @Test
  public void testTimeBoundary()
  {
    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource("testing")
                                    .build();

    List<Result<TimeBoundaryResultValue>> expectedResults = Arrays.asList(
        new Result<TimeBoundaryResultValue>(
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

    QueryRunner runner = TestQueryRunners.makeTimeBoundaryQueryRunner(segment);
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query), context));
  }
}
