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
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
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
import org.apache.druid.query.search.SearchHit;
import org.apache.druid.query.search.SearchQuery;
import org.apache.druid.query.search.SearchResultValue;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeboundary.TimeBoundaryQuery;
import org.apache.druid.query.timeboundary.TimeBoundaryResultValue;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.DimensionAndMetricValueExtractor;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class SchemalessTestSimpleTest extends InitializedNullHandlingTest
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
      argumentArrays.add(new Object[] {new QueryableIndexSegment(persistedIncrementalIndex, null), false});
      argumentArrays.add(new Object[] {new QueryableIndexSegment(mergedIncrementalIndex, null), true});
    }
    return argumentArrays;
  }

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

  private final Segment segment;
  private final boolean coalesceAbsentAndEmptyDims;

  public SchemalessTestSimpleTest(Segment segment, boolean coalesceAbsentAndEmptyDims)
  {
    this.segment = segment;
    // Empty and empty dims are equivalent only when replaceWithDefault is true
    this.coalesceAbsentAndEmptyDims = coalesceAbsentAndEmptyDims && NullHandling.replaceWithDefault();
  }

  @Test
  public void testFullOnTimeseries()
  {
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
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

    List<Result<TimeseriesResultValue>> expectedResults = Collections.singletonList(
        new Result(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TimeseriesResultValue(
                ImmutableMap.<String, Object>builder()
                    .put("rows", coalesceAbsentAndEmptyDims ? 10L : 11L)
                    .put("index", 900.0)
                    .put("addRowsIndexConstant", coalesceAbsentAndEmptyDims ? 911.0 : 912.0)
                    .put("uniques", 2.000977198748901D)
                    .put("maxIndex", 100.0)
                    .put("minIndex", NullHandling.replaceWithDefault() ? 0.0 : 100.0)
                    .build()
            )
        )
    );
    QueryRunner runner = TestQueryRunners.makeTimeSeriesQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }


  //  @Test TODO: Handling of null values is inconsistent right now, need to make it all consistent and re-enable test
  // TODO: Complain to Eric when you see this.  It shouldn't be like this...
  @Ignore
  @SuppressWarnings("unused")
  public void testFullOnTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
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

    try (CloseableStupidPool<ByteBuffer> pool = TestQueryRunners.createDefaultNonBlockingPool()) {
      QueryRunner runner = TestQueryRunners.makeTopNQueryRunner(segment, pool);
      TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
    }
  }

  @Test
  public void testFullOnSearch()
  {
    SearchQuery query = Druids.newSearchQueryBuilder()
                              .dataSource(dataSource)
                              .granularity(ALL_GRAN)
                              .intervals(fullOnInterval)
                              .query("a")
                              .build();

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

    QueryRunner runner = TestQueryRunners.makeSearchQueryRunner(segment);
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testTimeBoundary()
  {
    TimeBoundaryQuery query = Druids.newTimeBoundaryQueryBuilder()
                                    .dataSource("testing")
                                    .build();

    List<Result<TimeBoundaryResultValue>> expectedResults = Collections.singletonList(
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
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }
}
