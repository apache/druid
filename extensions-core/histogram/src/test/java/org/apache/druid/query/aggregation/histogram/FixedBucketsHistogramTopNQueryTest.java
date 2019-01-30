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

package org.apache.druid.query.aggregation.histogram;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TestQueryRunners;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.TestHelper;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class FixedBucketsHistogramTopNQueryTest
{
  private static final Closer resourceCloser = Closer.create();

  @AfterClass
  public static void teardown() throws IOException
  {
    resourceCloser.close();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final CloseableStupidPool<ByteBuffer> defaultPool = TestQueryRunners.createDefaultNonBlockingPool();
    final CloseableStupidPool<ByteBuffer> customPool = new CloseableStupidPool<>(
        "TopNQueryRunnerFactory-bufferPool",
        () -> ByteBuffer.allocate(2000)
    );
    resourceCloser.register(defaultPool);
    resourceCloser.register(customPool);

    return QueryRunnerTestHelper.transformToConstructionFeeder(
        Iterables.concat(
            QueryRunnerTestHelper.makeQueryRunners(
                new TopNQueryRunnerFactory(
                    defaultPool,
                    new TopNQueryQueryToolChest(
                        new TopNQueryConfig(),
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    ),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            ),
            QueryRunnerTestHelper.makeQueryRunners(
                new TopNQueryRunnerFactory(
                    customPool,
                    new TopNQueryQueryToolChest(
                        new TopNQueryConfig(),
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    ),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
        )
    );
  }

  private final QueryRunner runner;

  public FixedBucketsHistogramTopNQueryTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testTopNWithFixedHistogramAgg()
  {
    FixedBucketsHistogramAggregatorFactory factory = new FixedBucketsHistogramAggregatorFactory(
        "histo",
        "index",
        10,
        0,
        2000,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW
    );

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnIntervalSpec)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonDoubleAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index"),
                        factory
                    )
                )
            )
        )
        .postAggregators(
            Arrays.asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg,
                new QuantilePostAggregator("quantile", "histo", 0.5f)
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.9217529296875D)
                        .put("minIndex", 792.3260498046875D)
                        .put("quantile", 1135.238f)
                        .put(
                            "histo",
                            new FixedBucketsHistogram(
                                0,
                                2000,
                                10,
                                FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
                                new long[]{0, 0, 0, 1, 21, 105, 42, 12, 5, 0},
                                186,
                                1743.92175,
                                792.326066,
                                0,
                                0,
                                0
                            )
                        )
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.06103515625D)
                        .put("minIndex", 545.9906005859375D)
                        .put("quantile", 969.69696f)
                        .put(
                            "histo",
                            new FixedBucketsHistogram(
                                0,
                                2000,
                                10,
                                FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
                                new long[]{0, 0, 4, 33, 66, 35, 25, 11, 10, 2},
                                186,
                                1870.061029,
                                545.990623,
                                0,
                                0,
                                0
                            )
                        )
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.2735290527344D)
                        .put("minIndex", 59.02102279663086D)
                        .put("quantile", 100.23952f)
                        .put(
                            "histo",
                            new FixedBucketsHistogram(
                                0,
                                2000,
                                10,
                                FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
                                new long[]{835, 2, 0, 0, 0, 0, 0, 0, 0, 0},
                                837,
                                277.273533,
                                59.021022,
                                0,
                                0,
                                0
                            )
                        )
                        .build()
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();

    List<Result<TopNResultValue>> results = runner.run(QueryPlus.wrap(query), context).toList();
    TestHelper.assertExpectedResults(expectedResults, results);
  }
}
