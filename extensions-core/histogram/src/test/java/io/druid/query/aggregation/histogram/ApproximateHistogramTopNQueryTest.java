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

package io.druid.query.aggregation.histogram;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import io.druid.collections.StupidPool;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
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
public class ApproximateHistogramTopNQueryTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.transformToConstructionFeeder(
        Iterables.concat(
            QueryRunnerTestHelper.makeQueryRunners(
                new TopNQueryRunnerFactory(
                    TestQueryRunners.getPool(),
                    new TopNQueryQueryToolChest(
                        new TopNQueryConfig(),
                        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                    ),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            ),
            QueryRunnerTestHelper.makeQueryRunners(
                new TopNQueryRunnerFactory(
                    new StupidPool<ByteBuffer>(
                        "TopNQueryRunnerFactory-bufferPool",
                        new Supplier<ByteBuffer>()
                        {
                          @Override
                          public ByteBuffer get()
                          {
                            return ByteBuffer.allocate(2000);
                          }
                        }
                    ),
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

  public ApproximateHistogramTopNQueryTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testTopNWithApproximateHistogramAgg()
  {
    ApproximateHistogramAggregatorFactory factory = new ApproximateHistogramAggregatorFactory(
        "apphisto",
        "index",
        10,
        5,
        Float.NEGATIVE_INFINITY,
        Float.POSITIVE_INFINITY
    );

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
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
            Arrays.<PostAggregator>asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg,
                new QuantilePostAggregator("quantile", "apphisto", 0.5f)
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<TopNResultValue>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
                        .put("quantile", 1085.6775f)
                        .put(
                            "apphisto",
                            new Histogram(
                                new float[]{
                                    554.4271240234375f,
                                    792.3260498046875f,
                                    1030.2249755859375f,
                                    1268.1239013671875f,
                                    1506.0228271484375f,
                                    1743.9217529296875f
                                },
                                new double[]{
                                    0.0D,
                                    39.42073059082031D,
                                    103.29110717773438D,
                                    34.93659591674805D,
                                    8.351564407348633D
                                }
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
                        .put("quantile", 880.9881f)
                        .put(
                            "apphisto",
                            new Histogram(
                                new float[]{
                                    214.97299194335938f,
                                    545.9906005859375f,
                                    877.0081787109375f,
                                    1208.0257568359375f,
                                    1539.0433349609375f,
                                    1870.06103515625f
                                },
                                new double[]{
                                    0.0D,
                                    67.53287506103516D,
                                    72.22068786621094D,
                                    31.984678268432617D,
                                    14.261756896972656D
                                }
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
                        .put("quantile", 101.78856f)
                        .put(
                            "apphisto",
                            new Histogram(
                                new float[]{
                                    4.457897186279297f,
                                    59.02102279663086f,
                                    113.58415222167969f,
                                    168.14727783203125f,
                                    222.7104034423828f,
                                    277.2735290527344f
                                },
                                new double[]{
                                    0.0D,
                                    462.4309997558594D,
                                    357.5404968261719D,
                                    15.022850036621094D,
                                    2.0056631565093994D
                                }
                            )
                        )
                        .build()
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();

    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }
}
