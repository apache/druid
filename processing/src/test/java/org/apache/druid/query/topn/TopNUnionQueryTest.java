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

package org.apache.druid.query.topn;

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
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class TopNUnionQueryTest extends InitializedNullHandlingTest
{
  private static final Closer RESOURCE_CLOSER = Closer.create();

  @AfterClass
  public static void teardown() throws IOException
  {
    RESOURCE_CLOSER.close();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final CloseableStupidPool<ByteBuffer> defaultPool = TestQueryRunners.createDefaultNonBlockingPool();
    final CloseableStupidPool<ByteBuffer> customPool = new CloseableStupidPool<>(
        "TopNQueryRunnerFactory-bufferPool",
        () -> ByteBuffer.allocate(2000)
    );

    return QueryRunnerTestHelper.cartesian(
        Iterables.concat(
            QueryRunnerTestHelper.makeUnionQueryRunners(
                new TopNQueryRunnerFactory(
                    defaultPool,
                    new TopNQueryQueryToolChest(new TopNQueryConfig()),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            ),
            QueryRunnerTestHelper.makeUnionQueryRunners(
                new TopNQueryRunnerFactory(
                    customPool,
                    new TopNQueryQueryToolChest(new TopNQueryConfig()),
                    QueryRunnerTestHelper.NOOP_QUERYWATCHER
                )
            )
        )
    );
  }

  private final QueryRunner runner;

  public TopNUnionQueryTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  @Test
  public void testTopNUnionQuery()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.UNION_DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(
            QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT,
            QueryRunnerTestHelper.DEPENDENT_POST_AGG,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAgg
        )
        .build();

    //
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82817599995D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 792.3260498046875D)
                        .put(
                            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                            QueryRunnerTestHelper.UNIQUES_2 + 1.0
                        )
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.10593600012D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.06103515625D)
                        .put("minIndex", 545.9906005859375D)
                        .put(
                            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                            QueryRunnerTestHelper.UNIQUES_2 + 1.0
                        )
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57239499995D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put(
                            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                            QueryRunnerTestHelper.UNIQUES_9 + 1.0
                        )
                        .put("maxIndex", 277.2735290527344D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }


}
