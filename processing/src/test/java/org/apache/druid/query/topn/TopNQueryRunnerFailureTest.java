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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.Result;
import org.apache.druid.query.TestQueryRunners;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TopNQueryRunnerFailureTest extends InitializedNullHandlingTest
{
  private static final Closer RESOURCE_CLOSER = Closer.create();

  @AfterClass
  public static void teardown() throws IOException
  {
    RESOURCE_CLOSER.close();
  }

  @Parameterized.Parameters(name = "{7}")
  public static Iterable<Object[]> constructorFeeder()
  {
    List<QueryRunner<Result<TopNResultValue>>> retVal = queryRunners();
    List<Object[]> parameters = new ArrayList<>();
    final int nParam = 7;
    for (int i = 0; i < 32; i++) {
      for (QueryRunner<Result<TopNResultValue>> firstParameter : retVal) {
        Object[] params = new Object[nParam];
        params[0] = firstParameter;
        params[1] = (i & 1) != 0;
        params[2] = (i & 2) != 0;
        params[3] = (i & 4) != 0;
        params[4] = (i & 8) != 0;
        params[5] = QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS;
        params[6] = firstParameter + " double aggs";
        Object[] params2 = Arrays.copyOf(params, nParam);
        params2[5] = QueryRunnerTestHelper.COMMON_FLOAT_AGGREGATORS;
        params2[6] = firstParameter + " float aggs";
        parameters.add(params);
        parameters.add(params2);
      }
    }
    return parameters;
  }

  public static List<QueryRunner<Result<TopNResultValue>>> queryRunners()
  {
    final CloseableStupidPool<ByteBuffer> defaultPool = TestQueryRunners.createDefaultNonBlockingPool();
    final CloseableStupidPool<ByteBuffer> customPool = new CloseableStupidPool<>(
        "TopNQueryRunnerFactory-bufferPool",
        () -> ByteBuffer.allocate(20000)
    );

    List<QueryRunner<Result<TopNResultValue>>> retVal = new ArrayList<>();
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunnersToMerge(
            new TopNQueryRunnerFactory(
                defaultPool,
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            ),
            false
        )
    );
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunnersToMerge(
            new TopNQueryRunnerFactory(
                customPool,
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            ),
            false
        )
    );

    RESOURCE_CLOSER.register(() -> {
      // Verify that all objects have been returned to the pool.
      Assert.assertEquals("defaultPool objects created", defaultPool.poolSize(), defaultPool.objectsCreatedCount());
      Assert.assertEquals("customPool objects created", customPool.poolSize(), customPool.objectsCreatedCount());
      defaultPool.close();
      customPool.close();
    });

    return retVal;
  }

  private final QueryRunner<Result<TopNResultValue>> runner;
  private final List<AggregatorFactory> commonAggregators;


  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @SuppressWarnings("unused")
  public TopNQueryRunnerFailureTest(
      QueryRunner<Result<TopNResultValue>> runner,
      boolean specializeGeneric1AggPooledTopN,
      boolean specializeGeneric2AggPooledTopN,
      boolean specializeHistorical1SimpleDoubleAggPooledTopN,
      boolean specializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN,
      List<AggregatorFactory> commonAggregators,
      String testName
  )
  {
    this.runner = runner;
    PooledTopNAlgorithm.setSpecializeGeneric1AggPooledTopN(specializeGeneric1AggPooledTopN);
    PooledTopNAlgorithm.setSpecializeGeneric2AggPooledTopN(specializeGeneric2AggPooledTopN);
    PooledTopNAlgorithm.setSpecializeHistorical1SimpleDoubleAggPooledTopN(
        specializeHistorical1SimpleDoubleAggPooledTopN
    );
    PooledTopNAlgorithm.setSpecializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN(
        specializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN
    );
    this.commonAggregators = commonAggregators;
  }

  private Sequence<Result<TopNResultValue>> assertExpectedResults(
      Iterable<Result<TopNResultValue>> expectedResults,
      TopNQuery query
  )
  {
    final Sequence<Result<TopNResultValue>> retval = runWithMerge(query);;
    TestHelper.assertExpectedResults(expectedResults, retval);
    return retval;
  }

  private Sequence<Result<TopNResultValue>> runWithMerge(TopNQuery query)
  {
    return runWithMerge(query, ResponseContext.createEmpty());
  }

  private Sequence<Result<TopNResultValue>> runWithMerge(TopNQuery query, ResponseContext context)
  {
    return runner.run(QueryPlus.wrap(query), context);
  }

  @Test
  public void testEmptyTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.EMPTY_INTERVAL)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index"),
                        new DoubleFirstAggregatorFactory("first", "index", null)
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            DateTimes.of("2020-04-02T00:00:00.000Z"),
            TopNResultValue.create(ImmutableList.of())
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test(expected = ResourceLimitExceededException.class)
  public void testFullOnTopN()
  {
    Map<String, Object> context = new HashMap<>();
    context.put(QueryContexts.MAX_TOP_N_AGGREGATOR_HEAP_SIZE_BYTES, 1);
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .context(context)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
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
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.emptyList();
    assertExpectedResults(expectedResults,
                          query.withAggregatorSpecs(Lists.newArrayList(Iterables.concat(
                              QueryRunnerTestHelper.COMMON_FLOAT_AGGREGATORS,
                              Lists.newArrayList(
                                  new FloatMaxAggregatorFactory("maxIndex", "indexFloat"),
                                  new FloatMinAggregatorFactory("minIndex", "indexFloat")
                              )
                          )))
    );
  }
}
