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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.TestQueryRunners;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 *
 */
@RunWith(Parameterized.class)
public class UnnestTopNQueryRunnerTest extends InitializedNullHandlingTest
{
  private static final Closer RESOURCE_CLOSER = Closer.create();
  private final QueryRunner<Result<TopNResultValue>> runner;
  private final boolean duplicateSingleAggregatorQueries;
  private final List<AggregatorFactory> commonAggregators;
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public UnnestTopNQueryRunnerTest(
      QueryRunner<Result<TopNResultValue>> runner,
      boolean specializeGeneric1AggPooledTopN,
      boolean specializeGeneric2AggPooledTopN,
      boolean specializeHistorical1SimpleDoubleAggPooledTopN,
      boolean specializeHistoricalSingleValueDimSelector1SimpleDoubleAggPooledTopN,
      boolean duplicateSingleAggregatorQueries,
      List<AggregatorFactory> commonAggregators
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
    this.duplicateSingleAggregatorQueries = duplicateSingleAggregatorQueries;
    this.commonAggregators = commonAggregators;
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    RESOURCE_CLOSER.close();
  }

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    List<QueryRunner<Result<TopNResultValue>>> retVal = queryRunners();
    List<Object[]> parameters = new ArrayList<>();
    for (int i = 0; i < 32; i++) {
      for (QueryRunner<Result<TopNResultValue>> firstParameter : retVal) {
        Object[] params = new Object[7];
        params[0] = firstParameter;
        params[1] = (i & 1) != 0;
        params[2] = (i & 2) != 0;
        params[3] = (i & 4) != 0;
        params[4] = (i & 8) != 0;
        params[5] = (i & 16) != 0;
        params[6] = QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS;
        Object[] params2 = Arrays.copyOf(params, 7);
        params2[6] = QueryRunnerTestHelper.COMMON_FLOAT_AGGREGATORS;
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

    List<QueryRunner<Result<TopNResultValue>>> retVal = new ArrayList<>(Collections.singletonList(
        QueryRunnerTestHelper.makeUnnestQueryRunners(
            new TopNQueryRunnerFactory(
                defaultPool,
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            ),
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION,
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            null
        ).get(0)));

    RESOURCE_CLOSER.register(() -> {
      // Verify that all objects have been returned to the pool.
      Assert.assertEquals("defaultPool objects created", defaultPool.poolSize(), defaultPool.objectsCreatedCount());
      Assert.assertEquals("customPool objects created", customPool.poolSize(), customPool.objectsCreatedCount());
      defaultPool.close();
      customPool.close();
    });

    return retVal;
  }

  private static Map<String, Object> makeRowWithNulls(
      String dimName,
      @Nullable Object dimValue,
      String metric,
      @Nullable Object metricVal
  )
  {
    Map<String, Object> nullRow = new HashMap<>();
    nullRow.put(dimName, dimValue);
    nullRow.put(metric, metricVal);
    return nullRow;
  }

  private List<AggregatorFactory> duplicateAggregators(AggregatorFactory aggregatorFactory, AggregatorFactory duplicate)
  {
    if (duplicateSingleAggregatorQueries) {
      return ImmutableList.of(aggregatorFactory, duplicate);
    } else {
      return Collections.singletonList(aggregatorFactory);
    }
  }

  private List<Map<String, Object>> withDuplicateResults(
      List<? extends Map<String, Object>> results,
      String key,
      String duplicateKey
  )
  {
    if (!duplicateSingleAggregatorQueries) {
      return (List<Map<String, Object>>) results;
    }
    List<Map<String, Object>> resultsWithDuplicates = new ArrayList<>();
    for (Map<String, Object> result : results) {
      resultsWithDuplicates.add(
          ImmutableMap.<String, Object>builder().putAll(result).put(duplicateKey, result.get(key)).build()
      );
    }
    return resultsWithDuplicates;
  }

  private Sequence<Result<TopNResultValue>> assertExpectedResults(
      Iterable<Result<TopNResultValue>> expectedResults,
      TopNQuery query
  )
  {
    final Sequence<Result<TopNResultValue>> retval = runWithMerge(query);
    TestHelper.assertExpectedResults(expectedResults, retval);
    return retval;
  }

  private Sequence<Result<TopNResultValue>> runWithMerge(TopNQuery query)
  {
    return runWithMerge(query, ResponseContext.createEmpty());
  }

  private Sequence<Result<TopNResultValue>> runWithMerge(TopNQuery query, ResponseContext context)
  {
    final TopNQueryQueryToolChest chest = new TopNQueryQueryToolChest(new TopNQueryConfig());
    final QueryRunner<Result<TopNResultValue>> mergeRunner = new FinalizeResultsQueryRunner(
        chest.mergeResults(runner),
        chest
    );
    return mergeRunner.run(QueryPlus.wrap(query), context);
  }

  @Test
  public void testEmptyTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.UNNEST_DATA_SOURCE)
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
            new TopNResultValue(ImmutableList.of())
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicUnnest()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.UNNEST_DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .metric(new DimensionTopNMetricSpec("", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "a",
                        "rows", 2L,
                        "index", 283.311029D,
                        "addRowsIndexConstant", 286.311029D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "b",
                        "rows", 2L,
                        "index", 231.557367D,
                        "addRowsIndexConstant", 234.557367D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "e",
                        "rows", 2L,
                        "index", 324.763273D,
                        "addRowsIndexConstant", 327.763273D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "h",
                        "rows", 2L,
                        "index", 233.580712D,
                        "addRowsIndexConstant", 236.580712D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNStringVirtualColumnUnnest()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(UnnestDataSource.create(
            new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
            "vc",
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            null
        ))
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .virtualColumns(
            new ExpressionVirtualColumn(
                "vc",
                "mv_to_array(\"placementish\")",
                ColumnType.STRING_ARRAY,
                TestExprMacroTable.INSTANCE
            )
        )
        .dimension(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "preferred",
                        "rows", 26L,
                        "index", 12459.361287D,
                        "addRowsIndexConstant", 12486.361287D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "m",
                        "rows", 6L,
                        "index", 5320.717303D,
                        "addRowsIndexConstant", 5327.717303D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "p",
                        "rows", 6L,
                        "index", 5407.213795D,
                        "addRowsIndexConstant", 5414.213795D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "t",
                        "rows", 4L,
                        "index", 422.344086D,
                        "addRowsIndexConstant", 427.344086D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }
}

