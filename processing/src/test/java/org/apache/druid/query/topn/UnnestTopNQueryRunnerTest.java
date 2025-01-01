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
import org.apache.druid.query.aggregation.firstlast.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;


/**
 *
 */
@RunWith(Parameterized.class)
public class UnnestTopNQueryRunnerTest extends InitializedNullHandlingTest
{
  private static final Closer RESOURCE_CLOSER = Closer.create();
  private final List<AggregatorFactory> commonAggregators;


  public UnnestTopNQueryRunnerTest(
      List<AggregatorFactory> commonAggregators
  )
  {
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
    List<Object[]> constructors = new ArrayList<>();
    constructors.add(new Object[]{QueryRunnerTestHelper.COMMON_FLOAT_AGGREGATORS});
    return constructors;
  }

  private Sequence<Result<TopNResultValue>> assertExpectedResultsWithCustomRunner(
      Iterable<Result<TopNResultValue>> expectedResults,
      TopNQuery query,
      QueryRunner runner
  )
  {
    final Sequence<Result<TopNResultValue>> retval = runWithMerge(query, runner);
    TestHelper.assertExpectedResults(expectedResults, retval);
    return retval;
  }

  private Sequence<Result<TopNResultValue>> runWithMerge(TopNQuery query, QueryRunner runner)
  {
    return runWithMerge(query, ResponseContext.createEmpty(), runner);
  }


  private Sequence<Result<TopNResultValue>> runWithMerge(TopNQuery query, ResponseContext context, QueryRunner runner1)
  {
    final TopNQueryQueryToolChest chest = new TopNQueryQueryToolChest(new TopNQueryConfig());
    final QueryRunner<Result<TopNResultValue>> mergeRunner = new FinalizeResultsQueryRunner(
        chest.mergeResults(runner1),
        chest
    );
    return mergeRunner.run(QueryPlus.wrap(query), context);
  }

  @Test
  public void testEmptyTopN()
  {
    final CloseableStupidPool<ByteBuffer> defaultPool = TestQueryRunners.createDefaultNonBlockingPool();
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

    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();

    QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        new TopNQueryRunnerFactory(
            defaultPool,
            new TopNQueryQueryToolChest(new TopNQueryConfig()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        new IncrementalIndexSegment(
            rtIndex,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    List<Result<TopNResultValue>> expectedResults = ImmutableList.of(
        new Result<>(
            DateTimes.of("2020-04-02T00:00:00.000Z"),
            TopNResultValue.create(ImmutableList.of())
        )
    );
    assertExpectedResultsWithCustomRunner(expectedResults, query, queryRunner);
  }

  @Test
  public void testTopNLexicographicUnnest()
  {
    final CloseableStupidPool<ByteBuffer> defaultPool = TestQueryRunners.createDefaultNonBlockingPool();

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

    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();

    QueryRunner queryRunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        new TopNQueryRunnerFactory(
            defaultPool,
            new TopNQueryQueryToolChest(new TopNQueryConfig()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        new IncrementalIndexSegment(
            rtIndex,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            TopNResultValue.create(
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
    assertExpectedResultsWithCustomRunner(expectedResults, query, queryRunner);
  }

  @Test
  public void testTopNStringVirtualColumnUnnest()
  {
    final CloseableStupidPool<ByteBuffer> defaultPool = TestQueryRunners.createDefaultNonBlockingPool();

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(UnnestDataSource.create(
            new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
            new ExpressionVirtualColumn(
                QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                "mv_to_array(\"placementish\")",
                ColumnType.STRING_ARRAY,
                TestExprMacroTable.INSTANCE
            ),
            null
        ))
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();
    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();

    QueryRunner vcrunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        new TopNQueryRunnerFactory(
            defaultPool,
            new TopNQueryQueryToolChest(new TopNQueryConfig()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        new IncrementalIndexSegment(
            rtIndex,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            TopNResultValue.create(
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
    assertExpectedResultsWithCustomRunner(expectedResults, query, vcrunner);
  }

  @Test
  public void testTopNStringVirtualMultiColumnUnnest()
  {
    final CloseableStupidPool<ByteBuffer> defaultPool = TestQueryRunners.createDefaultNonBlockingPool();
    final CloseableStupidPool<ByteBuffer> customPool = new CloseableStupidPool<>(
        "TopNQueryRunnerFactory-bufferPool",
        () -> ByteBuffer.allocate(20000)
    );

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(UnnestDataSource.create(
            new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
            new ExpressionVirtualColumn(
                QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                "array(\"market\",\"quality\")",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            ),
            null
        ))
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST)
        .metric("rows")
        .threshold(2)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    final IncrementalIndex rtIndex = TestIndex.getIncrementalTestIndex();

    QueryRunner vcrunner = QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
        new TopNQueryRunnerFactory(
            defaultPool,
            new TopNQueryQueryToolChest(new TopNQueryConfig()),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        new IncrementalIndexSegment(
            rtIndex,
            QueryRunnerTestHelper.SEGMENT_ID
        ),
        query,
        "rtIndexvc"
    );

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            TopNResultValue.create(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "premium",
                        "rows", 6L,
                        "index", 5407.213795D,
                        "addRowsIndexConstant", 5414.213795D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResultsWithCustomRunner(expectedResults, query, vcrunner);
    RESOURCE_CLOSER.register(() -> {
      // Verify that all objects have been returned to the pool.
      Assert.assertEquals("defaultPool objects created", defaultPool.poolSize(), defaultPool.objectsCreatedCount());
      Assert.assertEquals("customPool objects created", customPool.poolSize(), customPool.objectsCreatedCount());
      defaultPool.close();
      customPool.close();
    });
  }
}

