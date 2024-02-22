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

package org.apache.druid.query.groupby;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.UnnestDataSource;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.StringFormatExtractionFn;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;


@RunWith(Parameterized.class)
public class UnnestGroupByQueryRunnerTest extends InitializedNullHandlingTest
{
  private static TestGroupByBuffers BUFFER_POOLS = null;

  private final GroupByQueryRunnerFactory factory;
  private final GroupByQueryConfig config;
  private final boolean vectorize;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public UnnestGroupByQueryRunnerTest(
      GroupByQueryConfig config,
      GroupByQueryRunnerFactory factory,
      boolean vectorize
  )
  {
    this.config = config;
    this.factory = factory;
    this.vectorize = vectorize;
  }

  public static List<GroupByQueryConfig> testConfigs()
  {

    final GroupByQueryConfig v2Config = new GroupByQueryConfig()
    {

      @Override
      public int getBufferGrouperInitialBuckets()
      {
        // Small initial table to force some growing.
        return 4;
      }

      @Override
      public String toString()
      {
        return "v2";
      }
    };

    return ImmutableList.of(
        v2Config
    );
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools
  )
  {
    return makeQueryRunnerFactory(
        GroupByQueryRunnerTest.DEFAULT_MAPPER,
        config,
        bufferPools,
        GroupByQueryRunnerTest.DEFAULT_PROCESSING_CONFIG
    );
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools
  )
  {
    return makeQueryRunnerFactory(mapper, config, bufferPools, GroupByQueryRunnerTest.DEFAULT_PROCESSING_CONFIG);
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools,
      final DruidProcessingConfig processingConfig
  )
  {
    if (bufferPools.getBufferSize() != processingConfig.intermediateComputeSizeBytes()) {
      throw new ISE(
          "Provided buffer size [%,d] does not match configured size [%,d]",
          bufferPools.getBufferSize(),
          processingConfig.intermediateComputeSizeBytes()
      );
    }
    if (bufferPools.getNumMergeBuffers() != processingConfig.getNumMergeBuffers()) {
      throw new ISE(
          "Provided merge buffer count [%,d] does not match configured count [%,d]",
          bufferPools.getNumMergeBuffers(),
          processingConfig.getNumMergeBuffers()
      );
    }
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(bufferPools.getMergePool(), config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        processingConfig,
        configSupplier,
        bufferPools.getProcessingPool(),
        groupByResourcesReservationPool,
        TestHelper.makeJsonMapper(),
        mapper,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final GroupByQueryQueryToolChest toolChest =
        new GroupByQueryQueryToolChest(groupingEngine, groupByResourcesReservationPool);
    return new GroupByQueryRunnerFactory(groupingEngine, toolChest);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    NullHandling.initializeForTests();
    setUpClass();

    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : testConfigs()) {
      final GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(config, BUFFER_POOLS);

      for (boolean vectorize : ImmutableList.of(false)) {
        // Add vectorization tests for any indexes that support it.
        constructors.add(new Object[]{config, factory, vectorize});
      }

    }

    return constructors;
  }

  @BeforeClass
  public static void setUpClass()
  {
    if (BUFFER_POOLS == null) {
      BUFFER_POOLS = TestGroupByBuffers.createDefault();
    }
  }

  @AfterClass
  public static void tearDownClass()
  {
    BUFFER_POOLS.close();
    BUFFER_POOLS = null;
  }

  private static ResultRow makeRow(final GroupByQuery query, final String timestamp, final Object... vals)
  {
    return GroupByQueryRunnerTestHelper.createExpectedRow(query, timestamp, vals);
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(UnnestDataSource.create(
            new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
            new ExpressionVirtualColumn(
                QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                "\"" + QueryRunnerTestHelper.PLACEMENTISH_DIMENSION + "\"",
                null,
                ExprMacroTable.nil()
            ),
            null
        ))
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            2L,
            "idx",
            270L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            2L,
            "idx",
            236L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            2L,
            "idx",
            316L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            2L,
            "idx",
            240L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "idx",
            5740L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            2L,
            "idx",
            242L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "idx",
            5800L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            2L,
            "idx",
            156L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            2L,
            "idx",
            238L
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            2L,
            "idx",
            294L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "rows",
            2L,
            "idx",
            224L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            2L,
            "idx",
            332L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "rows",
            2L,
            "idx",
            226L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "idx",
            4894L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "rows",
            2L,
            "idx",
            228L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            6L,
            "idx",
            5010L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            2L,
            "idx",
            194L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            2L,
            "idx",
            252L
        )
    );

    Iterable<ResultRow> results = runQuery(query, TestIndex.getIncrementalTestIndex());
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByOnMissingColumn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(UnnestDataSource.create(
            new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
            new ExpressionVirtualColumn(
                QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
                "\"" + QueryRunnerTestHelper.PLACEMENTISH_DIMENSION + "\"",
                null,
                ExprMacroTable.nil()
            ),
            null
        ))
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("nonexistent0", "alias0"),
            new ExtractionDimensionSpec("nonexistent1", "alias1", new StringFormatExtractionFn("foo"))
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", null,
            "alias1", "foo",
            "rows", 52L
        )
    );

    Iterable<ResultRow> results = runQuery(query, TestIndex.getIncrementalTestIndex());
    TestHelper.assertExpectedObjects(expectedResults, results, "missing-column");
  }

  @Test
  public void testGroupByOnUnnestedColumn()
  {
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.UNNEST_DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "alias0"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    // Total rows should add up to 26 * 2 = 52
    // 26 rows and each has 2 entries in the column to be unnested
    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", "a",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "b",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "e",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "h",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "m",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "n",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "p",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "preferred",
            "rows", 26L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "t",
            "rows", 4L
        )
    );

    Iterable<ResultRow> results = runQuery(query, TestIndex.getIncrementalTestIndex());
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy-on-unnested-column");
  }

  @Test
  public void testGroupByOnUnnestedVirtualColumn()
  {
    cannotVectorize();

    final DataSource unnestDataSource = UnnestDataSource.create(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new ExpressionVirtualColumn(
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            "mv_to_array(placementish)",
            ColumnType.STRING_ARRAY,
            TestExprMacroTable.INSTANCE
        ),
        null
    );

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(unnestDataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "alias0")
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .addOrderByColumn("alias0", OrderByColumnSpec.Direction.ASCENDING)
        .build();

    // Total rows should add up to 26 * 2 = 52
    // 26 rows and each has 2 entries in the column to be unnested
    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", "a",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "b",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "e",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "h",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "m",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "n",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "p",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "preferred",
            "rows", 26L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "t",
            "rows", 4L
        )
    );

    Iterable<ResultRow> results = runQuery(query, TestIndex.getIncrementalTestIndex());
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy-on-unnested-virtual-column");
  }

  @Test
  public void testGroupByOnUnnestedVirtualMultiColumn()
  {
    cannotVectorize();

    final DataSource unnestDataSource = UnnestDataSource.create(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new ExpressionVirtualColumn(
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            "array(\"market\",\"quality\")",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        ),
        null
    );

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(unnestDataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "alias0"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setLimit(3)
        .build();

    // Each count should be 2, since we are unnesting "market" and "quality", which are singly-valued fields.
    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", "automotive",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "business",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "entertainment",
            "rows", 2L
        )
    );

    Iterable<ResultRow> results = runQuery(query, TestIndex.getIncrementalTestIndex());
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy-on-unnested-virtual-columns");
  }

  /**
   * Use this method instead of makeQueryBuilder() to make sure the context is set properly. Also, avoid
   * setContext in tests. Only use overrideContext.
   */
  private GroupByQuery.Builder makeQueryBuilder()
  {
    return GroupByQuery.builder().overrideContext(makeContext());
  }

  private Iterable<ResultRow> runQuery(final GroupByQuery query, final IncrementalIndex index)
  {
    final QueryRunner<?> queryRunner = factory.mergeRunners(
        DirectQueryProcessingPool.INSTANCE,
        Collections.singletonList(
            QueryRunnerTestHelper.makeQueryRunnerWithSegmentMapFn(
                factory,
                new IncrementalIndexSegment(
                    index,
                    QueryRunnerTestHelper.SEGMENT_ID
                ),
                query,
                "rtIndexvc"
            )
        )
    );

    return GroupByQueryRunnerTestHelper.runQuery(factory, queryRunner, query);
  }

  @Test
  public void testGroupByOnUnnestedFilterMatch()
  {
    // testGroupByOnUnnestedColumn but with filter to match single value
    cannotVectorize();

    final DataSource unnestDataSource = UnnestDataSource.create(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new ExpressionVirtualColumn(
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            "\"" + QueryRunnerTestHelper.PLACEMENTISH_DIMENSION + "\"",
            null,
            ExprMacroTable.nil()
        ),
        null
    );

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(unnestDataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "alias0")
        )
        .setDimFilter(
            new EqualityFilter(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, ColumnType.STRING, "a", null)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .addOrderByColumn("alias0", OrderByColumnSpec.Direction.ASCENDING)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", "a",
            "rows", 2L
        )
    );

    Iterable<ResultRow> results = runQuery(query, TestIndex.getIncrementalTestIndex());
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy-on-unnested-virtual-column");
  }

  @Test
  public void testGroupByOnUnnestedNotFilterMatch()
  {
    // testGroupByOnUnnestedColumn but with negated filter to match everything except 1 value
    cannotVectorize();

    final DataSource unnestDataSource = UnnestDataSource.create(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new ExpressionVirtualColumn(
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            "\"" + QueryRunnerTestHelper.PLACEMENTISH_DIMENSION + "\"",
            null,
            ExprMacroTable.nil()
        ),
        null
    );

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(unnestDataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "alias0")
        )
        .setDimFilter(
            NotDimFilter.of(new EqualityFilter(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, ColumnType.STRING, "a", null))
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .addOrderByColumn("alias0", OrderByColumnSpec.Direction.ASCENDING)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", "b",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "e",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "h",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "m",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "n",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "p",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "preferred",
            "rows", 26L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "t",
            "rows", 4L
        )
    );

    Iterable<ResultRow> results = runQuery(query, TestIndex.getIncrementalTestIndex());
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy-on-unnested-virtual-column");
  }

  @Test
  public void testGroupByOnUnnestedNotFilterMatchNonexistentValue()
  {
    // testGroupByOnUnnestedColumn but with negated filter on nonexistent value to still match everything
    cannotVectorize();

    final DataSource unnestDataSource = UnnestDataSource.create(
        new TableDataSource(QueryRunnerTestHelper.DATA_SOURCE),
        new ExpressionVirtualColumn(
            QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST,
            "\"" + QueryRunnerTestHelper.PLACEMENTISH_DIMENSION + "\"",
            null,
            ExprMacroTable.nil()
        ),
        null
    );

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(unnestDataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, "alias0")
        )
        .setDimFilter(
            NotDimFilter.of(new EqualityFilter(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION_UNNEST, ColumnType.STRING, "noexist", null))
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .addOrderByColumn("alias0", OrderByColumnSpec.Direction.ASCENDING)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias0", "a",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "b",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "e",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "h",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "m",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "n",
            "rows", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "p",
            "rows", 6L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "preferred",
            "rows", 26L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias0", "t",
            "rows", 4L
        )
    );

    Iterable<ResultRow> results = runQuery(query, TestIndex.getIncrementalTestIndex());
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy-on-unnested-virtual-column");
  }

  private Map<String, Object> makeContext()
  {
    return ImmutableMap.<String, Object>builder()
                       .put(QueryContexts.VECTORIZE_KEY, vectorize ? "force" : "false")
                       .put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize ? "force" : "false")
                       .put("vectorSize", 16) // Small vector size to ensure we use more than one.
                       .build();
  }

  private void cannotVectorize()
  {
    if (vectorize) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("Cannot vectorize!");
    }
  }
}
