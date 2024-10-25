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
import com.google.common.collect.Lists;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.TestQueryRunner;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.firstlast.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@RunWith(Parameterized.class)
public class GroupByQueryMetricsTest
{
  public static final ObjectMapper DEFAULT_MAPPER = TestHelper.makeSmileMapper();

  private static TestGroupByBuffers BUFFER_POOLS = null;

  private final QueryRunner<ResultRow> runner;
  private final TestQueryRunner<ResultRow> originalRunner;
  private final GroupByQueryRunnerFactory factory;
  private final GroupByStatsProvider groupByStatsProvider;
  private final boolean vectorize;

  public static GroupByQueryConfig testConfig()
  {
    return new GroupByQueryConfig()
    {
      @Override
      public int getBufferGrouperMaxSize()
      {
        return 2;
      }

      @Override
      public HumanReadableBytes getMaxOnDiskStorage()
      {
        return HumanReadableBytes.valueOf(10L * 1024 * 1024);
      }

      @Override
      public String toString()
      {
        return "v2SmallBuffer";
      }
    };
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools,
      final DruidProcessingConfig processingConfig,
      final GroupByStatsProvider statsProvider
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
    final GroupByResourcesReservationPool groupByResourcesReservationPool =
        new GroupByResourcesReservationPool(bufferPools.getMergePool(), config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        processingConfig,
        configSupplier,
        groupByResourcesReservationPool,
        mapper,
        mapper,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        statsProvider
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(
        groupingEngine,
        () -> config,
        DefaultGroupByQueryMetricsFactory.instance(),
        groupByResourcesReservationPool,
        statsProvider
    );
    return new GroupByQueryRunnerFactory(groupingEngine, toolChest, bufferPools.getProcessingPool());
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    NullHandling.initializeForTests();
    setUpClass();

    final List<Object[]> constructors = new ArrayList<>();
    GroupByQueryConfig config = testConfig();
    GroupByStatsProvider statsProvider = new GroupByStatsProvider();
    final GroupByQueryRunnerFactory factory = makeQueryRunnerFactory(
        DEFAULT_MAPPER,
        config,
        BUFFER_POOLS,
        GroupByQueryRunnerTest.DEFAULT_PROCESSING_CONFIG,
        statsProvider
    );
    for (QueryRunner<ResultRow> runner : QueryRunnerTestHelper.makeQueryRunners(factory, true)) {
      for (boolean vectorize : ImmutableList.of(false, true)) {
        final String testName = StringUtils.format("config=%s, runner=%s, vectorize=%s", config, runner, vectorize);

        // Add vectorization tests for any indexes that support it.
        if (!vectorize || (QueryRunnerTestHelper.isTestRunnerVectorizable(runner))) {
          constructors.add(new Object[]{testName, factory, runner, statsProvider, vectorize});
        }
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

  @SuppressWarnings("unused")
  public GroupByQueryMetricsTest(
      String testName,
      GroupByQueryRunnerFactory factory,
      TestQueryRunner runner,
      GroupByStatsProvider groupByStatsProvider,
      boolean vectorize
  )
  {
    this.factory = factory;
    this.runner = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
    this.originalRunner = runner;
    this.groupByStatsProvider = groupByStatsProvider;
    this.vectorize = vectorize;
  }

  @Test
  public void testSubqueryWithPostAggregatorsAndHaving()
  {
    // Granularity != ALL requires time-ordering.
    GroupByQueryRunnerTest.assumeTimeOrdered(originalRunner);

    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx_subagg", "index"))
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg",
                    "+",
                    Arrays.asList(
                        new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                        new ConstantPostAggregator("thousand", 1000)
                    )
                )

            )
        )
        .setHavingSpec(
            new HavingSpec()
            {
              private GroupByQuery query;

              @Override
              public void setQuery(GroupByQuery query)
              {
                this.query = query;
              }

              @Override
              public byte[] getCacheKey()
              {
                return new byte[0];
              }

              @Override
              public boolean eval(ResultRow row)
              {
                final String field = "idx_subpostagg";
                final int p = query.getResultRowSignature().indexOf(field);
                return (Rows.objectToNumber(field, row.get(p), true).floatValue() < 3800);
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .queryId(UUID.randomUUID().toString())
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx_subpostagg")
        )
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new ArithmeticPostAggregator(
                    "idx_post", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_agg", "idx"),
                    new ConstantPostAggregator("ten_thousand", 10000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .queryId(UUID.randomUUID().toString())
        .build();

    // Subqueries are handled by the ToolChest
    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    verifyMetrics(2);
  }

  @Test
  public void testDifferentGroupingSubqueryMultipleAggregatorsOnSameField()
  {
    // Granularity != ALL requires time-ordering.
    GroupByQueryRunnerTest.assumeTimeOrdered(originalRunner);

    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new ArithmeticPostAggregator(
                    "post_agg",
                    "+",
                    Lists.newArrayList(
                        new FieldAccessPostAggregator("idx", "idx"),
                        new FieldAccessPostAggregator("idx", "idx")
                    )
                )
            )
        )
        .queryId(UUID.randomUUID().toString())
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(
            new DoubleMaxAggregatorFactory("idx1", "idx"),
            new DoubleMaxAggregatorFactory("idx2", "idx"),
            new DoubleMaxAggregatorFactory("idx3", "post_agg"),
            new DoubleMaxAggregatorFactory("idx4", "post_agg")
        )
        .queryId(UUID.randomUUID().toString())
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    verifyMetrics(1);
  }

  @Test
  public void testGroupByOnVirtualColumn()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "v",
                "qualityDouble * qualityLong",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionVirtualColumn(
                "two",
                "2",
                null,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(
            new DefaultDimensionSpec("v", "v", ColumnType.LONG)
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("twosum", null, "1 + two", TestExprMacroTable.INSTANCE)
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setLimit(5)
        .queryId(UUID.randomUUID().toString())
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    verifyMetrics(1, true);
  }

  @Test
  public void testDifferentGroupingSubqueryWithFilter()
  {
    // Granularity != ALL requires time-ordering.
    GroupByQueryRunnerTest.assumeTimeOrdered(originalRunner);

    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .queryId(UUID.randomUUID().toString())
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
        .setDimFilter(
            new OrDimFilter(
                Lists.newArrayList(
                    new SelectorDimFilter("quality", "automotive", null),
                    new SelectorDimFilter("quality", "premium", null),
                    new SelectorDimFilter("quality", "mezzanine", null),
                    new SelectorDimFilter("quality", "business", null),
                    new SelectorDimFilter("quality", "entertainment", null),
                    new SelectorDimFilter("quality", "health", null),
                    new SelectorDimFilter("quality", "news", null),
                    new SelectorDimFilter("quality", "technology", null),
                    new SelectorDimFilter("quality", "travel", null)
                )
            )
        )
        .queryId(UUID.randomUUID().toString())
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    verifyMetrics(1);
  }

  @Test
  public void testMergeResultsWithLimitPushDownSortByAggWithOffset()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("idx", OrderByColumnSpec.Direction.DESCENDING)),
                2,
                3
            )
        )
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .setGranularity(Granularities.ALL)
        .queryId(UUID.randomUUID().toString());

    final GroupByQuery allGranQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<ResultRow>()
        {
          @Override
          public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
          {
            // simulate two daily segments
            final QueryPlus<ResultRow> queryPlus1 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
                )
            );
            final QueryPlus<ResultRow> queryPlus2 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
                )
            );

            return Sequences.simple(factory.getToolchest().mergeResults(
                (queryPlus3, responseContext1) -> new MergeSequence<>(
                    queryPlus3.getQuery().getResultOrdering(),
                    Sequences.simple(
                        Arrays.asList(
                            Sequences.simple(runner.run(queryPlus1, responseContext1).toList()),
                            Sequences.simple(runner.run(queryPlus2, responseContext1).toList())
                        )
                    )
                )
            ).run(
                queryPlus.withQuery(
                    GroupByQueryRunnerTestHelper.populateResourceId(queryPlus.getQuery())
                ),
                responseContext
            ).toList());
          }
        }
    );

    mergedRunner.run(
        QueryPlus.wrap(GroupByQueryRunnerTestHelper.populateResourceId(allGranQuery))
    ).toList();

    verifyMetrics(1);
  }

  @Test
  public void testGroupByWithFirstLast()
  {
    // Granularity != ALL requires time-ordering.
    GroupByQueryRunnerTest.assumeTimeOrdered(originalRunner);

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(
            new LongFirstAggregatorFactory("first", "index", null),
            new LongLastAggregatorFactory("last", "index", null)
        )
        .setGranularity(QueryRunnerTestHelper.MONTH_GRAN)
        .queryId(UUID.randomUUID().toString())
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    verifyMetrics(1);
  }

  @Test
  public void testGroupByOnNullableDoubleNoLimitPushdown()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("doubleNumericNull", "nullable", ColumnType.DOUBLE)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_APPLY_LIMIT_PUSH_DOWN, false))
        .setLimitSpec(new DefaultLimitSpec(ImmutableList.of(new OrderByColumnSpec(
            "nullable",
            OrderByColumnSpec.Direction.ASCENDING
        )), 5))
        .queryId(UUID.randomUUID().toString())
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    verifyMetrics(1, true);
  }

  private void verifyMetrics(long queries, boolean skipMergeDictionary)
  {
    GroupByStatsProvider.AggregateStats aggregateStats = groupByStatsProvider.getStatsSince();
    Assert.assertEquals(queries, aggregateStats.getSpilledQueries());
    Assert.assertTrue(aggregateStats.getSpilledBytes() > 0);
    Assert.assertEquals(1, aggregateStats.getMergeBufferAcquisitionCount());
    Assert.assertTrue(aggregateStats.getMergeBufferAcquisitionTimeNs() > 0);
    if (!skipMergeDictionary) {
      Assert.assertTrue(aggregateStats.getMergeDictionarySize() > 0);
    }
  }

  private void verifyMetrics(long queries)
  {
    verifyMetrics(queries, false);
  }

  private GroupByQuery.Builder makeQueryBuilder()
  {
    return GroupByQuery.builder().overrideContext(makeContext());
  }

  private Map<String, Object> makeContext()
  {
    return ImmutableMap.<String, Object>builder()
                       .put(QueryContexts.VECTORIZE_KEY, vectorize ? "force" : "false")
                       .put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize ? "force" : "false")
                       .put("vectorSize", 16) // Small vector size to ensure we use more than one.
                       .build();
  }
}
