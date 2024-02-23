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
import com.google.common.base.Functions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Row;
import org.apache.druid.data.input.Rows;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.ChainedExecutionQueryRunner;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ResourceLimitExceededException;
import org.apache.druid.query.Result;
import org.apache.druid.query.TestBigDecimalSumAggregatorFactory;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.ExpressionLambdaAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.JavaScriptAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.mean.DoubleMeanAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.dimension.RegexFilteredDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.extraction.CascadeExtractionFn;
import org.apache.druid.query.extraction.DimExtractionFn;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.extraction.JavaScriptExtractionFn;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.extraction.RegexDimExtractionFn;
import org.apache.druid.query.extraction.SearchQuerySpecDimExtractionFn;
import org.apache.druid.query.extraction.StringFormatExtractionFn;
import org.apache.druid.query.extraction.StrlenExtractionFn;
import org.apache.druid.query.extraction.SubstringDimExtractionFn;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExtractionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.epinephelinae.UnexpectedMultiValueDimensionException;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.having.DimensionSelectorHavingSpec;
import org.apache.druid.query.groupby.having.EqualToHavingSpec;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
import org.apache.druid.query.groupby.having.HavingSpec;
import org.apache.druid.query.groupby.having.OrHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.ComparableList;
import org.apache.druid.segment.data.ComparableStringArray;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableCauseMatcher;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class GroupByQueryRunnerTest extends InitializedNullHandlingTest
{
  public static final ObjectMapper DEFAULT_MAPPER = TestHelper.makeSmileMapper();
  public static final DruidProcessingConfig DEFAULT_PROCESSING_CONFIG = new DruidProcessingConfig()
  {
    @Override
    public String getFormatString()
    {
      return null;
    }

    @Override
    public int intermediateComputeSizeBytes()
    {
      return 10 * 1024 * 1024;
    }

    @Override
    public int getNumMergeBuffers()
    {
      // Some tests need two buffers for testing nested groupBy (simulating two levels of merging).
      // Some tests need more buffers for parallel combine (testMergedPostAggHavingSpec).
      return 4;
    }

    @Override
    public int getNumThreads()
    {
      return 2;
    }
  };

  private static TestGroupByBuffers BUFFER_POOLS = null;

  private final QueryRunner<ResultRow> runner;
  private final QueryRunner<ResultRow> originalRunner;
  private final GroupByQueryRunnerFactory factory;
  private final GroupByQueryConfig config;
  private final boolean vectorize;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

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
    final GroupByQueryConfig v2SmallBufferConfig = new GroupByQueryConfig()
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
    final GroupByQueryConfig v2SmallDictionaryConfig = new GroupByQueryConfig()
    {

      @Override
      public long getConfiguredMaxSelectorDictionarySize()
      {
        return 20;
      }

      @Override
      public long getConfiguredMaxMergingDictionarySize()
      {
        return 400;
      }

      @Override
      public HumanReadableBytes getMaxOnDiskStorage()
      {
        return HumanReadableBytes.valueOf(10L * 1024 * 1024);
      }

      @Override
      public String toString()
      {
        return "v2SmallDictionary";
      }
    };
    final GroupByQueryConfig v2ParallelCombineConfig = new GroupByQueryConfig()
    {

      @Override
      public int getNumParallelCombineThreads()
      {
        return DEFAULT_PROCESSING_CONFIG.getNumThreads();
      }

      @Override
      public String toString()
      {
        return "v2ParallelCombine";
      }
    };

    return ImmutableList.of(
        v2Config,
        v2SmallBufferConfig,
        v2SmallDictionaryConfig,
        v2ParallelCombineConfig
    );
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final GroupByQueryConfig config
  )
  {
    return makeQueryRunnerFactory(
        DEFAULT_MAPPER,
        config,
        new TestGroupByBuffers(
            DEFAULT_PROCESSING_CONFIG.intermediateComputeSizeBytes(),
            DEFAULT_PROCESSING_CONFIG.getNumMergeBuffers()
        ),
        DEFAULT_PROCESSING_CONFIG
    );
  }
  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools
  )
  {
    return makeQueryRunnerFactory(DEFAULT_MAPPER, config, bufferPools, DEFAULT_PROCESSING_CONFIG);
  }

  public static GroupByQueryRunnerFactory makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final TestGroupByBuffers bufferPools
  )
  {
    return makeQueryRunnerFactory(mapper, config, bufferPools, DEFAULT_PROCESSING_CONFIG);
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
    final GroupingEngine groupingEngine = new GroupingEngine(
        processingConfig,
        configSupplier,
        bufferPools.getProcessingPool(),
        bufferPools.getMergePool(),
        TestHelper.makeJsonMapper(),
        mapper,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(groupingEngine);
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
      for (QueryRunner<ResultRow> runner : QueryRunnerTestHelper.makeQueryRunners(factory)) {
        for (boolean vectorize : ImmutableList.of(false, true)) {
          final String testName = StringUtils.format("config=%s, runner=%s, vectorize=%s", config, runner, vectorize);

          // Add vectorization tests for any indexes that support it.
          if (!vectorize || (QueryRunnerTestHelper.isTestRunnerVectorizable(runner))) {
            constructors.add(new Object[]{testName, config, factory, runner, vectorize});
          }
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
  public GroupByQueryRunnerTest(
      String testName,
      GroupByQueryConfig config,
      GroupByQueryRunnerFactory factory,
      QueryRunner runner,
      boolean vectorize
  )
  {
    this.config = config;
    this.factory = factory;
    this.runner = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
    this.originalRunner = runner;
    this.vectorize = vectorize;
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
            new DoubleSumAggregatorFactory("idxDouble", "index")
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
            1L,
            "idx",
            135L,
            "idxFloat",
            135.88510131835938f,
            "idxDouble",
            135.88510131835938d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            118L,
            "idxFloat",
            118.57034,
            "idxDouble",
            118.57034
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            158L,
            "idxFloat",
            158.747224,
            "idxDouble",
            158.747224
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            120L,
            "idxFloat",
            120.134704,
            "idxDouble",
            120.134704
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2870L,
            "idxFloat",
            2871.8866900000003f,
            "idxDouble",
            2871.8866900000003d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            121L,
            "idxFloat",
            121.58358f,
            "idxDouble",
            121.58358d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2900L,
            "idxFloat",
            2900.798647f,
            "idxDouble",
            2900.798647d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            78L,
            "idxFloat",
            78.622547f,
            "idxDouble",
            78.622547d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            119L,
            "idxFloat",
            119.922742f,
            "idxDouble",
            119.922742d
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            147L,
            "idxFloat",
            147.42593f,
            "idxDouble",
            147.42593d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            112L,
            "idxFloat",
            112.987027f,
            "idxDouble",
            112.987027d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            166L,
            "idxFloat",
            166.016049f,
            "idxDouble",
            166.016049d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            113L,
            "idxFloat",
            113.446008f,
            "idxDouble",
            113.446008d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2447L,
            "idxFloat",
            2448.830613f,
            "idxDouble",
            2448.830613d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            114L,
            "idxFloat",
            114.290141f,
            "idxDouble",
            114.290141d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2505L,
            "idxFloat",
            2506.415148f,
            "idxDouble",
            2506.415148d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            97L,
            "idxFloat",
            97.387433f,
            "idxDouble",
            97.387433d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            126L,
            "idxFloat",
            126.411364f,
            "idxDouble",
            126.411364d
        )
    );

    StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQueryWithEmitter(
        factory,
        originalRunner,
        query,
        serviceEmitter
    );
    serviceEmitter.verifyEmitted("query/wait/time", ImmutableMap.of("vectorized", vectorize), 1);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByOnMissingColumn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
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
            "rows", 26L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "missing-column");
  }

  @Test
  public void testGroupByWithStringPostAggregator()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "post",
            "travelx",
            "rows",
            1L,
            "idx",
            119L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "post",
            "technologyx",
            "rows",
            1L,
            "idx",
            78L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "post",
            "premiumx",
            "rows",
            3L,
            "idx",
            2900L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "post",
            "newsx",
            "rows",
            1L,
            "idx",
            121L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "post",
            "mezzaninex",
            "rows",
            3L,
            "idx",
            2870L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "post",
            "healthx",
            "rows",
            1L,
            "idx",
            120L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "post",
            "entertainmentx",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "post",
            "businessx",
            "rows",
            1L,
            "idx",
            118L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "automotive",
            "post",
            "automotivex",
            "rows",
            1L,
            "idx",
            135L
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "post",
            "travelx",
            "rows",
            1L,
            "idx",
            126L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "post",
            "technologyx",
            "rows",
            1L,
            "idx",
            97L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "post",
            "premiumx",
            "rows",
            3L,
            "idx",
            2505L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "post",
            "newsx",
            "rows",
            1L,
            "idx",
            114L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "post",
            "mezzaninex",
            "rows",
            3L,
            "idx",
            2447L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "post",
            "healthx",
            "rows",
            1L,
            "idx",
            113L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "post",
            "entertainmentx",
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "post",
            "businessx",
            "rows",
            1L,
            "idx",
            112L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "post",
            "automotivex",
            "rows",
            1L,
            "idx",
            147L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "string-postAgg");
  }

  @Test
  public void testGroupByWithStringVirtualColumn()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "vc",
                "quality + 'x'",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(new DefaultDimensionSpec("vc", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotivex", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "businessx", "rows", 1L, "idx", 118L),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainmentx",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(query, "2011-04-01", "alias", "healthx", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzaninex", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "newsx", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premiumx", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technologyx", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travelx", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotivex", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "businessx", "rows", 1L, "idx", 112L),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainmentx",
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(query, "2011-04-02", "alias", "healthx", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzaninex", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "newsx", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premiumx", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technologyx", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travelx", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "virtual-column");
  }

  @Test
  public void testGroupByWithStringVirtualColumnVectorizable()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "vc",
                "cast(quality, 'STRING')",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(new DefaultDimensionSpec("vc", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(query, "2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "virtual-column");
  }

  @Test
  public void testGroupByWithDurationGranularity()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new DurationGranularity(86400L, 0L))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "duration-granularity");
  }

  @Test
  public void testGroupByWithOutputNameCollisions()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("[alias] already defined");

    makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("alias", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();
  }

  @Test
  public void testGroupByWithSortDimsFirst()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .overrideContext(ImmutableMap.of("sortByDimsFirst", true))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),

        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),

        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),

        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),

        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),

        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),

        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),

        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),

        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),
        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "sort-by-dimensions-first");
  }

  @Test
  public void testGroupByNoAggregators()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive"),
        makeRow(query, "2011-04-01", "alias", "business"),
        makeRow(query, "2011-04-01", "alias", "entertainment"),
        makeRow(query, "2011-04-01", "alias", "health"),
        makeRow(query, "2011-04-01", "alias", "mezzanine"),
        makeRow(query, "2011-04-01", "alias", "news"),
        makeRow(query, "2011-04-01", "alias", "premium"),
        makeRow(query, "2011-04-01", "alias", "technology"),
        makeRow(query, "2011-04-01", "alias", "travel"),

        makeRow(query, "2011-04-02", "alias", "automotive"),
        makeRow(query, "2011-04-02", "alias", "business"),
        makeRow(query, "2011-04-02", "alias", "entertainment"),
        makeRow(query, "2011-04-02", "alias", "health"),
        makeRow(query, "2011-04-02", "alias", "mezzanine"),
        makeRow(query, "2011-04-02", "alias", "news"),
        makeRow(query, "2011-04-02", "alias", "premium"),
        makeRow(query, "2011-04-02", "alias", "technology"),
        makeRow(query, "2011-04-02", "alias", "travel")
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "no-aggs");
  }

  @Test
  public void testMultiValueDimension()
  {
    // Cannot vectorize due to multi-value dimensions.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("placementish", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "a", "rows", 2L, "idx", 282L),
        makeRow(query, "2011-04-01", "alias", "b", "rows", 2L, "idx", 230L),
        makeRow(query, "2011-04-01", "alias", "e", "rows", 2L, "idx", 324L),
        makeRow(query, "2011-04-01", "alias", "h", "rows", 2L, "idx", 233L),
        makeRow(query, "2011-04-01", "alias", "m", "rows", 6L, "idx", 5317L),
        makeRow(query, "2011-04-01", "alias", "n", "rows", 2L, "idx", 235L),
        makeRow(query, "2011-04-01", "alias", "p", "rows", 6L, "idx", 5405L),
        makeRow(query, "2011-04-01", "alias", "preferred", "rows", 26L, "idx", 12446L),
        makeRow(query, "2011-04-01", "alias", "t", "rows", 4L, "idx", 420L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "multi-value-dim");
  }

  @Test
  public void testMultiValueDimensionNotAllowed()
  {
    final String dimName = "placementish";

    if (!vectorize) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectCause(CoreMatchers.instanceOf(ExecutionException.class));
      expectedException.expectCause(
          ThrowableCauseMatcher.hasCause(CoreMatchers.instanceOf(UnexpectedMultiValueDimensionException.class))
      );
      expectedException.expect(
          new BaseMatcher<Throwable>()
          {
            @Override
            public boolean matches(Object o)
            {
              final UnexpectedMultiValueDimensionException cause =
                  (UnexpectedMultiValueDimensionException) ((Throwable) o).getCause().getCause();

              return dimName.equals(cause.getDimensionName());
            }

            @Override
            public void describeTo(Description description)
            {
              description.appendText("an UnexpectedMultiValueDimensionException with dimension [placementish]");
            }
          }
      );
      expectedException.expectMessage(
          StringUtils.format(
              "Encountered multi-value dimension [%s] that cannot be processed with '%s' set to false."
              + " Consider setting '%s' to true in your query context.",
              dimName,
              GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING,
              GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING
          )
      );
    } else {
      cannotVectorize();
    }

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec(dimName, "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false))
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }

  @Test
  public void testMultiValueDimensionAsArray()
  {
    // Cannot vectorize due to multi-value dimensions.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "mv_to_array(placementish)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias", ColumnType.STRING_ARRAY))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", ComparableStringArray.of("a", "preferred"), "rows", 2L, "idx", 282L),
        makeRow(query, "2011-04-01", "alias", ComparableStringArray.of("b", "preferred"), "rows", 2L, "idx", 230L),
        makeRow(query, "2011-04-01", "alias", ComparableStringArray.of("e", "preferred"), "rows", 2L, "idx", 324L),
        makeRow(query, "2011-04-01", "alias", ComparableStringArray.of("h", "preferred"), "rows", 2L, "idx", 233L),
        makeRow(query, "2011-04-01", "alias", ComparableStringArray.of("m", "preferred"), "rows", 6L, "idx", 5317L),
        makeRow(query, "2011-04-01", "alias", ComparableStringArray.of("n", "preferred"), "rows", 2L, "idx", 235L),
        makeRow(query, "2011-04-01", "alias", ComparableStringArray.of("p", "preferred"), "rows", 6L, "idx", 5405L),
        makeRow(query, "2011-04-01", "alias", ComparableStringArray.of("preferred", "t"), "rows", 4L, "idx", 420L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "multi-value-dim-groupby-arrays");
  }

  @Test
  public void testSingleValueDimensionAsArray()
  {
    // Cannot vectorize due to multi-value dimensions.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "mv_to_array(placement)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias", ColumnType.STRING_ARRAY))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = ImmutableList.of(
        makeRow(query, "2011-04-01", "alias",
                ComparableStringArray.of("preferred"), "rows", 26L, "idx", 12446L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "multi-value-dim-groupby-arrays");
  }

  @Test
  public void testMultiValueDimensionAsArrayWithOtherDims()
  {
    // Cannot vectorize due to multi-value dimensions.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "mv_to_array(placementish)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias", ColumnType.STRING_ARRAY),
            new DefaultDimensionSpec("quality", "quality")
        )
        .setLimitSpec(new DefaultLimitSpec(
            ImmutableList.of(new OrderByColumnSpec(
                "alias",
                OrderByColumnSpec.Direction.ASCENDING,
                StringComparators.LEXICOGRAPHIC
            ), new OrderByColumnSpec(
                "quality",
                OrderByColumnSpec.Direction.ASCENDING,
                StringComparators.LEXICOGRAPHIC
            )),
            Integer.MAX_VALUE - 1
        ))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("a", "preferred"),
            "quality",
            "automotive",
            "rows",
            2L,
            "idx",
            282L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("b", "preferred"),
            "quality",
            "business",
            "rows",
            2L,
            "idx",
            230L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("e", "preferred"),
            "quality",
            "entertainment",
            "rows",
            2L,
            "idx",
            324L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("h", "preferred"),
            "quality",
            "health",
            "rows",
            2L,
            "idx",
            233L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("m", "preferred"),
            "quality",
            "mezzanine",
            "rows",
            6L,
            "idx",
            5317L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("n", "preferred"),
            "quality",
            "news",
            "rows",
            2L,
            "idx",
            235L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("p", "preferred"),
            "quality",
            "premium",
            "rows",
            6L,
            "idx",
            5405L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("preferred", "t"),
            "quality",
            "technology",
            "rows",
            2L,
            "idx",
            175L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("preferred", "t"),
            "quality",
            "travel",
            "rows",
            2L,
            "idx",
            245L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "multi-value-dims-groupby-arrays");

    query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "mv_to_array(placementish)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias", ColumnType.STRING_ARRAY),
            new DefaultDimensionSpec("quality", "quality")
        )
        .setLimitSpec(new DefaultLimitSpec(
            ImmutableList.of(
                new OrderByColumnSpec(
                    "alias",
                    OrderByColumnSpec.Direction.DESCENDING,
                    StringComparators.LEXICOGRAPHIC
                ),
                new OrderByColumnSpec(
                    "quality",
                    OrderByColumnSpec.Direction.DESCENDING,
                    StringComparators.LEXICOGRAPHIC
                )
            ),
            Integer.MAX_VALUE - 1
        ))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    Collections.reverse(expectedResults);

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "multi-value-dims-groupby-arrays-descending");
  }

  @Test
  public void testMultiValueDimensionAsStringArrayWithoutExpression()
  {
    if (!vectorize) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("Not supported for multi-value dimensions");
    }

    cannotVectorize();
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("placementish", "alias", ColumnType.STRING_ARRAY)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }

  @Test
  public void testSingleValueDimensionAsStringArrayWithoutExpression()
  {
    if (!vectorize) {
      // cannot add exact class cast message due to discrepancies between various JDK versions
      expectedException.expect(RuntimeException.class);
    }
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("placement", "alias", ColumnType.STRING_ARRAY)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    List<ResultRow> expectedResults = ImmutableList.of(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            ComparableStringArray.of("preferred"),
            "rows",
            26L,
            "idx",
            12446L
        ));
    TestHelper.assertExpectedObjects(
        expectedResults,
        results,
        "single-value-dims-groupby-arrays-as-string-arrays"
    );
  }


  @Test
  public void testNumericDimAsStringArrayWithoutExpression()
  {
    if (!vectorize) {
      // cannot add exact class cast message due to discrepancies between various JDK versions
      expectedException.expect(RuntimeException.class);
    }

    cannotVectorize();
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("index", "alias", ColumnType.STRING_ARRAY)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }


  @Test
  public void testMultiValueVirtualDimAsString()
  {
    if (!vectorize) {
      // cannot add exact class cast message due to discrepancies between various JDK versions
      expectedException.expect(RuntimeException.class);
    }

    cannotVectorize();
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "mv_to_array(placementish)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias", ColumnType.STRING)
        )
        .setDimensions(
            new DefaultDimensionSpec("index", "alias", ColumnType.STRING_ARRAY)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }

  @Test
  public void testExtractionStringSpecWithMultiValueVirtualDimAsInput()
  {
    cannotVectorize();
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "mv_to_array(placementish)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new ExtractionDimensionSpec(
                "v0",
                "alias",
                ColumnType.STRING,
                new SubstringDimExtractionFn(1, 1)
            )
        )

        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            null,
            "rows",
            26L,
            "idx",
            12446L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "r",
            "rows",
            26L,
            "idx",
            12446L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(
        expectedResults,
        results,
        "multi-value-extraction-spec-as-string-dim-groupby-arrays"
    );
  }


  @Test
  public void testExtractionStringArraySpecWithMultiValueVirtualDimAsInput()
  {
    if (!vectorize) {
      expectedException.expect(RuntimeException.class);
      expectedException.expectMessage("Not supported for multi-value dimensions");
    }

    cannotVectorize();
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "mv_to_array(placementish)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new ExtractionDimensionSpec("v0", "alias", ColumnType.STRING_ARRAY,
                                        new SubstringDimExtractionFn(1, 1)
            )
        )

        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }

  @Test
  public void testVirtualColumnNumericTypeAsStringArray()
  {
    if (!vectorize) {
      // cannot add exact class cast message due to discrepancies between various JDK versions
      expectedException.expect(RuntimeException.class);
    }

    cannotVectorize();
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "array(index)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias", ColumnType.STRING_ARRAY
            )
        )

        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }

  @Test
  public void testNestedGroupByWithStringArray()
  {
    cannotVectorize();
    GroupByQuery inner = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "mv_to_array(placementish)",
            ColumnType.STRING_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias", ColumnType.STRING_ARRAY)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    GroupByQuery outer = makeQueryBuilder()
        .setDataSource(new QueryDataSource(inner))
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("alias", "alias_outer", ColumnType.STRING_ARRAY
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(outer, "2011-04-01", "alias_outer", ComparableStringArray.of("a", "preferred"), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", ComparableStringArray.of("b", "preferred"), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", ComparableStringArray.of("e", "preferred"), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", ComparableStringArray.of("h", "preferred"), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", ComparableStringArray.of("m", "preferred"), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", ComparableStringArray.of("n", "preferred"), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", ComparableStringArray.of("p", "preferred"), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", ComparableStringArray.of("preferred", "t"), "rows", 1L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outer);
    TestHelper.assertExpectedObjects(expectedResults, results, "multi-value-dim-nested-groupby-arrays");
  }

  @Test
  public void testNestedGroupByWithLongArrays()
  {
    cannotVectorize();
    GroupByQuery inner = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "array(1,2)",
            ColumnType.LONG_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias", ColumnType.LONG_ARRAY)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    GroupByQuery outer = makeQueryBuilder()
        .setDataSource(new QueryDataSource(inner))
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("alias", "alias_outer", ColumnType.LONG_ARRAY
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = ImmutableList.of(
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1L, 2L)),
                "rows", 1L
        ));

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outer);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-nested-groupby-arrays");
  }

  @Test
  public void testGroupByWithLongArrays()
  {
    cannotVectorize();
    GroupByQuery outer = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "array(index)",
            ColumnType.LONG_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias_outer", ColumnType.LONG_ARRAY)
        )
        .setLimitSpec(new DefaultLimitSpec(
            ImmutableList.of(new OrderByColumnSpec(
                "alias_outer",
                OrderByColumnSpec.Direction.ASCENDING,
                StringComparators.NUMERIC
            )),
            Integer.MAX_VALUE
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();


    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(78L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(97L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(109L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(110L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(112L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(113L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(114L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(118L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(119L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(120L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(121L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(126L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(135L)), "rows", 2L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(144L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(147L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(158L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(166L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1049L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1144L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1193L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1234L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1314L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1321L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1447L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1522L)), "rows", 1L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outer);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-groupby-arrays");
  }

  @Test
  public void testGroupByWithLongArraysDesc()
  {
    cannotVectorize();
    GroupByQuery outer = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "array(index)",
            ColumnType.LONG_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias_outer", ColumnType.LONG_ARRAY)
        )
        .setLimitSpec(new DefaultLimitSpec(
            ImmutableList.of(new OrderByColumnSpec(
                "alias_outer",
                OrderByColumnSpec.Direction.DESCENDING,
                StringComparators.NUMERIC
            )),
            Integer.MAX_VALUE - 1
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();


    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(78L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(97L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(109L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(110L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(112L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(113L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(114L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(118L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(119L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(120L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(121L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(126L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(135L)), "rows", 2L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(144L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(147L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(158L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(166L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1049L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1144L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1193L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1234L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1314L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1321L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1447L)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1522L)), "rows", 1L)
    );
    // reversing list
    Collections.reverse(expectedResults);
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outer);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-groupby-arrays");
  }

  @Test
  public void testGroupByWithDoubleArrays()
  {
    cannotVectorize();
    GroupByQuery outer = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "array(index)",
            ColumnType.DOUBLE_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias_outer", ColumnType.DOUBLE_ARRAY)
        )
        .setLimitSpec(new DefaultLimitSpec(
            ImmutableList.of(new OrderByColumnSpec(
                "alias_outer",
                OrderByColumnSpec.Direction.ASCENDING,
                StringComparators.NUMERIC
            )),
            Integer.MAX_VALUE - 1
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();


    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(78.622547)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(97.387433)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(109.705815)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(110.931934)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(112.987027)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(113.446008)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(114.290141)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(118.57034)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(119.922742)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(120.134704)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(121.583581)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(126.411364)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(135.301506)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(135.885094)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(144.507368)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(147.425935)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(158.747224)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(166.016049)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1049.738585)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1144.342401)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1193.556278)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1234.247546)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1314.839715)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1321.375057)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1447.34116)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1522.043733)), "rows", 1L)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outer);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-groupby-arrays");
  }


  @Test
  public void testGroupByWithDoubleArraysDesc()
  {
    cannotVectorize();
    GroupByQuery outer = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "v0",
            "array(index)",
            ColumnType.DOUBLE_ARRAY,
            ExprMacroTable.nil()
        ))
        .setDimensions(
            new DefaultDimensionSpec("v0", "alias_outer", ColumnType.DOUBLE_ARRAY)
        )
        .setLimitSpec(new DefaultLimitSpec(
            ImmutableList.of(new OrderByColumnSpec(
                "alias_outer",
                OrderByColumnSpec.Direction.DESCENDING,
                StringComparators.NUMERIC
            )),
            Integer.MAX_VALUE - 1
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();


    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(78.622547)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(97.387433)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(109.705815)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(110.931934)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(112.987027)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(113.446008)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(114.290141)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(118.57034)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(119.922742)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(120.134704)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(121.583581)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(126.411364)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(135.301506)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(135.885094)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(144.507368)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(147.425935)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(158.747224)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(166.016049)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1049.738585)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1144.342401)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1193.556278)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1234.247546)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1314.839715)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1321.375057)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1447.34116)), "rows", 1L),
        makeRow(outer, "2011-04-01", "alias_outer", new ComparableList(ImmutableList.of(1522.043733)), "rows", 1L)
    );
    // reversing list
    Collections.reverse(expectedResults);
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outer);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-groupby-arrays");
  }

  @Test
  public void testTwoMultiValueDimensions()
  {
    // Cannot vectorize due to multi-value dimensions.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimFilter(new SelectorDimFilter("placementish", "a", null))
        .setDimensions(
            new DefaultDimensionSpec("placementish", "alias"),
            new DefaultDimensionSpec("placementish", "alias2")
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "a",
            "alias2",
            "a",
            "rows",
            2L,
            "idx",
            282L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "a",
            "alias2",
            "preferred",
            "rows",
            2L,
            "idx",
            282L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "preferred",
            "alias2",
            "a",
            "rows",
            2L,
            "idx",
            282L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "preferred",
            "alias2",
            "preferred",
            "rows",
            2L,
            "idx",
            282L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "two-multi-value-dims");
  }

  @Test
  public void testMultipleDimensionsOneOfWhichIsMultiValue1()
  {
    // Cannot vectorize due to multi-value dimensions.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("placementish", "alias"),
            new DefaultDimensionSpec("quality", "quality")
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "automotive",
            "alias",
            "a",
            "rows",
            2L,
            "idx",
            282L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "business",
            "alias",
            "b",
            "rows",
            2L,
            "idx",
            230L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "entertainment",
            "alias",
            "e",
            "rows",
            2L,
            "idx",
            324L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "health",
            "alias",
            "h",
            "rows",
            2L,
            "idx",
            233L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "mezzanine",
            "alias",
            "m",
            "rows",
            6L,
            "idx",
            5317L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "news",
            "alias",
            "n",
            "rows",
            2L,
            "idx",
            235L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "premium",
            "alias",
            "p",
            "rows",
            6L,
            "idx",
            5405L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "automotive",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            282L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "business",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            230L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "entertainment",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            324L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "health",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            233L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "mezzanine",
            "alias",
            "preferred",
            "rows",
            6L,
            "idx",
            5317L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "news",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            235L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "premium",
            "alias",
            "preferred",
            "rows",
            6L,
            "idx",
            5405L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "technology",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            175L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "travel",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            245L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "technology",
            "alias",
            "t",
            "rows",
            2L,
            "idx",
            175L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "travel",
            "alias",
            "t",
            "rows",
            2L,
            "idx",
            245L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "one-multi-value-dim");
  }

  @Test
  public void testMultipleDimensionsOneOfWhichIsMultiValueDifferentOrder()
  {
    // Cannot vectorize due to multi-value dimensions.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("placementish", "alias")
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "automotive",
            "alias",
            "a",
            "rows",
            2L,
            "idx",
            282L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "automotive",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            282L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "business",
            "alias",
            "b",
            "rows",
            2L,
            "idx",
            230L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "business",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            230L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "entertainment",
            "alias",
            "e",
            "rows",
            2L,
            "idx",
            324L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "entertainment",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            324L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "health",
            "alias",
            "h",
            "rows",
            2L,
            "idx",
            233L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "health",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            233L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "mezzanine",
            "alias",
            "m",
            "rows",
            6L,
            "idx",
            5317L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "mezzanine",
            "alias",
            "preferred",
            "rows",
            6L,
            "idx",
            5317L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "news",
            "alias",
            "n",
            "rows",
            2L,
            "idx",
            235L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "news",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            235L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "premium",
            "alias",
            "p",
            "rows",
            6L,
            "idx",
            5405L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "premium",
            "alias",
            "preferred",
            "rows",
            6L,
            "idx",
            5405L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "technology",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            175L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "technology",
            "alias",
            "t",
            "rows",
            2L,
            "idx",
            175L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "travel",
            "alias",
            "preferred",
            "rows",
            2L,
            "idx",
            245L
        ),
        makeRow(
            query,
            "2011-04-01",
            "quality",
            "travel",
            "alias",
            "t",
            "rows",
            2L,
            "idx",
            245L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "one-multi-value-dim-different-order");
  }

  @Test
  public void testGroupByMaxRowsLimitContextOverride()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .overrideContext(ImmutableMap.of("maxResults", 1))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(query, "2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "override-maxResults");
  }

  @Test
  public void testGroupByTimeoutContextOverride()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .overrideContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 60000))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "override-timeout");
  }

  @Test
  public void testGroupByMaxOnDiskStorageContextOverride()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .overrideContext(ImmutableMap.of("maxOnDiskStorage", 0, "bufferGrouperMaxSize", 1))
        .build();

    List<ResultRow> expectedResults = null;
    expectedException.expect(ResourceLimitExceededException.class);
    expectedException.expectMessage("Not enough merge buffer memory to execute this query");


    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "overide-maxOnDiskStorage");
  }

  @Test
  public void testNotEnoughDiskSpaceThroughContextOverride()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .overrideContext(ImmutableMap.of("maxOnDiskStorage", 1, GroupByQueryConfig.CTX_KEY_BUFFER_GROUPER_MAX_SIZE, 1))
        .build();

    List<ResultRow> expectedResults = null;
    expectedException.expect(ResourceLimitExceededException.class);
    if (config.getMaxOnDiskStorage().getBytes() > 0) {
      // The error message always mentions disk if you have spilling enabled (maxOnDiskStorage > 0)
      expectedException.expectMessage("Not enough disk space to execute this query");
    } else {
      expectedException.expectMessage("Not enough merge buffer memory to execute this query");
    }

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "disk-space");
  }

  @Test
  public void testSubqueryWithOuterMaxOnDiskStorageContextOverride()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.ASCENDING)),
                null
            )
        )
        .overrideContext(
            ImmutableMap.of(
                "maxOnDiskStorage", Integer.MAX_VALUE,
                "bufferGrouperMaxSize", Integer.MAX_VALUE
            )
        )
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ArrayList<>()).setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .overrideContext(ImmutableMap.of("maxOnDiskStorage", 0, "bufferGrouperMaxSize", 0))
        .build();

    expectedException.expect(ResourceLimitExceededException.class);
    expectedException.expectMessage("Not enough merge buffer memory to execute this query");
    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }

  @Test
  public void testGroupByWithRebucketRename()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    Map<String, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(query, "2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(query, "2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "rebucket-rename");
  }


  @Test
  public void testGroupByWithSimpleRenameRetainMissingNonInjective()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    Map<String, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), true, null, false, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(query, "2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(query, "2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "non-injective");
  }


  @Test
  public void testGroupByWithSimpleRenameRetainMissing()
  {
    Map<String, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new ExtractionDimensionSpec(
                "quality",
                "alias",
                new LookupExtractionFn(new MapLookupExtractor(map, false), true, null, true, false)
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "entertainment0", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "entertainment0", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "retain-missing");
  }


  @Test
  public void testGroupByWithSimpleRenameAndMissingString()
  {
    Map<String, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, "MISSING", true, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "entertainment0", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "entertainment0", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "rename-and-missing-string");
  }

  @Test
  public void testGroupByWithSimpleRename()
  {
    Map<String, String> map = new HashMap<>();
    map.put("automotive", "automotive0");
    map.put("business", "business0");
    map.put("entertainment", "entertainment0");
    map.put("health", "health0");
    map.put("mezzanine", "mezzanine0");
    map.put("news", "news0");
    map.put("premium", "premium0");
    map.put("technology", "technology0");
    map.put("travel", "travel0");
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, true, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(query, "2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(query, "2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "simple-rename");
  }

  @Test
  public void testGroupByWithUniques()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, QueryRunnerTestHelper.QUALITY_UNIQUES)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            26L,
            "uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "uniques");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithUniquesAndPostAggWithSameName()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new HyperUniquesAggregatorFactory(
            "quality_uniques",
            "quality_uniques"
        ))
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator("quality_uniques", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            26L,
            "quality_uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "unique-postagg-same-name");
  }

  @Test
  public void testGroupByWithCardinality()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, QueryRunnerTestHelper.QUALITY_CARDINALITY)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            26L,
            "cardinality",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "cardinality");
  }

  @Test
  public void testGroupByWithFirstLast()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(
            new LongFirstAggregatorFactory("first", "index", null),
            new LongLastAggregatorFactory("last", "index", null)
        )
        .setGranularity(QueryRunnerTestHelper.MONTH_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-01-01", "market", "spot", "first", 100L, "last", 155L),
        makeRow(
            query,
            "2011-01-01",
            "market",
            "total_market",
            "first",
            1000L,
            "last",
            1127L
        ),
        makeRow(query, "2011-01-01", "market", "upfront", "first", 800L, "last", 943L),
        makeRow(query, "2011-02-01", "market", "spot", "first", 132L, "last", 114L),
        makeRow(
            query,
            "2011-02-01",
            "market",
            "total_market",
            "first",
            1203L,
            "last",
            1292L
        ),
        makeRow(
            query,
            "2011-02-01",
            "market",
            "upfront",
            "first",
            1667L,
            "last",
            1101L
        ),
        makeRow(query, "2011-03-01", "market", "spot", "first", 153L, "last", 125L),
        makeRow(
            query,
            "2011-03-01",
            "market",
            "total_market",
            "first",
            1124L,
            "last",
            1366L
        ),
        makeRow(
            query,
            "2011-03-01",
            "market",
            "upfront",
            "first",
            1166L,
            "last",
            1063L
        ),
        makeRow(query, "2011-04-01", "market", "spot", "first", 135L, "last", 120L),
        makeRow(
            query,
            "2011-04-01",
            "market",
            "total_market",
            "first",
            1314L,
            "last",
            1029L
        ),
        makeRow(query, "2011-04-01", "market", "upfront", "first", 1447L, "last", 780L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "first-last-aggs");
  }

  @Test
  public void testGroupByWithNoResult()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.EMPTY_INTERVAL)
        .setDimensions(new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            QueryRunnerTestHelper.INDEX_LONG_SUM,
            QueryRunnerTestHelper.QUALITY_CARDINALITY,
            new LongFirstAggregatorFactory("first", "index", null),
            new LongLastAggregatorFactory("last", "index", null)
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = ImmutableList.of();
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertEquals(expectedResults, results);
  }

  @Test
  public void testGroupByWithNullProducingDimExtractionFn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    final ExtractionFn nullExtractionFn = new RegexDimExtractionFn("(\\w{1})", false, null)
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{(byte) 0xFF};
      }

      @Override
      public String apply(String dimValue)
      {
        return "mezzanine".equals(dimValue) ? null : super.apply(dimValue);
      }
    };
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimensions(new ExtractionDimensionSpec("quality", "alias", nullExtractionFn))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", null, "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "a", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "b", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "e", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "h", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "n", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "p", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "t", "rows", 2L, "idx", 197L),

        makeRow(query, "2011-04-02", "alias", null, "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "a", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "b", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "e", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "h", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "n", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "p", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "t", "rows", 2L, "idx", 223L)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query),
        "null-dimextraction"
    );
  }

  @Test
  @Ignore
  /**
   * This test exists only to show what the current behavior is and not necessarily to define that this is
   * correct behavior.  In fact, the behavior when returning the empty string from a DimExtractionFn is, by
   * contract, undefined, so this can do anything.
   */
  public void testGroupByWithEmptyStringProducingDimExtractionFn()
  {
    final ExtractionFn emptyStringExtractionFn = new RegexDimExtractionFn("(\\w{1})", false, null)
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{(byte) 0xFF};
      }

      @Override
      public String apply(String dimValue)
      {
        return "mezzanine".equals(dimValue) ? "" : super.apply(dimValue);
      }
    };

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimensions(new ExtractionDimensionSpec("quality", "alias", emptyStringExtractionFn))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "a", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "b", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "e", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "h", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "n", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "p", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "t", "rows", 2L, "idx", 197L),

        makeRow(query, "2011-04-02", "alias", "", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "a", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "b", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "e", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "h", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "n", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "p", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "t", "rows", 2L, "idx", 223L)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query),
        "empty-string-dimextraction"
    );
  }

  @Test
  public void testGroupByWithTimeZone()
  {
    DateTimeZone tz = DateTimes.inferTzFromString("America/Los_Angeles");

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory(
            "idx",
            "index"
        ))
        .setGranularity(
            new PeriodGranularity(
                new Period("P1D"),
                null,
                tz
            )
        )
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            135L
        ),
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            118L
        ),
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            120L
        ),
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2870L
        ),
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            121L
        ),
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2900L
        ),
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            78L
        ),
        makeRow(
            query,
            new DateTime("2011-03-31", tz),
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            119L
        ),

        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            147L
        ),
        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            112L
        ),
        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            113L
        ),
        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2447L
        ),
        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            114L
        ),
        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2505L
        ),
        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            97L
        ),
        makeRow(
            query,
            new DateTime("2011-04-01", tz),
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            126L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "timezone");
  }

  @Test
  public void testMergeResults()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery fullQuery = builder.build();
    final GroupByQuery allGranQuery = builder.copy().setGranularity(Granularities.ALL).build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<ResultRow>()
        {
          @Override
          public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
                )
            );
            final QueryPlus queryPlus2 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
                )
            );
            return new MergeSequence(
                queryPlus.getQuery().getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(queryPlus1, responseContext), runner.run(queryPlus2, responseContext))
                )
            );
          }
        }
    );

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(fullQuery, "2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        makeRow(fullQuery, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(fullQuery, "2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        makeRow(fullQuery, "2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        makeRow(fullQuery, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(fullQuery, "2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        makeRow(fullQuery, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        makeRow(fullQuery, "2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        makeRow(fullQuery, "2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    ResponseContext context = ResponseContext.createEmpty();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(QueryPlus.wrap(fullQuery)), "merged");

    List<ResultRow> allGranExpectedResults = Arrays.asList(
        makeRow(allGranQuery, "2011-04-02", "alias", "automotive", "rows", 2L, "idx", 269L),
        makeRow(allGranQuery, "2011-04-02", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(allGranQuery, "2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 319L),
        makeRow(allGranQuery, "2011-04-02", "alias", "health", "rows", 2L, "idx", 216L),
        makeRow(allGranQuery, "2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(allGranQuery, "2011-04-02", "alias", "news", "rows", 2L, "idx", 221L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        makeRow(allGranQuery, "2011-04-02", "alias", "technology", "rows", 2L, "idx", 177L),
        makeRow(allGranQuery, "2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    TestHelper.assertExpectedObjects(
        allGranExpectedResults,
        mergedRunner.run(QueryPlus.wrap(allGranQuery)),
        "merged"
    );
  }

  @Test
  public void testMergeResultsWithLimitAndOffset()
  {
    for (int limit = 1; limit < 20; ++limit) {
      for (int offset = 0; offset < 21; ++offset) {
        doTestMergeResultsWithValidLimit(limit, offset);
      }
    }
  }

  private void doTestMergeResultsWithValidLimit(final int limit, final int offset)
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimitSpec(DefaultLimitSpec.builder().limit(limit).offset(offset).build());

    final GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            2L,
            "idx",
            269L
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            2L,
            "idx",
            217L
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            2L,
            "idx",
            319L
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            2L,
            "idx",
            216L
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "idx",
            4420L
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            2L,
            "idx",
            221L
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "idx",
            4416L
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            2L,
            "idx",
            177L
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            2L,
            "idx",
            243L
        )
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);

    TestHelper.assertExpectedObjects(
        Iterables.limit(Iterables.skip(expectedResults, offset), limit),
        mergeRunner.run(QueryPlus.wrap(fullQuery)),
        StringUtils.format("limit: %d", limit)
    );
  }

  @Test
  public void testMergeResultsAcrossMultipleDaysWithLimitAndOrderBy()
  {
    final int limit = 14;
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(Granularities.DAY)
        .setLimit(limit)
        .addOrderByColumn("idx", OrderByColumnSpec.Direction.DESCENDING);

    GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(fullQuery, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(fullQuery, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(fullQuery, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        makeRow(fullQuery, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(fullQuery, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(fullQuery, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(fullQuery, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),
        makeRow(fullQuery, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(fullQuery, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),

        makeRow(fullQuery, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        makeRow(fullQuery, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(fullQuery, "2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        makeRow(fullQuery, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        makeRow(fullQuery, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit),
        mergeRunner.run(QueryPlus.wrap(fullQuery)),
        StringUtils.format("limit: %d", limit)
    );
  }

  @Test
  public void testMergeResultsAcrossMultipleDaysWithLimitAndOrderByUsingMathExpressions()
  {
    final int limit = 14;
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "expr",
                "index * 2 + indexMin / 10",
                ColumnType.FLOAT,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "expr"))
        .setGranularity(Granularities.DAY)
        .setLimit(limit)
        .addOrderByColumn("idx", OrderByColumnSpec.Direction.DESCENDING);

    GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(fullQuery, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 6090L),
        makeRow(fullQuery, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 6030L),
        makeRow(fullQuery, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 333L),
        makeRow(fullQuery, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 285L),
        makeRow(fullQuery, "2011-04-01", "alias", "news", "rows", 1L, "idx", 255L),
        makeRow(fullQuery, "2011-04-01", "alias", "health", "rows", 1L, "idx", 252L),
        makeRow(fullQuery, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 251L),
        makeRow(fullQuery, "2011-04-01", "alias", "business", "rows", 1L, "idx", 248L),
        makeRow(fullQuery, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 165L),

        makeRow(fullQuery, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 5262L),
        makeRow(fullQuery, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 5141L),
        makeRow(fullQuery, "2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 348L),
        makeRow(fullQuery, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 309L),
        makeRow(fullQuery, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 265L)
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit),
        mergeRunner.run(QueryPlus.wrap(fullQuery)),
        StringUtils.format("limit: %d", limit)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeResultsWithNegativeLimit()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(-1);

    builder.build();
  }

  @Test
  public void testMergeResultsWithOrderBy()
  {
    LimitSpec[] orderBySpecs = new LimitSpec[]{
        new DefaultLimitSpec(OrderByColumnSpec.ascending("idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.ascending("rows", "idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.descending("idx"), null),
        new DefaultLimitSpec(OrderByColumnSpec.descending("rows", "idx"), null),
        };

    GroupByQuery baseQuery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .build();

    List<ResultRow> allResults = Arrays.asList(
        makeRow(baseQuery, "2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        makeRow(baseQuery, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(baseQuery, "2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        makeRow(baseQuery, "2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        makeRow(baseQuery, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(baseQuery, "2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        makeRow(baseQuery, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        makeRow(baseQuery, "2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        makeRow(baseQuery, "2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    final int idxPosition = baseQuery.getResultRowSignature().indexOf("idx");
    final int rowsPosition = baseQuery.getResultRowSignature().indexOf("rows");

    Comparator<ResultRow> idxComparator = Comparator.comparing(row -> ((Number) row.get(idxPosition)).floatValue());
    Comparator<ResultRow> rowsComparator = Comparator.comparing(row -> ((Number) row.get(rowsPosition)).floatValue());
    Comparator<ResultRow> rowsIdxComparator = Ordering.from(rowsComparator).thenComparing(idxComparator);

    List<List<ResultRow>> expectedResults = Lists.newArrayList(
        Ordering.from(idxComparator).sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).sortedCopy(allResults),
        Ordering.from(idxComparator).reverse().sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).reverse().sortedCopy(allResults)
    );

    for (int i = 0; i < orderBySpecs.length; ++i) {
      doTestMergeResultsWithOrderBy(baseQuery, orderBySpecs[i], expectedResults.get(i));
    }
  }

  private void doTestMergeResultsWithOrderBy(
      GroupByQuery baseQuery,
      LimitSpec limitSpec,
      List<ResultRow> expectedResults
  )
  {
    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<ResultRow>()
        {
          @Override
          public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
                )
            );
            final QueryPlus queryPlus2 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
                )
            );
            return new MergeSequence(
                queryPlus.getQuery().getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(queryPlus1, responseContext), runner.run(queryPlus2, responseContext))
                )
            );
          }
        }
    );

    final GroupByQuery query = baseQuery.withLimitSpec(limitSpec);
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(QueryPlus.wrap(query)), "merged");
  }

  @Test
  public void testGroupByOrderLimit()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn("rows")
        .addOrderByColumn("alias", OrderByColumnSpec.Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L)
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query)), "no-limit");

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build())),
        "limited"
    );

    // Now try it with an expression based aggregator.
    List<AggregatorFactory> aggregatorSpecs = Arrays.asList(
        QueryRunnerTestHelper.ROWS_COUNT,
        new DoubleSumAggregatorFactory("idx", null, "index / 2 + indexMin", TestExprMacroTable.INSTANCE)
    );
    builder.setLimit(Integer.MAX_VALUE).setAggregatorSpecs(aggregatorSpecs);

    expectedResults = makeRows(
        builder.build(),
        new String[]{"__time", "alias", "rows", "idx"},
        new Object[]{"2011-04-01", "travel", 2L, 365.4876403808594D},
        new Object[]{"2011-04-01", "technology", 2L, 267.3737487792969D},
        new Object[]{"2011-04-01", "news", 2L, 333.3147277832031D},
        new Object[]{"2011-04-01", "health", 2L, 325.467529296875D},
        new Object[]{"2011-04-01", "entertainment", 2L, 479.916015625D},
        new Object[]{"2011-04-01", "business", 2L, 328.083740234375D},
        new Object[]{"2011-04-01", "automotive", 2L, 405.5966796875D},
        new Object[]{"2011-04-01", "premium", 6L, 6627.927734375D},
        new Object[]{"2011-04-01", "mezzanine", 6L, 6635.47998046875D}
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        mergeRunner.run(QueryPlus.wrap(builder.build())),
        "no-limit"
    );
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build())),
        "limited"
    );

    // Now try it with an expression virtual column.
    ExpressionVirtualColumn expressionVirtualColumn = new ExpressionVirtualColumn(
        "expr",
        "index / 2 + indexMin",
        ColumnType.FLOAT,
        TestExprMacroTable.INSTANCE
    );
    List<AggregatorFactory> aggregatorSpecs2 = Arrays.asList(
        QueryRunnerTestHelper.ROWS_COUNT,
        new DoubleSumAggregatorFactory("idx", "expr")
    );
    builder.setLimit(Integer.MAX_VALUE).setVirtualColumns(expressionVirtualColumn).setAggregatorSpecs(aggregatorSpecs2);

    TestHelper.assertExpectedObjects(
        expectedResults,
        mergeRunner.run(QueryPlus.wrap(builder.build())),
        "no-limit"
    );
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build())),
        "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit2()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn("rows", OrderByColumnSpec.Direction.DESCENDING)
        .addOrderByColumn("alias", OrderByColumnSpec.Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L)
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query)), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build())),
        "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit3()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new DoubleSumAggregatorFactory("idx", "index"))
        .addOrderByColumn("idx", OrderByColumnSpec.Direction.DESCENDING)
        .addOrderByColumn("alias", OrderByColumnSpec.Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    GroupByQuery query = builder.build();

    List<ResultRow> expectedResults = makeRows(
        query,
        new String[]{"__time", "alias", "rows", "idx"},
        new Object[]{"2011-04-01", "mezzanine", 6L, 4423.6533203125D},
        new Object[]{"2011-04-01", "premium", 6L, 4418.61865234375D},
        new Object[]{"2011-04-01", "entertainment", 2L, 319.94403076171875D},
        new Object[]{"2011-04-01", "automotive", 2L, 270.3977966308594D},
        new Object[]{"2011-04-01", "travel", 2L, 243.65843200683594D},
        new Object[]{"2011-04-01", "news", 2L, 222.20980834960938D},
        new Object[]{"2011-04-01", "business", 2L, 218.7224884033203D},
        new Object[]{"2011-04-01", "health", 2L, 216.97836303710938D},
        new Object[]{"2011-04-01", "technology", 2L, 178.24917602539062D}
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query)), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build())),
        "limited"
    );
  }

  @Test
  public void testGroupByOrderLimitNumeric()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "rows",
            OrderByColumnSpec.Direction.DESCENDING,
            StringComparators.NUMERIC
        ))
        .addOrderByColumn(new OrderByColumnSpec(
            "alias",
            OrderByColumnSpec.Direction.ASCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query)), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build())),
        "limited"
    );
  }

  @Test
  public void testGroupByWithSameCaseOrdering()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("marketalias", OrderByColumnSpec.Direction.DESCENDING)),
                3
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "upfront",
            "rows",
            186L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "total_market",
            "rows",
            186L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "spot",
            "rows",
            837L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithOrderLimit4()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.MARKET_DIMENSION
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.MARKET_DIMENSION,
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                3
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "1970-01-01T00:00:00.000Z", "market", "upfront", "rows", 186L),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "total_market",
            "rows",
            186L
        ),
        makeRow(query, "1970-01-01T00:00:00.000Z", "market", "spot", "rows", 837L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithOrderOnHyperUnique()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.MARKET_DIMENSION
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(QueryRunnerTestHelper.UNIQUE_METRIC, OrderByColumnSpec.Direction.DESCENDING)
                ),
                3
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.QUALITY_UNIQUES)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                    QueryRunnerTestHelper.UNIQUE_METRIC
                )
            )
        )
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.UNIQUE_METRIC,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
            QueryRunnerTestHelper.UNIQUES_9
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "total_market",
            QueryRunnerTestHelper.UNIQUE_METRIC,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
            QueryRunnerTestHelper.UNIQUES_2
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "upfront",
            QueryRunnerTestHelper.UNIQUE_METRIC,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
            QueryRunnerTestHelper.UNIQUES_2
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithHavingOnHyperUnique()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.MARKET_DIMENSION
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(QueryRunnerTestHelper.UNIQUE_METRIC, OrderByColumnSpec.Direction.DESCENDING)
                ),
                3
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(QueryRunnerTestHelper.UNIQUE_METRIC, 8))
        .setAggregatorSpecs(QueryRunnerTestHelper.QUALITY_UNIQUES)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                    QueryRunnerTestHelper.UNIQUE_METRIC
                )
            )
        )
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.UNIQUE_METRIC,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithHavingOnFinalizedHyperUnique()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.MARKET_DIMENSION
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                3
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC, 8))
        .setAggregatorSpecs(QueryRunnerTestHelper.QUALITY_UNIQUES)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                    QueryRunnerTestHelper.UNIQUE_METRIC
                )
            )
        )
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.UNIQUE_METRIC,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithLimitOnFinalizedHyperUnique()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.MARKET_DIMENSION
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                3
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.QUALITY_UNIQUES)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                    QueryRunnerTestHelper.UNIQUE_METRIC
                )
            )
        )
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.UNIQUE_METRIC,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
            QueryRunnerTestHelper.UNIQUES_9
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "total_market",
            QueryRunnerTestHelper.UNIQUE_METRIC,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
            QueryRunnerTestHelper.UNIQUES_2
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "market",
            "upfront",
            QueryRunnerTestHelper.UNIQUE_METRIC,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
            QueryRunnerTestHelper.UNIQUES_2
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithAlphaNumericDimensionOrder()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    Map<String, String> map = new HashMap<>();
    map.put("automotive", "health105");
    map.put("business", "health20");
    map.put("entertainment", "travel47");
    map.put("health", "health55");
    map.put("mezzanine", "health09");
    map.put("news", "health0000");
    map.put("premium", "health999");
    map.put("technology", "travel123");
    map.put("travel", "travel555");

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", null, StringComparators.ALPHANUMERIC)),
                null
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "health0000", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "health09", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "health20", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "health55", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "health105", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "health999", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "travel47", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "travel123", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel555", "rows", 1L, "idx", 119L),
        makeRow(query, "2011-04-02", "alias", "health0000", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "health09", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "health20", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "health55", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "health105", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "health999", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "travel47", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "travel123", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel555", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "alphanumeric-dimension-order");
  }

  @Test
  public void testGroupByWithLookupAndLimitAndSortByDimsFirst()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    Map<String, String> map = new HashMap<>();
    map.put("automotive", "9");
    map.put("business", "8");
    map.put("entertainment", "7");
    map.put("health", "6");
    map.put("mezzanine", "5");
    map.put("news", "4");
    map.put("premium", "3");
    map.put("technology", "2");
    map.put("travel", "1");

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", null, StringComparators.ALPHANUMERIC)),
                11
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .overrideContext(ImmutableMap.of("sortByDimsFirst", true))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "1", "rows", 1L, "idx", 119L),
        makeRow(query, "2011-04-02", "alias", "1", "rows", 1L, "idx", 126L),

        makeRow(query, "2011-04-01", "alias", "2", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-02", "alias", "2", "rows", 1L, "idx", 97L),

        makeRow(query, "2011-04-01", "alias", "3", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-02", "alias", "3", "rows", 3L, "idx", 2505L),

        makeRow(query, "2011-04-01", "alias", "4", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-02", "alias", "4", "rows", 1L, "idx", 114L),

        makeRow(query, "2011-04-01", "alias", "5", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-02", "alias", "5", "rows", 3L, "idx", 2447L),

        makeRow(query, "2011-04-01", "alias", "6", "rows", 1L, "idx", 120L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "lookup-limit");
  }

  @Ignore
  @Test
  // This is a test to verify per limit groupings, but Druid currently does not support this functionality. At a point
  // in time when Druid does support this, we can re-evaluate this test.
  public void testLimitPerGrouping()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.MARKET_DIMENSION
        ))
        .setInterval(QueryRunnerTestHelper.FIRST_TO_THIRD)
        // Using a limitSpec here to achieve a per group limit is incorrect.
        // Limit is applied on the overall results.
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("rows", OrderByColumnSpec.Direction.DESCENDING)),
                2
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01T00:00:00.000Z", "market", "spot", "rows", 9L),
        makeRow(query, "2011-04-02T00:00:00.000Z", "market", "spot", "rows", 9L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Iterator resultsIter = results.iterator();
    Iterator expectedResultsIter = expectedResults.iterator();

    final Object next1 = resultsIter.next();
    Object expectedNext1 = expectedResultsIter.next();
    Assert.assertEquals("order-limit", expectedNext1, next1);

    final Object next2 = resultsIter.next();
    Object expectedNext2 = expectedResultsIter.next();
    Assert.assertNotEquals("order-limit", expectedNext2, next2);
  }

  @Test
  public void testPostAggMergedHavingSpec()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("index", "index"))
        .setPostAggregatorSpecs(ImmutableList.of(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(new GreaterThanHavingSpec(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC, 1000L))
            )
        );

    final GroupByQuery fullQuery = builder.build();
    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "index",
            4420L,
            QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC,
            (double) (6L + 4420L + 1L)
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "index",
            4416L,
            QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC,
            (double) (6L + 4416L + 1L)
        )
    );

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<ResultRow>()
        {
          @Override
          public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
                )
            );
            final QueryPlus queryPlus2 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
                )
            );
            return new MergeSequence(
                queryPlus.getQuery().getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(queryPlus1, responseContext), runner.run(queryPlus2, responseContext))
                )
            );
          }
        }
    );

    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(QueryPlus.wrap(fullQuery)), "merged");
  }

  @Test
  public void testGroupByWithOrderLimitHavingSpec()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-01-25/2011-01-28")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT,
                            new DoubleSumAggregatorFactory("index", "index"),
                            QueryRunnerTestHelper.INDEX_LONG_MIN,
                            QueryRunnerTestHelper.INDEX_LONG_MAX,
                            QueryRunnerTestHelper.INDEX_DOUBLE_MIN,
                            QueryRunnerTestHelper.INDEX_DOUBLE_MAX,
                            QueryRunnerTestHelper.INDEX_FLOAT_MIN,
                            QueryRunnerTestHelper.INDEX_FLOAT_MAX
        )
        .setGranularity(Granularities.ALL)
        .setHavingSpec(new GreaterThanHavingSpec("index", 310L))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("index", OrderByColumnSpec.Direction.ASCENDING)),
                5
            )
        );

    GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(fullQuery,
                "2011-01-25",
                "alias",
                "business",
                "rows",
                3L,
                "index",
                312.38165283203125,
                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                101L,
                QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                105L,
                QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                101.624789D,
                QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                105.873942D,
                QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                101.62479F,
                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                105.87394F
        ),
        makeRow(fullQuery,
                "2011-01-25",
                "alias",
                "news",
                "rows",
                3L,
                "index",
                312.7834167480469,
                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                102L,
                QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                105L,
                QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                102.907866D,
                QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                105.266058D,
                QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                102.90787F,
                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                105.26606F
        ),
        makeRow(fullQuery,
                "2011-01-25",
                "alias",
                "technology",
                "rows",
                3L,
                "index",
                324.6412353515625,
                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                102L,
                QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                116L,
                QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                102.044542D,
                QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                116.979005D,
                QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                102.04454F,
                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                116.979004F
        ),
        makeRow(fullQuery,
                "2011-01-25",
                "alias",
                "travel",
                "rows",
                3L,
                "index",
                393.36322021484375,
                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                122L,
                QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                149L,
                QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                122.077247D,
                QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                149.125271D,
                QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                122.07725F,
                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                149.12527F
        ),
        makeRow(fullQuery,
                "2011-01-25",
                "alias",
                "health",
                "rows",
                3L,
                "index",
                511.2996826171875,
                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                159L,
                QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                180L,
                QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                159.988606D,
                QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                180.575246D,
                QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                159.9886F,
                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                180.57524F
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery);
    TestHelper.assertExpectedObjects(
        expectedResults,
        results,
        "order-limit-havingspec"
    );
  }

  @Test
  public void testPostAggHavingSpec()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("index", "index"))
        .setPostAggregatorSpecs(ImmutableList.of(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC, 1000L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "index",
            4420L,
            QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC,
            (double) (6L + 4420L + 1L)
        ),
        makeRow(
            fullQuery,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "index",
            4416L,
            QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC,
            (double) (6L + 4416L + 1L)
        )
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        "postagg-havingspec"
    );
  }


  @Test
  public void testHavingSpec()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec("rows", 2L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(fullQuery, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(fullQuery, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(fullQuery, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        "havingspec"
    );
  }

  @Test
  public void testDimFilterHavingSpec()
  {
    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(
        new AndDimFilter(
            ImmutableList.of(
                new OrDimFilter(
                    ImmutableList.of(
                        new BoundDimFilter("rows", "2", null, true, false, null, null, StringComparators.NUMERIC),
                        new SelectorDimFilter("idx", "217", null)
                    )
                ),
                new SelectorDimFilter("__time", String.valueOf(DateTimes.of("2011-04-01").getMillis()), null)
            )
        ),
        null
    );

    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"),
                            QueryRunnerTestHelper.INDEX_LONG_MIN, QueryRunnerTestHelper.INDEX_LONG_MAX,
                            QueryRunnerTestHelper.INDEX_DOUBLE_MIN, QueryRunnerTestHelper.INDEX_DOUBLE_MAX,
                            QueryRunnerTestHelper.INDEX_FLOAT_MIN, QueryRunnerTestHelper.INDEX_FLOAT_MAX
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(havingSpec);

    final GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(fullQuery,
                "2011-04-01",
                "alias",
                "business",
                "rows",
                2L,
                "idx",
                217L,
                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                105L,
                QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                112L,
                QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                105.735462D,
                QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                112.987027D,
                QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                105.73546F,
                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                112.98703F
        ),
        makeRow(fullQuery,
                "2011-04-01",
                "alias",
                "mezzanine",
                "rows",
                6L,
                "idx",
                4420L,
                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                107L,
                QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                1193L,
                QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                107.047773D,
                QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                1193.556278D,
                QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                107.047775F,
                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                1193.5563F
        ),
        makeRow(fullQuery,
                "2011-04-01",
                "alias",
                "premium",
                "rows",
                6L,
                "idx",
                4416L,
                QueryRunnerTestHelper.LONG_MIN_INDEX_METRIC,
                122L,
                QueryRunnerTestHelper.LONG_MAX_INDEX_METRIC,
                1321L,
                QueryRunnerTestHelper.DOUBLE_MIN_INDEX_METRIC,
                122.141707D,
                QueryRunnerTestHelper.DOUBLE_MAX_INDEX_METRIC,
                1321.375057D,
                QueryRunnerTestHelper.FLOAT_MIN_INDEX_METRIC,
                122.14171F,
                QueryRunnerTestHelper.FLOAT_MAX_INDEX_METRIC,
                1321.375F
        )
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        "dimfilter-havingspec"
    );
  }

  @Test
  public void testDimFilterHavingSpecWithExtractionFns()
  {
    String extractionJsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(
        extractionJsFn,
        false,
        JavaScriptConfig.getEnabledInstance()
    );

    String extractionJsFn2 = "function(num) { return num + 10; }";
    ExtractionFn extractionFn2 = new JavaScriptExtractionFn(
        extractionJsFn2,
        false,
        JavaScriptConfig.getEnabledInstance()
    );

    final DimFilterHavingSpec havingSpec = new DimFilterHavingSpec(
        new OrDimFilter(
            ImmutableList.of(
                new BoundDimFilter("rows", "12", null, true, false, null, extractionFn2, StringComparators.NUMERIC),
                new SelectorDimFilter("idx", "super-217", extractionFn)
            )
        ),
        null
    );

    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(havingSpec);

    final GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(fullQuery, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(fullQuery, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(fullQuery, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        "extractionfn-havingspec"
    );
  }

  @Test
  public void testMergedHavingSpec()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec("rows", 2L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    GroupByQuery fullQuery = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(fullQuery, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        makeRow(fullQuery, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(fullQuery, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<ResultRow>()
        {
          @Override
          public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
                )
            );
            final QueryPlus queryPlus2 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
                )
            );
            return new MergeSequence(
                queryPlus.getQuery().getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(queryPlus1, responseContext), runner.run(queryPlus2, responseContext))
                )
            );
          }
        }
    );

    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(QueryPlus.wrap(fullQuery)), "merged");
  }

  @Test
  public void testMergedPostAggHavingSpec()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new ArithmeticPostAggregator(
                    "rows_times_10",
                    "*",
                    Arrays.asList(
                        new FieldAccessPostAggregator(
                            "rows",
                            "rows"
                        ),
                        new ConstantPostAggregator(
                            "const",
                            10L
                        )
                    )
                )
            )
        )
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec("rows_times_10", 20L),
                    new EqualToHavingSpec("idx", 217L)
                )
            )
        );

    GroupByQuery query = builder.build();

    // Same query, but with expressions instead of arithmetic.
    final GroupByQuery expressionQuery = query.withPostAggregatorSpecs(
        Collections.singletonList(
            new ExpressionPostAggregator("rows_times_10", "rows * 10.0", null, null, TestExprMacroTable.INSTANCE)
        )
    );

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "business", "rows", 2L, "idx", 217L, "rows_times_10", 20.0),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L, "rows_times_10", 60.0),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L, "rows_times_10", 60.0)
    );

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<ResultRow>()
        {
          @Override
          public Sequence<ResultRow> run(QueryPlus<ResultRow> queryPlus, ResponseContext responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
                )
            );
            final QueryPlus queryPlus2 = queryPlus.withQuery(
                queryPlus.getQuery().withQuerySegmentSpec(
                    new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
                )
            );
            return new MergeSequence(
                queryPlus.getQuery().getResultOrdering(),
                Sequences.simple(
                    Arrays.asList(runner.run(queryPlus1, responseContext), runner.run(queryPlus2, responseContext))
                )
            );
          }
        }
    );

    ResponseContext context = ResponseContext.createEmpty();
    // add an extra layer of merging, simulate broker forwarding query to historical
    TestHelper.assertExpectedObjects(
        expectedResults,
        factory.getToolchest().postMergeQueryDecoration(
            factory.getToolchest().mergeResults(
                factory.getToolchest().preMergeQueryDecoration(mergedRunner)
            )
        ).run(QueryPlus.wrap(query)),
        "merged"
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        factory.getToolchest().postMergeQueryDecoration(
            factory.getToolchest().mergeResults(
                factory.getToolchest().preMergeQueryDecoration(mergedRunner)
            )
        ).run(QueryPlus.wrap(expressionQuery)),
        "merged"
    );
  }

  @Test
  public void testCustomAggregatorHavingSpec()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new TestBigDecimalSumAggregatorFactory("idxDouble", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new EqualToHavingSpec("rows", 3L),
                    new GreaterThanHavingSpec("idxDouble", 135.00d)
                )
            )
        )
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idxDouble", 135.885094d),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 1L, "idxDouble", 158.747224d),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idxDouble", 2871.8866900000003d),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idxDouble", 2900.798647d),
        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idxDouble", 147.425935d),
        makeRow(query, "2011-04-02", "alias", "entertainment", "rows", 1L, "idxDouble", 166.016049d),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idxDouble", 2448.830613d),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idxDouble", 2506.415148d)
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query),
        "custom-havingspec"
    );
  }

  @Test
  public void testGroupByWithRegEx()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimFilter(new RegexDimFilter("quality", "auto.*", null))
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "2011-04-01", "quality", "automotive", "rows", 2L)
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query)), "no-limit");
  }

  @Test
  public void testGroupByWithNonexistentDimension()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .addDimension("billy")
        .addDimension("quality").setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "billy",
            null,
            "quality",
            "automotive",
            "rows",
            2L
        ),
        makeRow(query, "2011-04-01", "billy", null, "quality", "business", "rows", 2L),
        makeRow(
            query,
            "2011-04-01",
            "billy",
            null,
            "quality",
            "entertainment",
            "rows",
            2L
        ),
        makeRow(query, "2011-04-01", "billy", null, "quality", "health", "rows", 2L),
        makeRow(query, "2011-04-01", "billy", null, "quality", "mezzanine", "rows", 6L),
        makeRow(query, "2011-04-01", "billy", null, "quality", "news", "rows", 2L),
        makeRow(query, "2011-04-01", "billy", null, "quality", "premium", "rows", 6L),
        makeRow(
            query,
            "2011-04-01",
            "billy",
            null,
            "quality",
            "technology",
            "rows",
            2L
        ),
        makeRow(query, "2011-04-01", "billy", null, "quality", "travel", "rows", 2L)
    );

    QueryRunner<ResultRow> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query)), "no-limit");
  }

  // A subquery identical to the query should yield identical results
  @Test
  public void testIdenticalSubquery()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"), new LongSumAggregatorFactory("idx", "idx"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "sub-query");
  }

  @Test
  public void testSubqueryWithMultipleIntervalsInOuterQuery()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(
                    Intervals.of("2011-04-01T00:00:00.000Z/2011-04-01T23:58:00.000Z"),
                    Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                )
            )
        )
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"), new LongSumAggregatorFactory("idx", "idx"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-multiple-intervals");
  }

  @Test
  public void testSubqueryWithExtractionFnInOuterQuery()
  {
    //https://github.com/apache/druid/issues/2556

    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(
                    Intervals.of("2011-04-01T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                )
            )
        )
        .setDimensions(new ExtractionDimensionSpec("alias", "alias", new RegexDimExtractionFn("(a).*", true, "a")))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"), new LongSumAggregatorFactory("idx", "idx"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "a", "rows", 13L, "idx", 6619L),
        makeRow(query, "2011-04-02", "alias", "a", "rows", 13L, "idx", 5827L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-extractionfn");
  }

  @Test
  public void testDifferentGroupingSubquery()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new DoubleMaxAggregatorFactory("idx", "idx"),
            new DoubleMaxAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = makeRows(
        query,
        new String[]{"__time", "rows", "idx", "indexMaxPlusTen"},
        new Object[]{"2011-04-01", 9L, 2900.0, 2930.0},
        new Object[]{"2011-04-02", 9L, 2505.0, 2535.0}
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query), "subquery"
    );

    subquery = makeQueryBuilder(subquery)
        .setVirtualColumns(
            new ExpressionVirtualColumn("expr", "-index + 100", ColumnType.FLOAT, TestExprMacroTable.INSTANCE)
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "expr"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .build();
    query = (GroupByQuery) query.withDataSource(new QueryDataSource(subquery));

    expectedResults = makeRows(
        query,
        new String[]{"__time", "rows", "idx", "indexMaxPlusTen"},
        new Object[]{"2011-04-01", 9L, 21.0, 2930.0},
        new Object[]{"2011-04-02", 9L, 2.0, 2535.0}
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query), "subquery"
    );
  }

  @Test
  public void testDifferentGroupingSubqueryMultipleAggregatorsOnSameField()
  {
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
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "idx1", 2900.0, "idx2", 2900.0,
                "idx3", 5800.0, "idx4", 5800.0
        ),
        makeRow(query, "2011-04-02", "idx1", 2505.0, "idx2", 2505.0,
                "idx3", 5010.0, "idx4", 5010.0
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-multiple-aggs");
  }


  @Test
  public void testDifferentGroupingSubqueryWithFilter()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
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
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "idx", 2900.0),
        makeRow(query, "2011-04-02", "idx", 2505.0)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-filter");
  }

  @Test
  public void testDifferentIntervalSubquery()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.SECOND_ONLY)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "2011-04-02", "idx", 2505.0)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-different-intervals");
  }

  @Test
  public void testDoubleMeanQuery()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(Granularities.ALL)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(
            new DoubleMeanAggregatorFactory("meanOnDouble", "doubleNumericNull")
        )
        .build();

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Row result = Iterables.getOnlyElement(results).toMapBasedRow(query);
    if (NullHandling.replaceWithDefault()) {
      Assert.assertEquals(39.2307d, result.getMetric("meanOnDouble").doubleValue(), 0.0001d);
    } else {
      Assert.assertEquals(51.0d, result.getMetric("meanOnDouble").doubleValue(), 0.0001d);
    }
  }

  @Test
  public void testGroupByTimeExtractionNamedUnderUnderTime()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage(
        "'__time' cannot be used as an output name for dimensions, aggregators, or post-aggregators."
    );

    makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(
            new DefaultDimensionSpec("market", "market"),
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                ColumnHolder.TIME_COLUMN_NAME,
                new TimeFormatExtractionFn("EEEE", null, null, null, false)
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, QueryRunnerTestHelper.INDEX_DOUBLE_SUM)
        .setPostAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimFilter(
            new OrDimFilter(
                Arrays.asList(
                    new SelectorDimFilter("market", "spot", null),
                    new SelectorDimFilter("market", "upfront", null)
                )
            )
        )
        .setLimitSpec(new DefaultLimitSpec(ImmutableList.of(), 1))
        .build();
  }

  @Test
  public void testGroupByWithUnderUnderTimeAsDimensionNameWithHavingAndLimit()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage(
        "'__time' cannot be used as an output name for dimensions, aggregators, or post-aggregators."
    );

    makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "__time"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new DimensionSelectorHavingSpec("__time", "automotive", null),
                    new DimensionSelectorHavingSpec("__time", "business", null)
                )
            )
        )
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(new OrderByColumnSpec("__time", OrderByColumnSpec.Direction.DESCENDING)),
                null
            )
        )
        .build();
  }

  @Test
  public void testEmptySubquery()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.EMPTY_INTERVAL)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertFalse(results.iterator().hasNext());
  }

  @Test
  public void testSubqueryWithPostAggregators()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx_subagg", "index"))
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new ArithmeticPostAggregator(
                    "idx_subpostagg", "+", Arrays.asList(
                    new FieldAccessPostAggregator("the_idx_subagg", "idx_subagg"),
                    new ConstantPostAggregator("thousand", 1000)
                )
                )

            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
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
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx_post",
            11135.0,
            "idx",
            1135L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx_post",
            11118.0,
            "idx",
            1118L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx_post",
            13870.0,
            "idx",
            3870L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            3L,
            "idx_post",
            13900.0,
            "idx",
            3900L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx_post",
            11147.0,
            "idx",
            1147L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "rows",
            1L,
            "idx_post",
            11112.0,
            "idx",
            1112L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11166.0,
            "idx",
            1166L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11113.0,
            "idx",
            1113L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx_post",
            13447.0,
            "idx",
            3447L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11114.0,
            "idx",
            1114L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            3L,
            "idx_post",
            13505.0,
            "idx",
            3505L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11097.0,
            "idx",
            1097L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11126.0,
            "idx",
            1126L
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-postaggs");
  }

  @Test
  public void testSubqueryWithPostAggregatorsAndHaving()
  {
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
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx_post",
            11135.0,
            "idx",
            1135L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx_post",
            11118.0,
            "idx",
            1118L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx_post",
            11147.0,
            "idx",
            1147L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "rows",
            1L,
            "idx_post",
            11112.0,
            "idx",
            1112L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11166.0,
            "idx",
            1166L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11113.0,
            "idx",
            1113L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx_post",
            13447.0,
            "idx",
            3447L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11114.0,
            "idx",
            1114L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            3L,
            "idx_post",
            13505.0,
            "idx",
            3505L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11097.0,
            "idx",
            1097L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11126.0,
            "idx",
            1126L
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-postaggs");
  }

  @Test
  public void testSubqueryWithMultiColumnAggregators()
  {
    // Cannot vectorize due to javascript functionality.
    cannotVectorize();

    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "market",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new DoubleSumAggregatorFactory("idx_subagg", "index"),
            new JavaScriptAggregatorFactory(
                "js_agg",
                Arrays.asList("index", "market"),
                "function(current, index, dim){return current + index + dim.length;}",
                "function(){return 0;}",
                "function(a,b){return a + b;}",
                JavaScriptConfig.getEnabledInstance()
            )
        )
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
              public byte[] getCacheKey()
              {
                return new byte[0];
              }

              @Override
              public void setQuery(GroupByQuery query)
              {
                this.query = query;
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
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx_subpostagg"),
            new DoubleSumAggregatorFactory("js_outer_agg", "js_agg")
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
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(
                        "alias",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                5
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx_post",
            11119.0,
            "idx",
            1119L,
            "js_outer_agg",
            123.92274475097656
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx_post",
            11078.0,
            "idx",
            1078L,
            "js_outer_agg",
            82.62254333496094
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx_post",
            11121.0,
            "idx",
            1121L,
            "js_outer_agg",
            125.58358001708984
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx_post",
            11120.0,
            "idx",
            1120L,
            "js_outer_agg",
            124.13470458984375
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx_post",
            11158.0,
            "idx",
            1158L,
            "js_outer_agg",
            162.74722290039062
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-multi-aggs");
  }

  @Test
  public void testSubqueryWithOuterFilterAggregator()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final DimFilter filter = new SelectorDimFilter("market", "spot", null);
    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(Collections.emptyList())
        .setAggregatorSpecs(new FilteredAggregatorFactory(QueryRunnerTestHelper.ROWS_COUNT, filter))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "1970-01-01", "rows", 837L)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-filter-agg");
  }

  @Test
  public void testSubqueryWithOuterTimeFilter()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final DimFilter fridayFilter = new SelectorDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        "Friday",
        new TimeFormatExtractionFn("EEEE", null, null, null, false)
    );
    final DimFilter firstDaysFilter = new InDimFilter(
        ColumnHolder.TIME_COLUMN_NAME,
        ImmutableList.of("1", "2", "3"),
        new TimeFormatExtractionFn("d", null, null, null, false)
    );
    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(Collections.emptyList())
        .setDimFilter(firstDaysFilter)
        .setAggregatorSpecs(new FilteredAggregatorFactory(QueryRunnerTestHelper.ROWS_COUNT, fridayFilter))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-02-01", "rows", 0L),
        makeRow(query, "2011-02-02", "rows", 0L),
        makeRow(query, "2011-02-03", "rows", 0L),
        makeRow(query, "2011-03-01", "rows", 0L),
        makeRow(query, "2011-03-02", "rows", 0L),
        makeRow(query, "2011-03-03", "rows", 0L),
        makeRow(query, "2011-04-01", "rows", 13L),
        makeRow(query, "2011-04-02", "rows", 0L),
        makeRow(query, "2011-04-03", "rows", 0L)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-time-filter");
  }

  @Test
  public void testSubqueryWithContextTimeout()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ArrayList<>()).setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .overrideContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 10000))
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "2011-04-01", "count", 18L)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-timeout");
  }

  @Test
  public void testSubqueryWithOuterVirtualColumns()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn("expr", "1", ColumnType.FLOAT, TestExprMacroTable.INSTANCE))
        .setDimensions(new ArrayList<>()).setAggregatorSpecs(new LongSumAggregatorFactory("count", "expr"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "2011-04-01", "count", 18L)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-virtual");
  }

  @Test
  public void testSubqueryWithOuterCardinalityAggregator()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(Collections.emptyList())
        .setAggregatorSpecs(new CardinalityAggregatorFactory(
            "car",
            ImmutableList.of(new DefaultDimensionSpec("quality", "quality")),
            false
        ))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "1970-01-01", "car", QueryRunnerTestHelper.UNIQUES_9)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-cardinality");
  }

  @Test
  public void testSubqueryWithOuterCountAggregator()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.ASCENDING)),
                null
            )
        )
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ArrayList<>()).setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "2011-04-01", "count", 18L)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-count-agg");
  }

  @Test
  public void testSubqueryWithOuterDimJavascriptAggregators()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(new JavaScriptAggregatorFactory(
            "js_agg",
            Arrays.asList("index", "market"),
            "function(current, index, dim){return current + index + dim.length;}",
            "function(){return 0;}",
            "function(a,b){return a + b;}",
            JavaScriptConfig.getEnabledInstance()
        ))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "quality", "automotive", "js_agg", 139D),
        makeRow(query, "2011-04-01", "quality", "business", "js_agg", 122D),
        makeRow(query, "2011-04-01", "quality", "entertainment", "js_agg", 162D),
        makeRow(query, "2011-04-01", "quality", "health", "js_agg", 124D),
        makeRow(query, "2011-04-01", "quality", "mezzanine", "js_agg", 2893D),
        makeRow(query, "2011-04-01", "quality", "news", "js_agg", 125D),
        makeRow(query, "2011-04-01", "quality", "premium", "js_agg", 2923D),
        makeRow(query, "2011-04-01", "quality", "technology", "js_agg", 82D),
        makeRow(query, "2011-04-01", "quality", "travel", "js_agg", 123D),

        makeRow(query, "2011-04-02", "quality", "automotive", "js_agg", 151D),
        makeRow(query, "2011-04-02", "quality", "business", "js_agg", 116D),
        makeRow(query, "2011-04-02", "quality", "entertainment", "js_agg", 170D),
        makeRow(query, "2011-04-02", "quality", "health", "js_agg", 117D),
        makeRow(query, "2011-04-02", "quality", "mezzanine", "js_agg", 2470D),
        makeRow(query, "2011-04-02", "quality", "news", "js_agg", 118D),
        makeRow(query, "2011-04-02", "quality", "premium", "js_agg", 2528D),
        makeRow(query, "2011-04-02", "quality", "technology", "js_agg", 101D),
        makeRow(query, "2011-04-02", "quality", "travel", "js_agg", 130D)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-javascript");
  }

  @Test
  public void testSubqueryWithOuterJavascriptAggregators()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(new JavaScriptAggregatorFactory(
            "js_agg",
            Arrays.asList("index", "rows"),
            "function(current, index, rows){return current + index + rows;}",
            "function(){return 0;}",
            "function(a,b){return a + b;}",
            JavaScriptConfig.getEnabledInstance()
        ))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "quality", "automotive", "js_agg", 136D),
        makeRow(query, "2011-04-01", "quality", "business", "js_agg", 119D),
        makeRow(query, "2011-04-01", "quality", "entertainment", "js_agg", 159D),
        makeRow(query, "2011-04-01", "quality", "health", "js_agg", 121D),
        makeRow(query, "2011-04-01", "quality", "mezzanine", "js_agg", 2873D),
        makeRow(query, "2011-04-01", "quality", "news", "js_agg", 122D),
        makeRow(query, "2011-04-01", "quality", "premium", "js_agg", 2903D),
        makeRow(query, "2011-04-01", "quality", "technology", "js_agg", 79D),
        makeRow(query, "2011-04-01", "quality", "travel", "js_agg", 120D),

        makeRow(query, "2011-04-02", "quality", "automotive", "js_agg", 148D),
        makeRow(query, "2011-04-02", "quality", "business", "js_agg", 113D),
        makeRow(query, "2011-04-02", "quality", "entertainment", "js_agg", 167D),
        makeRow(query, "2011-04-02", "quality", "health", "js_agg", 114D),
        makeRow(query, "2011-04-02", "quality", "mezzanine", "js_agg", 2450D),
        makeRow(query, "2011-04-02", "quality", "news", "js_agg", 115D),
        makeRow(query, "2011-04-02", "quality", "premium", "js_agg", 2508D),
        makeRow(query, "2011-04-02", "quality", "technology", "js_agg", 98D),
        makeRow(query, "2011-04-02", "quality", "travel", "js_agg", 127D)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-javascript");
  }

  @Test
  public void testSubqueryWithHyperUniques()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new HyperUniquesAggregatorFactory("quality_uniques", "quality_uniques")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx"),
            new HyperUniquesAggregatorFactory("uniq", "quality_uniques")
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
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
            282L,
            "uniq",
            1.0002442201269182
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            2L,
            "idx",
            230L,
            "uniq",
            1.0002442201269182
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            2L,
            "idx",
            324L,
            "uniq",
            1.0002442201269182
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            2L,
            "idx",
            233L,
            "uniq",
            1.0002442201269182
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "idx",
            5317L,
            "uniq",
            1.0002442201269182
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            2L,
            "idx",
            235L,
            "uniq",
            1.0002442201269182
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "idx",
            5405L,
            "uniq",
            1.0002442201269182
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            2L,
            "idx",
            175L,
            "uniq",
            1.0002442201269182
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            2L,
            "idx",
            245L,
            "uniq",
            1.0002442201269182
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-hyperunique");
  }

  @Test
  public void testSubqueryWithHyperUniquesPostAggregator()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ArrayList<>())
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new HyperUniquesAggregatorFactory("quality_uniques_inner", "quality_uniques")
        )
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new FieldAccessPostAggregator("quality_uniques_inner_post", "quality_uniques_inner")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ArrayList<>())
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx"),
            new HyperUniquesAggregatorFactory("quality_uniques_outer", "quality_uniques_inner_post")
        )
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator("quality_uniques_outer_post", "quality_uniques_outer")
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            26L,
            "idx",
            12446L,
            "quality_uniques_outer",
            9.019833517963864,
            "quality_uniques_outer_post",
            9.019833517963864
        )
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-hyperunique");
  }

  @Test
  public void testSubqueryWithFirstLast()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongFirstAggregatorFactory("innerfirst", "index", null),
            new LongLastAggregatorFactory("innerlast", "index", null)
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .overrideContext(ImmutableMap.of(QueryContexts.FINALIZE_KEY, true))
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(Collections.emptyList())
        .setAggregatorSpecs(
            new LongFirstAggregatorFactory("first", "innerfirst", null),
            new LongLastAggregatorFactory("last", "innerlast", null)
        )
        .setGranularity(QueryRunnerTestHelper.MONTH_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-01-01", "first", 100L, "last", 943L),
        makeRow(query, "2011-02-01", "first", 132L, "last", 1101L),
        makeRow(query, "2011-03-01", "first", 153L, "last", 1063L),
        makeRow(query, "2011-04-01", "first", 135L, "last", 780L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-firstlast");
  }

  @Test
  public void testGroupByWithSubtotalsSpecOfDimensionsPrefixes()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "alias",
            "quality",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        ))
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("market", "market2"),
            new DefaultDimensionSpec("alias", "alias2")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("market2"),
            ImmutableList.of()
        ))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "spot",
            "rows",
            9L,
            "idx",
            1102L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "total_market",
            "rows",
            2L,
            "idx",
            2836L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "upfront",
            "rows",
            2L,
            "idx",
            2681L
        ),

        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "spot",
            "rows",
            9L,
            "idx",
            1120L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "total_market",
            "rows",
            2L,
            "idx",
            2514L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "upfront",
            "rows",
            2L,
            "idx",
            2193L
        ),

        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "rows",
            13L,
            "idx",
            6619L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "rows",
            13L,
            "idx",
            5827L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal");
  }

  @Test
  public void testGroupByWithSubtotalsSpecGeneral()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "alias",
            "quality",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        ))
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "quality2"),
            new DefaultDimensionSpec("market", "market2"),
            new DefaultDimensionSpec("alias", "alias2")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new FieldAccessPostAggregator("idxPostAgg", "idx")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias2"),
            ImmutableList.of("market2"),
            ImmutableList.of()
        ))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "automotive",
            "rows",
            1L,
            "idx",
            135L,
            "idxPostAgg",
            135L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "business",
            "rows",
            1L,
            "idx",
            118L,
            "idxPostAgg",
            118L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "entertainment",
            "rows",
            1L,
            "idx",
            158L,
            "idxPostAgg",
            158L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "health",
            "rows",
            1L,
            "idx",
            120L,
            "idxPostAgg",
            120L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2870L,
            "idxPostAgg",
            2870L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "news",
            "rows",
            1L,
            "idx",
            121L,
            "idxPostAgg",
            121L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "premium",
            "rows",
            3L,
            "idx",
            2900L,
            "idxPostAgg",
            2900L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "technology",
            "rows",
            1L,
            "idx",
            78L,
            "idxPostAgg",
            78L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias2",
            "travel",
            "rows",
            1L,
            "idx",
            119L,
            "idxPostAgg",
            119L
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "automotive",
            "rows",
            1L,
            "idx",
            147L,
            "idxPostAgg",
            147L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "business",
            "rows",
            1L,
            "idx",
            112L,
            "idxPostAgg",
            112L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "entertainment",
            "rows",
            1L,
            "idx",
            166L,
            "idxPostAgg",
            166L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "health",
            "rows",
            1L,
            "idx",
            113L,
            "idxPostAgg",
            113L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2447L,
            "idxPostAgg",
            2447L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "news",
            "rows",
            1L,
            "idx",
            114L,
            "idxPostAgg",
            114L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "premium",
            "rows",
            3L,
            "idx",
            2505L,
            "idxPostAgg",
            2505L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "technology",
            "rows",
            1L,
            "idx",
            97L,
            "idxPostAgg",
            97L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias2",
            "travel",
            "rows",
            1L,
            "idx",
            126L,
            "idxPostAgg",
            126L
        ),

        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "spot",
            "rows",
            9L,
            "idx",
            1102L,
            "idxPostAgg",
            1102L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "total_market",
            "rows",
            2L,
            "idx",
            2836L,
            "idxPostAgg",
            2836L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "upfront",
            "rows",
            2L,
            "idx",
            2681L,
            "idxPostAgg",
            2681L
        ),

        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "spot",
            "rows",
            9L,
            "idx",
            1120L,
            "idxPostAgg",
            1120L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "total_market",
            "rows",
            2L,
            "idx",
            2514L,
            "idxPostAgg",
            2514L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "upfront",
            "rows",
            2L,
            "idx",
            2193L,
            "idxPostAgg",
            2193L
        ),

        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "rows",
            13L,
            "idx",
            6619L,
            "idxPostAgg",
            6619L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "rows",
            13L,
            "idx",
            5827L,
            "idxPostAgg",
            5827L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal");
  }

  // https://github.com/apache/druid/issues/7820
  @Test
  public void testGroupByWithSubtotalsSpecWithRenamedDimensionAndFilter()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(new ExpressionVirtualColumn(
            "alias",
            "quality",
            ColumnType.STRING,
            TestExprMacroTable.INSTANCE
        ))
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("market", "market"),
            new DefaultDimensionSpec("alias", "alias_renamed")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index"),
                new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
                new DoubleSumAggregatorFactory("idxDouble", "index")
            )
        )
        .setDimFilter(new SelectorDimFilter("alias", "automotive", null))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias_renamed"),
            ImmutableList.of()
        ))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias_renamed",
            "automotive",
            "rows",
            1L,
            "idx",
            135L,
            "idxFloat",
            135.88510131835938f,
            "idxDouble",
            135.88510131835938d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias_renamed",
            "automotive",
            "rows",
            1L,
            "idx",
            147L,
            "idxFloat",
            147.42593f,
            "idxDouble",
            147.42593d
        ),

        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "rows",
            1L,
            "idx",
            135L,
            "idxFloat",
            135.88510131835938f,
            "idxDouble",
            135.88510131835938d
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "rows",
            1L,
            "idx",
            147L,
            "idxFloat",
            147.42593f,
            "idxDouble",
            147.42593d
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal");
  }

  @Test
  public void testGroupByWithSubtotalsSpecWithLongDimensionColumn()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("qualityLong", "ql", ColumnType.LONG),
            new DefaultDimensionSpec("market", "market2")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("ql"),
            ImmutableList.of("market2"),
            ImmutableList.of()
        ))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1000L,
            "rows",
            1L,
            "idx",
            135L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1100L,
            "rows",
            1L,
            "idx",
            118L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1200L,
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1300L,
            "rows",
            1L,
            "idx",
            120L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1400L,
            "rows",
            3L,
            "idx",
            2870L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1500L,
            "rows",
            1L,
            "idx",
            121L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1600L,
            "rows",
            3L,
            "idx",
            2900L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1700L,
            "rows",
            1L,
            "idx",
            78L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "ql",
            1800L,
            "rows",
            1L,
            "idx",
            119L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1000L,
            "rows",
            1L,
            "idx",
            147L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1100L,
            "rows",
            1L,
            "idx",
            112L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1200L,
            "rows",
            1L,
            "idx",
            166L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1300L,
            "rows",
            1L,
            "idx",
            113L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1400L,
            "rows",
            3L,
            "idx",
            2447L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1500L,
            "rows",
            1L,
            "idx",
            114L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1600L,
            "rows",
            3L,
            "idx",
            2505L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1700L,
            "rows",
            1L,
            "idx",
            97L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "ql",
            1800L,
            "rows",
            1L,
            "idx",
            126L
        ),

        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "spot",
            "rows",
            9L,
            "idx",
            1102L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "total_market",
            "rows",
            2L,
            "idx",
            2836L
        ),
        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "market2",
            "upfront",
            "rows",
            2L,
            "idx",
            2681L
        ),

        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "spot",
            "rows",
            9L,
            "idx",
            1120L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "total_market",
            "rows",
            2L,
            "idx",
            2514L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "market2",
            "upfront",
            "rows",
            2L,
            "idx",
            2193L
        ),


        makeRow(
            query,
            "2011-04-01T00:00:00.000Z",
            "rows",
            13L,
            "idx",
            6619L
        ),
        makeRow(
            query,
            "2011-04-02T00:00:00.000Z",
            "rows",
            13L,
            "idx",
            5827L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal-long-dim");
  }

  @Test
  public void testGroupByWithSubtotalsSpecWithOrderLimit()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias"),
            ImmutableList.of("market"),
            ImmutableList.of()
        ))
        .setLimitSpec(DefaultLimitSpec.builder().limit(3).orderBy("idx", "alias", "market").build())
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal-order-limit");
  }

  @Test
  public void testGroupByWithSubtotalsSpecWithOrderLimitAndOffset()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias"),
            ImmutableList.of("market"),
            ImmutableList.of()
        ))
        .setLimitSpec(DefaultLimitSpec.builder().limit(2).offset(1).orderBy("idx", "alias", "market").build())
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal-order-limit");
  }


  @Test
  public void testGroupByWithSubtotalsSpecWithOrderLimitForcePushdown()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("placement", "placement"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.ROWS_COUNT,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("placement"),
            ImmutableList.of("market"),
            ImmutableList.of()
        ))
        .setLimitSpec(DefaultLimitSpec.builder().limit(25).orderBy("placement", "market").build())
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "placement", "preferred", "market", null, "rows", 13L, "idx", 6619L),
        makeRow(query, "2011-04-02", "placement", "preferred", "market", null, "rows", 13L, "idx", 5827L),
        makeRow(query, "2011-04-01", "placement", null, "market", "spot", "rows", 9L, "idx", 1102L),
        makeRow(query, "2011-04-01", "placement", null, "market", "total_market", "rows", 2L, "idx", 2836L),
        makeRow(query, "2011-04-01", "placement", null, "market", "upfront", "rows", 2L, "idx", 2681L),
        makeRow(query, "2011-04-02", "placement", null, "market", "spot", "rows", 9L, "idx", 1120L),
        makeRow(query, "2011-04-02", "placement", null, "market", "total_market", "rows", 2L, "idx", 2514L),
        makeRow(query, "2011-04-02", "placement", null, "market", "upfront", "rows", 2L, "idx", 2193L),
        makeRow(query, "2011-04-01", "placement", null, "market", null, "rows", 13L, "idx", 6619L),
        makeRow(query, "2011-04-02", "placement", null, "market", null, "rows", 13L, "idx", 5827L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal-order-limit");
  }

  @Test
  public void testGroupByWithTimeColumn()
  {
    // Cannot vectorize due to javascript aggregator.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            QueryRunnerTestHelper.JS_COUNT_IF_TIME_GREATER_THAN,
            QueryRunnerTestHelper.TIME_LONG_SUM
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            26L,
            "ntimestamps",
            13.0,
            "sumtime",
            33843139200000L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "time");
  }

  @Test
  public void testGroupByTimeExtraction()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(
            new DefaultDimensionSpec("market", "market"),
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null, null, false)
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, QueryRunnerTestHelper.INDEX_DOUBLE_SUM)
        .setPostAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimFilter(
            new OrDimFilter(
                Arrays.asList(
                    new SelectorDimFilter("market", "spot", null),
                    new SelectorDimFilter("market", "upfront", null)
                )
            )
        )
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Friday",
            "market",
            "spot",
            "index",
            13219.574157714844,
            "rows",
            117L,
            "addRowsIndexConstant",
            13337.574157714844
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Monday",
            "market",
            "spot",
            "index",
            13557.738830566406,
            "rows",
            117L,
            "addRowsIndexConstant",
            13675.738830566406
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Saturday",
            "market",
            "spot",
            "index",
            13493.751281738281,
            "rows",
            117L,
            "addRowsIndexConstant",
            13611.751281738281
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Sunday",
            "market",
            "spot",
            "index",
            13585.541015625,
            "rows",
            117L,
            "addRowsIndexConstant",
            13703.541015625
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Thursday",
            "market",
            "spot",
            "index",
            14279.127197265625,
            "rows",
            126L,
            "addRowsIndexConstant",
            14406.127197265625
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Tuesday",
            "market",
            "spot",
            "index",
            13199.471435546875,
            "rows",
            117L,
            "addRowsIndexConstant",
            13317.471435546875
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Wednesday",
            "market",
            "spot",
            "index",
            14271.368591308594,
            "rows",
            126L,
            "addRowsIndexConstant",
            14398.368591308594
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Friday",
            "market",
            "upfront",
            "index",
            27297.8623046875,
            "rows",
            26L,
            "addRowsIndexConstant",
            27324.8623046875
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Monday",
            "market",
            "upfront",
            "index",
            27619.58447265625,
            "rows",
            26L,
            "addRowsIndexConstant",
            27646.58447265625
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Saturday",
            "market",
            "upfront",
            "index",
            27820.83154296875,
            "rows",
            26L,
            "addRowsIndexConstant",
            27847.83154296875
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Sunday",
            "market",
            "upfront",
            "index",
            24791.223876953125,
            "rows",
            26L,
            "addRowsIndexConstant",
            24818.223876953125
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Thursday",
            "market",
            "upfront",
            "index",
            28562.748901367188,
            "rows",
            28L,
            "addRowsIndexConstant",
            28591.748901367188
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Tuesday",
            "market",
            "upfront",
            "index",
            26968.280639648438,
            "rows",
            26L,
            "addRowsIndexConstant",
            26995.280639648438
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Wednesday",
            "market",
            "upfront",
            "index",
            28985.5751953125,
            "rows",
            28L,
            "addRowsIndexConstant",
            29014.5751953125
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "time-extraction");
  }


  @Test
  public void testGroupByTimeExtractionWithNulls()
  {
    // Cannot vectorize due to extraction dimension specs.
    cannotVectorize();

    final DimExtractionFn nullWednesdays = new DimExtractionFn()
    {
      @Override
      public String apply(String dimValue)
      {
        if ("Wednesday".equals(dimValue)) {
          return null;
        } else {
          return dimValue;
        }
      }

      @Override
      public byte[] getCacheKey()
      {
        throw new UnsupportedOperationException();
      }

      @Override
      public boolean preservesOrdering()
      {
        return false;
      }

      @Override
      public ExtractionType getExtractionType()
      {
        return ExtractionType.MANY_TO_ONE;
      }
    };

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(
            new DefaultDimensionSpec("market", "market"),
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                "dayOfWeek",
                new CascadeExtractionFn(
                    new ExtractionFn[]{new TimeFormatExtractionFn("EEEE", null, null, null, false), nullWednesdays}
                )
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, QueryRunnerTestHelper.INDEX_DOUBLE_SUM)
        .setPostAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimFilter(
            new OrDimFilter(
                Arrays.asList(
                    new SelectorDimFilter("market", "spot", null),
                    new SelectorDimFilter("market", "upfront", null)
                )
            )
        )
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            null,
            "market",
            "spot",
            "index",
            14271.368591308594,
            "rows",
            126L,
            "addRowsIndexConstant",
            14398.368591308594
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Friday",
            "market",
            "spot",
            "index",
            13219.574157714844,
            "rows",
            117L,
            "addRowsIndexConstant",
            13337.574157714844
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Monday",
            "market",
            "spot",
            "index",
            13557.738830566406,
            "rows",
            117L,
            "addRowsIndexConstant",
            13675.738830566406
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Saturday",
            "market",
            "spot",
            "index",
            13493.751281738281,
            "rows",
            117L,
            "addRowsIndexConstant",
            13611.751281738281
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Sunday",
            "market",
            "spot",
            "index",
            13585.541015625,
            "rows",
            117L,
            "addRowsIndexConstant",
            13703.541015625
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Thursday",
            "market",
            "spot",
            "index",
            14279.127197265625,
            "rows",
            126L,
            "addRowsIndexConstant",
            14406.127197265625
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Tuesday",
            "market",
            "spot",
            "index",
            13199.471435546875,
            "rows",
            117L,
            "addRowsIndexConstant",
            13317.471435546875
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            null,
            "market",
            "upfront",
            "index",
            28985.5751953125,
            "rows",
            28L,
            "addRowsIndexConstant",
            29014.5751953125
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Friday",
            "market",
            "upfront",
            "index",
            27297.8623046875,
            "rows",
            26L,
            "addRowsIndexConstant",
            27324.8623046875
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Monday",
            "market",
            "upfront",
            "index",
            27619.58447265625,
            "rows",
            26L,
            "addRowsIndexConstant",
            27646.58447265625
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Saturday",
            "market",
            "upfront",
            "index",
            27820.83154296875,
            "rows",
            26L,
            "addRowsIndexConstant",
            27847.83154296875
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Sunday",
            "market",
            "upfront",
            "index",
            24791.223876953125,
            "rows",
            26L,
            "addRowsIndexConstant",
            24818.223876953125
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Thursday",
            "market",
            "upfront",
            "index",
            28562.748901367188,
            "rows",
            28L,
            "addRowsIndexConstant",
            28591.748901367188
        ),
        makeRow(
            query,
            "1970-01-01",
            "dayOfWeek",
            "Tuesday",
            "market",
            "upfront",
            "index",
            26968.280639648438,
            "rows",
            26L,
            "addRowsIndexConstant",
            26995.280639648438
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "time-extraction");
  }

  @Test
  public void testBySegmentResults()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, true));
    final GroupByQuery fullQuery = builder.build();

    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<>(
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass<>(
            Collections.singletonList(
                makeRow(
                    fullQuery,
                    "2011-04-01",
                    "alias",
                    "mezzanine",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ),
            QueryRunnerTestHelper.SEGMENT_ID.toString(),
            Intervals.of("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<ResultRow>> singleSegmentRunners = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(Executors.newCachedThreadPool(), singleSegmentRunners)),
            toolChest
        )
    );

    TestHelper.assertExpectedObjects(
        bySegmentResults,
        theRunner.run(QueryPlus.wrap(fullQuery)),
        "bySegment"
    );
    exec.shutdownNow();
  }


  @Test
  public void testBySegmentResultsUnOptimizedDimextraction()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04").setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(
                new MapLookupExtractor(ImmutableMap.of("mezzanine", "mezzanine0"), false),
                false,
                null,
                false,
                false
            )
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, true));
    final GroupByQuery fullQuery = builder.build();

    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<>(
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass<>(
            Collections.singletonList(
                makeRow(
                    fullQuery,
                    "2011-04-01",
                    "alias",
                    "mezzanine0",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ),
            QueryRunnerTestHelper.SEGMENT_ID.toString(),
            Intervals.of("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<ResultRow>> singleSegmentRunners = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(Executors.newCachedThreadPool(), singleSegmentRunners)),
            toolChest
        )
    );

    TestHelper.assertExpectedObjects(
        bySegmentResults,
        theRunner.run(QueryPlus.wrap(fullQuery)),
        "bySegment"
    );
    exec.shutdownNow();
  }

  @Test
  public void testBySegmentResultsOptimizedDimextraction()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04").setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(
                new MapLookupExtractor(ImmutableMap.of("mezzanine", "mezzanine0"), false),
                false,
                null,
                true,
                false
            )
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .overrideContext(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, true));
    final GroupByQuery fullQuery = builder.build();

    int segmentCount = 32;
    Result<BySegmentResultValue<ResultRow>> singleSegmentResult = new Result<>(
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass<>(
            Collections.singletonList(
                makeRow(
                    fullQuery,
                    "2011-04-01",
                    "alias",
                    "mezzanine0",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ),
            QueryRunnerTestHelper.SEGMENT_ID.toString(),
            Intervals.of("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<ResultRow>> singleSegmentRunners = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(Executors.newCachedThreadPool(), singleSegmentRunners)),
            toolChest
        )
    );

    TestHelper.assertExpectedObjects(
        bySegmentResults,
        theRunner.run(QueryPlus.wrap(fullQuery)),
        "bySegment-dim-extraction"
    );
    exec.shutdownNow();
  }

  // Extraction Filters testing

  @Test
  public void testGroupByWithExtractionDimFilter()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("automotive", "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put("business", "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put("mezzanine", "automotiveAndBusinessAndNewsAndMezzanine");
    extractionMap.put("news", "automotiveAndBusinessAndNewsAndMezzanine");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    List<DimFilter> dimFilters = Lists.newArrayList(
        new ExtractionDimFilter("quality", "automotiveAndBusinessAndNewsAndMezzanine", lookupExtractionFn, null),
        new SelectorDimFilter("quality", "entertainment", null),
        new SelectorDimFilter("quality", "health", null),
        new SelectorDimFilter("quality", "premium", null),
        new SelectorDimFilter("quality", "technology", null),
        new SelectorDimFilter("quality", "travel", null)
    );

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimFilter(new OrDimFilter(dimFilters))
        .build();
    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        makeRow(query, "2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        makeRow(query, "2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        makeRow(query, "2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        makeRow(query, "2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "dim-extraction");

  }

  @Test
  public void testGroupByWithExtractionDimFilterCaseMappingValueIsNullOrEmpty()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("automotive", "automotive0");
    extractionMap.put("business", "business0");
    extractionMap.put("entertainment", "entertainment0");
    extractionMap.put("health", "health0");
    extractionMap.put("mezzanine", null);
    extractionMap.put("news", "");
    extractionMap.put("premium", "premium0");
    extractionMap.put("technology", "technology0");
    extractionMap.put("travel", "travel0");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimFilter(new ExtractionDimFilter("quality", "", lookupExtractionFn, null))
        .build();

    List<ResultRow> expectedResults;

    if (NullHandling.replaceWithDefault()) {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
          makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
          makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
          makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L)
      );
    } else {
      // Only empty string should match, nulls will not match
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
          makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L)
      );
    }

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "dim-extraction");
  }

  @Test
  public void testGroupByWithExtractionDimFilterWhenSearchValueNotInTheMap()
  {
    Map<String, String> extractionMap = new HashMap<>();
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimFilter(
            new ExtractionDimFilter("quality", "NOT_THERE", lookupExtractionFn, null)
        )
        .build();
    List<ResultRow> expectedResults = Collections.emptyList();

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "dim-extraction");
  }


  @Test
  public void testGroupByWithExtractionDimFilterKeyisNull()
  {
    Map<String, String> extractionMap = new HashMap<>();


    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn;
    if (NullHandling.replaceWithDefault()) {
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);
      extractionMap.put("", "REPLACED_VALUE");
    } else {
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, "REPLACED_VALUE", true, false);
      extractionMap.put("", "NOT_USED");
    }

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimFilter(
            new ExtractionDimFilter(
                "null_column",
                "REPLACED_VALUE",
                lookupExtractionFn,
                null
            )
        ).build();

    List<ResultRow> expectedResults = Arrays
        .asList(
            makeRow(query, "2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
            makeRow(query, "2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
        );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "dim-extraction");
  }

  @Test
  public void testGroupByWithAggregatorFilterAndExtractionFunction()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("automotive", "automotive0");
    extractionMap.put("business", "business0");
    extractionMap.put("entertainment", "entertainment0");
    extractionMap.put("health", "health0");
    extractionMap.put("mezzanine", "mezzanineANDnews");
    extractionMap.put("news", "mezzanineANDnews");
    extractionMap.put("premium", "premium0");
    extractionMap.put("technology", "technology0");
    extractionMap.put("travel", "travel0");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, "missing", true, false);
    DimFilter filter = new ExtractionDimFilter("quality", "mezzanineANDnews", lookupExtractionFn, null);
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(new FilteredAggregatorFactory(
            QueryRunnerTestHelper.ROWS_COUNT,
            filter
        ), new FilteredAggregatorFactory(
            new LongSumAggregatorFactory("idx", "index"),
            filter
        ))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();
    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "agg-filter");

  }

  @Test
  public void testGroupByWithExtractionDimFilterOptimazitionManyToOne()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("mezzanine", "newsANDmezzanine");
    extractionMap.put("news", "newsANDmezzanine");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimFilter(
            new ExtractionDimFilter(
                "quality",
                "newsANDmezzanine",
                lookupExtractionFn,
                null
            )
        )
        .build();
    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        makeRow(query, "2011-04-02", "alias", "news", "rows", 1L, "idx", 114L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-dim-filter");
  }


  @Test
  public void testGroupByWithInjectiveLookupDimFilterNullDimsOptimized()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("", "EMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn;
    if (NullHandling.replaceWithDefault()) {
      extractionMap.put("", "EMPTY");
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    } else {
      extractionMap.put("", "SHOULD_NOT_BE_USED");
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, "EMPTY", true, true);
    }

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimFilter(new ExtractionDimFilter("null_column", "EMPTY", lookupExtractionFn, null))
        .build();
    List<ResultRow> expectedResults = Arrays
        .asList(
            makeRow(query, "2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
            makeRow(query, "2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
        );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-dim-filter");
  }

  @Test
  public void testGroupByWithInjectiveLookupDimFilterNullDimsNotOptimized()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("", "EMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn;
    if (NullHandling.replaceWithDefault()) {
      extractionMap.put("", "EMPTY");
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);
    } else {
      extractionMap.put("", "SHOULD_NOT_BE_USED");
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, "EMPTY", true, false);
    }

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimFilter(new ExtractionDimFilter("null_column", "EMPTY", lookupExtractionFn, null))
        .build();
    List<ResultRow> expectedResults = Arrays
        .asList(
            makeRow(query, "2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
            makeRow(query, "2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
        );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-dim-filter");
  }

  @Test
  public void testBySegmentResultsWithAllFiltersWithExtractionFns()
  {
    String extractionJsFn = "function(str) { return 'super-' + str; }";
    String jsFn = "function(x) { return(x === 'super-mezzanine') }";
    ExtractionFn extractionFn = new JavaScriptExtractionFn(
        extractionJsFn,
        false,
        JavaScriptConfig.getEnabledInstance()
    );

    List<DimFilter> superFilterList = new ArrayList<>();
    superFilterList.add(new SelectorDimFilter("quality", "super-mezzanine", extractionFn));
    superFilterList.add(new InDimFilter(
        "quality",
        Arrays.asList("not-super-mezzanine", "FOOBAR", "super-mezzanine"),
        extractionFn
    ));
    superFilterList.add(new BoundDimFilter(
        "quality",
        "super-mezzanine",
        "super-mezzanine",
        false,
        false,
        true,
        extractionFn,
        StringComparators.ALPHANUMERIC
    ));
    superFilterList.add(new RegexDimFilter("quality", "super-mezzanine", extractionFn));
    superFilterList.add(new SearchQueryDimFilter(
        "quality",
        new ContainsSearchQuerySpec("super-mezzanine", true),
        extractionFn
    ));
    superFilterList.add(new JavaScriptDimFilter("quality", jsFn, extractionFn, JavaScriptConfig.getEnabledInstance()));
    DimFilter superFilter = new AndDimFilter(superFilterList);

    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(superFilter)
        .overrideContext(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, true));
    final GroupByQuery fullQuery = builder.build();

    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<>(
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass<>(
            Collections.singletonList(
                makeRow(
                    fullQuery,
                    "2011-04-01",
                    "alias",
                    "mezzanine",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ),
            QueryRunnerTestHelper.SEGMENT_ID.toString(),
            Intervals.of("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }

    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<ResultRow>> singleSegmentRunners = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }
    ExecutorService exec = Executors.newCachedThreadPool();
    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(Executors.newCachedThreadPool(), singleSegmentRunners)),
            toolChest
        )
    );

    TestHelper.assertExpectedObjects(
        bySegmentResults,
        theRunner.run(QueryPlus.wrap(fullQuery)),
        "bySegment-filter"
    );
    exec.shutdownNow();
  }

  @Test
  public void testGroupByWithAllFiltersOnNullDimsWithExtractionFns()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("", "EMPTY");
    extractionMap.put(null, "EMPTY");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn extractionFn = new LookupExtractionFn(mapLookupExtractor, false, "EMPTY", true, true);
    String jsFn = "function(x) { return(x === 'EMPTY') }";

    List<DimFilter> superFilterList = new ArrayList<>();
    superFilterList.add(new SelectorDimFilter("null_column", "EMPTY", extractionFn));
    superFilterList.add(new InDimFilter("null_column", Arrays.asList("NOT-EMPTY", "FOOBAR", "EMPTY"), extractionFn));
    superFilterList.add(new BoundDimFilter("null_column", "EMPTY", "EMPTY", false, false, true, extractionFn,
                                           StringComparators.ALPHANUMERIC
    ));
    superFilterList.add(new RegexDimFilter("null_column", "EMPTY", extractionFn));
    superFilterList.add(
        new SearchQueryDimFilter("null_column", new ContainsSearchQuerySpec("EMPTY", true), extractionFn)
    );
    superFilterList.add(
        new JavaScriptDimFilter("null_column", jsFn, extractionFn, JavaScriptConfig.getEnabledInstance())
    );
    DimFilter superFilter = new AndDimFilter(superFilterList);

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setDimFilter(superFilter).build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
        makeRow(query, "2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction");
  }

  @Test
  public void testGroupByCardinalityAggWithExtractionFn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("market", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new CardinalityAggregatorFactory(
            "numVals",
            ImmutableList.of(new ExtractionDimensionSpec(
                QueryRunnerTestHelper.QUALITY_DIMENSION,
                QueryRunnerTestHelper.QUALITY_DIMENSION,
                helloFn
            )),
            false
        ))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "spot",
            "rows",
            9L,
            "numVals",
            1.0002442201269182d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "total_market",
            "rows",
            2L,
            "numVals",
            1.0002442201269182d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "upfront",
            "rows",
            2L,
            "numVals",
            1.0002442201269182d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "spot",
            "rows",
            9L,
            "numVals",
            1.0002442201269182d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "total_market",
            "rows",
            2L,
            "numVals",
            1.0002442201269182d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "upfront",
            "rows",
            2L,
            "numVals",
            1.0002442201269182d
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "cardinality-agg");
  }

  @Test
  public void testGroupByCardinalityAggOnFloat()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("market", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new CardinalityAggregatorFactory(
            "numVals",
            ImmutableList.of(new DefaultDimensionSpec(
                QueryRunnerTestHelper.INDEX_METRIC,
                QueryRunnerTestHelper.INDEX_METRIC
            )),
            false
        ))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "spot",
            "rows",
            9L,
            "numVals",
            8.015665809687173d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "total_market",
            "rows",
            2L,
            "numVals",
            2.000977198748901d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "upfront",
            "rows",
            2L,
            "numVals",
            2.000977198748901d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "spot",
            "rows",
            9L,
            "numVals",
            9.019833517963864d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "total_market",
            "rows",
            2L,
            "numVals",
            2.000977198748901d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "upfront",
            "rows",
            2L,
            "numVals",
            2.000977198748901d
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "cardinality-agg");
  }

  @Test
  public void testGroupByCardinalityAggOnMultiStringExpression()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(
            new ExpressionVirtualColumn("v0", "concat(quality,market)", ColumnType.STRING, TestExprMacroTable.INSTANCE)
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new CardinalityAggregatorFactory(
                "numVals",
                ImmutableList.of(DefaultDimensionSpec.of("v0")),
                false
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            26L,
            "numVals",
            13.041435202975777d
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "cardinality-agg");
  }

  @Test
  public void testGroupByCardinalityAggOnHyperUnique()
  {
    // Cardinality aggregator on complex columns (like hyperUnique) returns 0.

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new CardinalityAggregatorFactory(
                "cardinality",
                ImmutableList.of(DefaultDimensionSpec.of("quality_uniques")),
                false
            ),
            new HyperUniquesAggregatorFactory("hyperUnique", "quality_uniques", false, false)
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            26L,
            "cardinality",
            NullHandling.replaceWithDefault() ? 1.0002442201269182 : 0.0d,
            "hyperUnique",
            9.019833517963864d
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "cardinality-agg");
  }

  @Test
  public void testGroupByLongColumn()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("qualityLong", "ql_alias", ColumnType.LONG))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "ql_alias",
            OrderByColumnSpec.Direction.ASCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    Assert.assertEquals(Functions.<Sequence<ResultRow>>identity(), query.getLimitSpec().build(query));

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "ql_alias",
            1200L,
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-02",
            "ql_alias",
            1200L,
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long");
  }

  @Test
  public void testGroupByComplexColumn()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality_uniques", "quality_uniques"))
        .setDimFilter(new SelectorDimFilter("quality_uniques", null, null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    Assert.assertEquals(Functions.<Sequence<ResultRow>>identity(), query.getLimitSpec().build(query));

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "quality_uniques",
            null,
            "rows",
            26L,
            "idx",
            12446L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long");
  }

  @Test
  public void testGroupByLongColumnDescending()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("qualityLong", "ql_alias", ColumnType.LONG))
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "ql_alias",
            OrderByColumnSpec.Direction.DESCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    Assert.assertNotEquals(Functions.<Sequence<ResultRow>>identity(), query.getLimitSpec().build(query));

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "ql_alias",
            1700L,
            "rows",
            2L,
            "idx",
            175L
        ),
        makeRow(
            query,
            "2011-04-01",
            "ql_alias",
            1200L,
            "rows",
            2L,
            "idx",
            324L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long");
  }

  @Test
  public void testGroupByLongColumnWithExFn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ExtractionDimensionSpec("qualityLong", "ql_alias", jsExtractionFn))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "ql_alias",
            "super-1200",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-02",
            "ql_alias",
            "super-1200",
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-extraction");
  }

  @Test
  public void testGroupByLongTimeColumn()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("__time", "time_alias", ColumnType.LONG))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "time_alias",
            1301616000000L,
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-02",
            "time_alias",
            1301702400000L,
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long");
  }

  @Test
  public void testGroupByLongTimeColumnWithExFn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ExtractionDimensionSpec("__time", "time_alias", jsExtractionFn))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "time_alias",
            "super-1301616000000",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-02",
            "time_alias",
            "super-1301702400000",
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-extraction");
  }

  @Test
  public void testGroupByFloatColumn()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("index", "index_alias", ColumnType.FLOAT))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "index_alias",
            OrderByColumnSpec.Direction.ASCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    Assert.assertEquals(Functions.<Sequence<ResultRow>>identity(), query.getLimitSpec().build(query));

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "index_alias",
            158.747224f,
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-02",
            "index_alias",
            166.016049f,
            "rows",
            1L,
            "idx",
            166L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "float");
  }

  @Test
  public void testGroupByFloatColumnDescending()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("qualityFloat", "qf_alias", ColumnType.FLOAT))
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "qf_alias",
            OrderByColumnSpec.Direction.DESCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    Assert.assertNotEquals(Functions.<Sequence<ResultRow>>identity(), query.getLimitSpec().build(query));

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "qf_alias",
            17000.0f,
            "rows",
            2L,
            "idx",
            175L
        ),
        makeRow(
            query,
            "2011-04-01",
            "qf_alias",
            12000.0f,
            "rows",
            2L,
            "idx",
            324L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "float");
  }

  @Test
  public void testGroupByDoubleColumnDescending()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("qualityDouble", "alias", ColumnType.DOUBLE))
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "alias",
            OrderByColumnSpec.Direction.DESCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    Assert.assertNotEquals(Functions.<Sequence<ResultRow>>identity(), query.getLimitSpec().build(query));

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            17000.0d,
            "rows",
            2L,
            "idx",
            175L
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            12000.0d,
            "rows",
            2L,
            "idx",
            324L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "double");
  }

  @Test
  public void testGroupByFloatColumnWithExFn()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ExtractionDimensionSpec("index", "index_alias", jsExtractionFn))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults;

    expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "index_alias",
            "super-158.747224",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-02",
            "index_alias",
            "super-166.016049",
            "rows",
            1L,
            "idx",
            166L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "float");
  }

  @Test
  public void testGroupByWithHavingSpecOnLongAndFloat()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("market", "alias"),
            new DefaultDimensionSpec("qualityLong", "ql_alias", ColumnType.LONG),
            new DefaultDimensionSpec("__time", "time_alias", ColumnType.LONG),
            new DefaultDimensionSpec("index", "index_alias", ColumnType.FLOAT)
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setHavingSpec(
            new DimFilterHavingSpec(
                new AndDimFilter(
                    Lists.newArrayList(
                        new SelectorDimFilter("ql_alias", "1400", null),
                        new SelectorDimFilter("time_alias", "1301616000000", null),
                        new BoundDimFilter(
                            "index_alias",
                            "1310.0",
                            "1320.0",
                            true,
                            true,
                            null,
                            null,
                            StringComparators.NUMERIC
                        )
                    )
                ),
                null
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "alias", "total_market",
            "time_alias", 1301616000000L,
            "index_alias", 1314.8397,
            "ql_alias", 1400L,
            "rows", 1L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "havingspec-long-float");
  }

  @Test
  public void testGroupByLongAndFloatOutputAsString()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("qualityLong", "ql_alias"),
            new DefaultDimensionSpec("qualityFloat", "qf_alias")
        )
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "ql_alias",
            "1200",
            "qf_alias",
            "12000.0",
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-02",
            "ql_alias",
            "1200",
            "qf_alias",
            "12000.0",
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-float");
  }

  @Test
  public void testGroupByNumericStringsAsNumeric()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("qualityLong", "ql_alias"),
            new DefaultDimensionSpec("qualityFloat", "qf_alias"),
            new DefaultDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, "time_alias")
        )
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery outerQuery = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("time_alias", "time_alias2", ColumnType.LONG),
            new DefaultDimensionSpec("ql_alias", "ql_alias_long", ColumnType.LONG),
            new DefaultDimensionSpec("qf_alias", "qf_alias_float", ColumnType.FLOAT),
            new DefaultDimensionSpec("ql_alias", "ql_alias_float", ColumnType.FLOAT)
        ).setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            outerQuery,
            "2011-04-01",
            "time_alias2", 1301616000000L,
            "ql_alias_long", 1200L,
            "qf_alias_float", 12000.0,
            "ql_alias_float", 1200.0,
            "count", 1L
        ),
        makeRow(
            outerQuery,
            "2011-04-01",
            "time_alias2", 1301702400000L,
            "ql_alias_long", 1200L,
            "qf_alias_float", 12000.0,
            "ql_alias_float", 1200.0,
            "count", 1L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "numeric-string");
  }

  @Test
  public void testGroupByNumericStringsAsNumericWithDecoration()
  {
    // Cannot vectorize due to regex-filtered dimension spec.
    cannotVectorize();

    // rows with `technology` have `170000` in the qualityNumericString field
    RegexFilteredDimensionSpec regexSpec = new RegexFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityNumericString", "ql", ColumnType.LONG),
        "170000"
    );

    ListFilteredDimensionSpec listFilteredSpec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityNumericString", "qf", ColumnType.FLOAT),
        Sets.newHashSet("170000"),
        true
    );

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(regexSpec, listFilteredSpec)
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .addOrderByColumn("ql")
        .build();

    List<ResultRow> expectedResults;
    // "entertainment" rows are excluded by the decorated specs, they become empty rows
    expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "ql", NullHandling.defaultLongValue(),
            "qf", NullHandling.defaultDoubleValue(),
            "count", 2L
        ),
        makeRow(
            query,
            "2011-04-01",
            "ql", 170000L,
            "qf", 170000.0,
            "count", 2L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "numeric-string");
  }

  @Test
  public void testGroupByDecorationOnNumerics()
  {
    // Cannot vectorize due to filtered dimension spec.
    cannotVectorize();

    RegexFilteredDimensionSpec regexSpec = new RegexFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityLong", "ql", ColumnType.LONG),
        "1700"
    );

    ListFilteredDimensionSpec listFilteredSpec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityFloat", "qf", ColumnType.FLOAT),
        Sets.newHashSet("17000.0"),
        true
    );

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(regexSpec, listFilteredSpec)
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();
    List<ResultRow> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = Arrays.asList(
          makeRow(
              query,
              "2011-04-01",
              "ql", 0L,
              "qf", 0.0,
              "count", 2L
          ),
          makeRow(
              query,
              "2011-04-01",
              "ql", 1700L,
              "qf", 17000.0,
              "count", 2L
          )
      );
    } else {
      expectedResults = Arrays.asList(
          makeRow(
              query,
              "2011-04-01",
              "ql", null,
              "qf", null,
              "count", 2L
          ),
          makeRow(
              query,
              "2011-04-01",
              "ql", 1700L,
              "qf", 17000.0,
              "count", 2L
          )
      );
    }

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "numeric");
  }

  @Test
  public void testGroupByNestedWithInnerQueryNumerics()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("qualityLong", "ql_alias", ColumnType.LONG),
            new DefaultDimensionSpec("qualityFloat", "qf_alias", ColumnType.FLOAT)
        )
        .setDimFilter(
            new InDimFilter(
                "quality",
                Collections.singletonList("entertainment"),
                null
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery outerQuery = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("ql_alias", "quallong", ColumnType.LONG),
            new DefaultDimensionSpec("qf_alias", "qualfloat", ColumnType.FLOAT)
        )
        .setDimFilter(
            new AndDimFilter(
                Lists.newArrayList(
                    new SelectorDimFilter("ql_alias", "1200", null),
                    new BoundDimFilter(
                        "qf_alias",
                        "11095.0",
                        "12005.0",
                        true,
                        true,
                        null,
                        null,
                        StringComparators.NUMERIC
                    )
                )
            )
        )
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("ql_alias_sum", "ql_alias"),
            new DoubleSumAggregatorFactory("qf_alias_sum", "qf_alias")
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            outerQuery,
            "2011-04-01",
            "quallong",
            1200L,
            "qualfloat",
            12000.0,
            "ql_alias_sum",
            2400L,
            "qf_alias_sum",
            24000.0
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "numerics");
  }

  @Test
  public void testGroupByNestedWithInnerQueryOutputNullNumerics()
  {
    cannotVectorize();

    // Following extractionFn will generate null value for one kind of quality
    ExtractionFn extractionFn = new SearchQuerySpecDimExtractionFn(new ContainsSearchQuerySpec("1200", false));
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("quality", "alias"),
            new ExtractionDimensionSpec("qualityLong", "ql_alias", ColumnType.LONG, extractionFn),
            new ExtractionDimensionSpec("qualityFloat", "qf_alias", ColumnType.FLOAT, extractionFn),
            new ExtractionDimensionSpec("qualityDouble", "qd_alias", ColumnType.DOUBLE, extractionFn)
        )
        .setDimFilter(
            new InDimFilter(
                "quality",
                Arrays.asList("entertainment", "business"),
                null
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery outerQuery = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("ql_alias", "quallong", ColumnType.LONG),
            new DefaultDimensionSpec("qf_alias", "qualfloat", ColumnType.FLOAT),
            new DefaultDimensionSpec("qd_alias", "qualdouble", ColumnType.DOUBLE)
        )
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("ql_alias_sum", "ql_alias"),
            new DoubleSumAggregatorFactory("qf_alias_sum", "qf_alias"),
            new DoubleSumAggregatorFactory("qd_alias_sum", "qd_alias")
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            outerQuery,
            "2011-04-01",
            "quallong",
            NullHandling.defaultLongValue(),
            "qualfloat",
            NullHandling.defaultFloatValue(),
            "qualdouble",
            NullHandling.defaultDoubleValue(),
            "ql_alias_sum",
            NullHandling.defaultLongValue(),
            "qf_alias_sum",
            NullHandling.defaultFloatValue(),
            "qd_alias_sum",
            NullHandling.defaultDoubleValue()
        ),
        makeRow(
            outerQuery,
            "2011-04-01",
            "quallong",
            1200L,
            "qualfloat",
            12000.0,
            "qualdouble",
            12000.0,
            "ql_alias_sum",
            2400L,
            "qf_alias_sum",
            24000.0,
            "qd_alias_sum",
            24000.0
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "numerics");
  }

  @Test
  public void testGroupByNestedWithInnerQueryNumericsWithLongTime()
  {
    GroupByQuery subQuery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("market", "alias"),
            new DefaultDimensionSpec("__time", "time_alias", ColumnType.LONG),
            new DefaultDimensionSpec("index", "index_alias", ColumnType.FLOAT)
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    GroupByQuery outerQuery = makeQueryBuilder()
        .setDataSource(subQuery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("alias", "market"),
            new DefaultDimensionSpec("time_alias", "time_alias2", ColumnType.LONG)
        )
        .setAggregatorSpecs(
            new LongMaxAggregatorFactory("time_alias_max", "time_alias"),
            new DoubleMaxAggregatorFactory("index_alias_max", "index_alias")
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            outerQuery,
            "2011-04-01",
            "market", "spot",
            "time_alias2", 1301616000000L,
            "time_alias_max", 1301616000000L,
            "index_alias_max", 158.74722290039062
        ),
        makeRow(
            outerQuery,
            "2011-04-01",
            "market", "spot",
            "time_alias2", 1301702400000L,
            "time_alias_max", 1301702400000L,
            "index_alias_max", 166.01605224609375
        ),
        makeRow(
            outerQuery,
            "2011-04-01",
            "market", "total_market",
            "time_alias2", 1301616000000L,
            "time_alias_max", 1301616000000L,
            "index_alias_max", 1522.043701171875
        ),
        makeRow(
            outerQuery,
            "2011-04-01",
            "market", "total_market",
            "time_alias2", 1301702400000L,
            "time_alias_max", 1301702400000L,
            "index_alias_max", 1321.375
        ),
        makeRow(
            outerQuery,
            "2011-04-01",
            "market", "upfront",
            "time_alias2", 1301616000000L,
            "time_alias_max", 1301616000000L,
            "index_alias_max", 1447.3411865234375
        ),
        makeRow(
            outerQuery,
            "2011-04-01",
            "market", "upfront",
            "time_alias2", 1301702400000L,
            "time_alias_max", 1301702400000L,
            "index_alias_max", 1144.3424072265625
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "numerics");
  }

  @Test
  public void testGroupByStringOutputAsLong()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    ExtractionFn strlenFn = StrlenExtractionFn.instance();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new ExtractionDimensionSpec(
            QueryRunnerTestHelper.QUALITY_DIMENSION,
            "alias",
            ColumnType.LONG,
            strlenFn
        ))
        .setDimFilter(new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            13L,
            "rows",
            1L,
            "idx",
            158L
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            13L,
            "rows",
            1L,
            "idx",
            166L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "string-long");
  }

  @Test
  public void testGroupByWithAggsOnNumericDimensions()
  {
    // Cannot vectorize due to javascript aggregators.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new SelectorDimFilter("quality", "technology", null))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("qlLong", "qualityLong"),
            new DoubleSumAggregatorFactory("qlFloat", "qualityLong"),
            new JavaScriptAggregatorFactory(
                "qlJs",
                ImmutableList.of("qualityLong"),
                "function(a,b) { return a + b; }",
                "function() { return 0; }",
                "function(a,b) { return a + b }",
                JavaScriptConfig.getEnabledInstance()
            ),
            new DoubleSumAggregatorFactory("qfFloat", "qualityFloat"),
            new LongSumAggregatorFactory("qfLong", "qualityFloat"),
            new JavaScriptAggregatorFactory(
                "qfJs",
                ImmutableList.of("qualityFloat"),
                "function(a,b) { return a + b; }",
                "function() { return 0; }",
                "function(a,b) { return a + b }",
                JavaScriptConfig.getEnabledInstance()
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias", "technology",
            "rows", 1L,
            "qlLong", 1700L,
            "qlFloat", 1700.0,
            "qlJs", 1700.0,
            "qfFloat", 17000.0,
            "qfLong", 17000L,
            "qfJs", 17000.0
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias", "technology",
            "rows", 1L,
            "qlLong", 1700L,
            "qlFloat", 1700.0,
            "qlJs", 1700.0,
            "qfFloat", 17000.0,
            "qfLong", 17000L,
            "qfJs", 17000.0
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "numeric-dims");
  }

  @Test
  public void testGroupByNestedOuterExtractionFnOnFloatInner()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    String jsFn = "function(obj) { return obj; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"), new ExtractionDimensionSpec(
            "qualityFloat",
            "qf_inner",
            ColumnType.FLOAT,
            jsExtractionFn
        ))
        .setDimFilter(new SelectorDimFilter("quality", "technology", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery outerQuery = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"), new ExtractionDimensionSpec(
            "qf_inner",
            "qf_outer",
            ColumnType.FLOAT,
            jsExtractionFn
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(outerQuery, "2011-04-01", "alias", "technology", "qf_outer", 17000.0f, "rows", 2L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-fn");
  }

  @Test
  public void testGroupByNestedDoubleTimeExtractionFnWithLongOutputTypes()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("quality", "alias"),
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                "time_day",
                ColumnType.LONG,
                new TimeFormatExtractionFn(null, null, null, Granularities.DAY, true)
            )
        )
        .setDimFilter(new SelectorDimFilter("quality", "technology", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery outerQuery = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"), new ExtractionDimensionSpec(
            "time_day",
            "time_week",
            ColumnType.LONG,
            new TimeFormatExtractionFn(null, null, null, Granularities.WEEK, true)
        )).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(outerQuery, "2011-04-01", "alias", "technology", "time_week", 1301270400000L, "rows", 2L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-fn");
  }

  @Test
  public void testGroupByLimitPushDown()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec(
                    "marketalias",
                    OrderByColumnSpec.Direction.DESCENDING
                )),
                2
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "upfront",
            "rows",
            186L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "total_market",
            "rows",
            186L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByLimitPushDownWithOffset()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec(
                    "marketalias",
                    OrderByColumnSpec.Direction.DESCENDING
                )),
                1,
                2
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "total_market",
            "rows",
            186L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "spot",
            "rows",
            837L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByLimitPushDownWithLongDimensionNotInLimitSpec()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN).setDimensions(
            new ExtractionDimensionSpec("quality", "qualityLen", ColumnType.LONG, StrlenExtractionFn.instance())
        )
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.emptyList(),
                6
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "qualityLen",
            4L,
            "rows",
            93L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "qualityLen",
            6L,
            "rows",
            186L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "qualityLen",
            7L,
            "rows",
            279L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "qualityLen",
            8L,
            "rows",
            93L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "qualityLen",
            9L,
            "rows",
            279L
        ),
        makeRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "qualityLen",
            10L,
            "rows",
            186L
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testMergeLimitPushDownResultsWithLongDimensionNotInLimitSpec()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new ExtractionDimensionSpec(
            "quality",
            "qualityLen",
            ColumnType.LONG,
            StrlenExtractionFn.instance()
        ))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.emptyList(),
                20
            )
        )
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .setGranularity(Granularities.ALL);

    final GroupByQuery allGranQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        (queryPlus, responseContext) -> {
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

          return factory.getToolchest().mergeResults(
              (queryPlus3, responseContext1) -> new MergeSequence<>(
                  queryPlus3.getQuery().getResultOrdering(),
                  Sequences.simple(
                      Arrays.asList(
                          runner.run(queryPlus1, responseContext1),
                          runner.run(queryPlus2, responseContext1)
                      )
                  )
              )
          ).run(queryPlus, responseContext);
        }
    );
    Map<String, Object> context = new HashMap<>();
    List<ResultRow> allGranExpectedResults = Arrays.asList(
        makeRow(allGranQuery, "2011-04-02", "qualityLen", 4L, "rows", 2L),
        makeRow(allGranQuery, "2011-04-02", "qualityLen", 6L, "rows", 4L),
        makeRow(allGranQuery, "2011-04-02", "qualityLen", 7L, "rows", 6L),
        makeRow(allGranQuery, "2011-04-02", "qualityLen", 8L, "rows", 2L),
        makeRow(allGranQuery, "2011-04-02", "qualityLen", 9L, "rows", 6L),
        makeRow(allGranQuery, "2011-04-02", "qualityLen", 10L, "rows", 4L),
        makeRow(allGranQuery, "2011-04-02", "qualityLen", 13L, "rows", 2L)
    );

    TestHelper.assertExpectedObjects(
        allGranExpectedResults,
        mergedRunner.run(QueryPlus.wrap(allGranQuery)),
        "merged"
    );
  }

  @Test
  public void testMergeResultsWithLimitPushDown()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.DESCENDING)),
                5
            )
        )
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .setGranularity(Granularities.ALL);

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

            return factory.getToolchest().mergeResults(
                (queryPlus3, responseContext1) -> new MergeSequence<>(
                    queryPlus3.getQuery().getResultOrdering(),
                    Sequences.simple(
                        Arrays.asList(
                            runner.run(queryPlus1, responseContext1),
                            runner.run(queryPlus2, responseContext1)
                        )
                    )
                )
            ).run(queryPlus, responseContext);
          }
        }
    );
    Map<String, Object> context = new HashMap<>();
    List<ResultRow> allGranExpectedResults = Arrays.asList(
        makeRow(allGranQuery, "2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L),
        makeRow(allGranQuery, "2011-04-02", "alias", "technology", "rows", 2L, "idx", 177L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        makeRow(allGranQuery, "2011-04-02", "alias", "news", "rows", 2L, "idx", 221L),
        makeRow(allGranQuery, "2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L)
    );

    TestHelper.assertExpectedObjects(
        allGranExpectedResults,
        mergedRunner.run(QueryPlus.wrap(allGranQuery)),
        "merged"
    );
  }

  @Test
  public void testMergeResultsWithLimitPushDownSortByAgg()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("idx", OrderByColumnSpec.Direction.DESCENDING)),
                5
            )
        )
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .setGranularity(Granularities.ALL);

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

            return factory.getToolchest().mergeResults(
                (queryPlus3, responseContext1) -> new MergeSequence<>(
                    queryPlus3.getQuery().getResultOrdering(),
                    Sequences.simple(
                        Arrays.asList(
                            runner.run(queryPlus1, responseContext1),
                            runner.run(queryPlus2, responseContext1)
                        )
                    )
                )
            ).run(queryPlus, responseContext);
          }
        }
    );

    List<ResultRow> allGranExpectedResults = Arrays.asList(
        makeRow(allGranQuery, "2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        makeRow(allGranQuery, "2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 319L),
        makeRow(allGranQuery, "2011-04-02", "alias", "automotive", "rows", 2L, "idx", 269L),
        makeRow(allGranQuery, "2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    Iterable<ResultRow> results = mergedRunner.run(QueryPlus.wrap(allGranQuery)).toList();
    TestHelper.assertExpectedObjects(allGranExpectedResults, results, "merged");
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
        .setGranularity(Granularities.ALL);

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

            return factory.getToolchest().mergeResults(
                (queryPlus3, responseContext1) -> new MergeSequence<>(
                    queryPlus3.getQuery().getResultOrdering(),
                    Sequences.simple(
                        Arrays.asList(
                            runner.run(queryPlus1, responseContext1),
                            runner.run(queryPlus2, responseContext1)
                        )
                    )
                )
            ).run(queryPlus, responseContext);
          }
        }
    );

    List<ResultRow> allGranExpectedResults = Arrays.asList(
        makeRow(allGranQuery, "2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 319L),
        makeRow(allGranQuery, "2011-04-02", "alias", "automotive", "rows", 2L, "idx", 269L),
        makeRow(allGranQuery, "2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    Iterable<ResultRow> results = mergedRunner.run(QueryPlus.wrap(allGranQuery)).toList();
    TestHelper.assertExpectedObjects(allGranExpectedResults, results, "merged");
  }

  @Test
  public void testMergeResultsWithLimitPushDownSortByDimDim()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"), new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.DESCENDING),
                    new OrderByColumnSpec("market", OrderByColumnSpec.Direction.DESCENDING)
                ),
                5
            )
        )
        .overrideContext(
            ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true)
        )
        .setGranularity(Granularities.ALL);

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

            return factory.getToolchest().mergeResults(
                (queryPlus3, responseContext1) -> new MergeSequence<>(
                    queryPlus3.getQuery().getResultOrdering(),
                    Sequences.simple(
                        Arrays.asList(
                            runner.run(queryPlus1, responseContext1),
                            runner.run(queryPlus2, responseContext1)
                        )
                    )
                )
            ).run(queryPlus, responseContext);
          }
        }
    );

    List<ResultRow> allGranExpectedResults = Arrays.asList(
        makeRow(allGranQuery, "2011-04-02", "alias", "travel", "market", "spot", "rows", 2L, "idx", 243L),
        makeRow(allGranQuery, "2011-04-02", "alias", "technology", "market", "spot", "rows", 2L, "idx", 177L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "market", "upfront", "rows", 2L, "idx", 1817L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "market", "total_market", "rows", 2L, "idx", 2342L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "market", "spot", "rows", 2L, "idx", 257L)
    );

    Iterable<ResultRow> results = mergedRunner.run(QueryPlus.wrap(allGranQuery)).toList();
    TestHelper.assertExpectedObjects(allGranExpectedResults, results, "merged");
  }

  @Test
  public void testMergeResultsWithLimitPushDownSortByDimAggDim()
  {
    GroupByQuery.Builder builder = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"), new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "alias",
                        OrderByColumnSpec.Direction.DESCENDING
                    ),
                    new OrderByColumnSpec(
                        "idx",
                        OrderByColumnSpec.Direction.DESCENDING
                    ),
                    new OrderByColumnSpec(
                        "market",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                5
            )
        )
        .overrideContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN,
                true
            )
        )
        .setGranularity(Granularities.ALL);

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

            return factory.getToolchest().mergeResults(
                (queryPlus3, responseContext1) -> new MergeSequence<>(
                    queryPlus3.getQuery().getResultOrdering(),
                    Sequences.simple(
                        Arrays.asList(
                            runner.run(queryPlus1, responseContext1),
                            runner.run(queryPlus2, responseContext1)
                        )
                    )
                )
            ).run(queryPlus, responseContext);
          }
        }
    );

    List<ResultRow> allGranExpectedResults = Arrays.asList(
        makeRow(allGranQuery, "2011-04-02", "alias", "travel", "market", "spot", "rows", 2L, "idx", 243L),
        makeRow(allGranQuery, "2011-04-02", "alias", "technology", "market", "spot", "rows", 2L, "idx", 177L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "market", "total_market", "rows", 2L, "idx", 2342L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "market", "upfront", "rows", 2L, "idx", 1817L),
        makeRow(allGranQuery, "2011-04-02", "alias", "premium", "market", "spot", "rows", 2L, "idx", 257L)
    );

    Iterable<ResultRow> results = mergedRunner.run(QueryPlus.wrap(allGranQuery)).toList();
    TestHelper.assertExpectedObjects(allGranExpectedResults, results, "merged");
  }

  @Test
  public void testGroupByLimitPushDownPostAggNotSupported()
  {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Limit push down when sorting by a post aggregator is not supported.");

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec(
                    "constant",
                    OrderByColumnSpec.Direction.DESCENDING
                )),
                2
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setPostAggregatorSpecs(
            Collections.singletonList(new ConstantPostAggregator("constant", 1))
        )
        .overrideContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN,
                true
            )
        )
        .build();

    GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
  }

  @Test
  public void testEmptySubqueryWithLimitPushDown()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.EMPTY_INTERVAL)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec(
                    "alias",
                    OrderByColumnSpec.Direction.DESCENDING
                )),
                5
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
        .setLimitSpec(new DefaultLimitSpec(null, 5))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertFalse(results.iterator().hasNext());
  }


  @Test
  public void testSubqueryWithMultipleIntervalsInOuterQueryWithLimitPushDown()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.DESCENDING)),
                12
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(
                    Intervals.of("2011-04-01T00:00:00.000Z/2011-04-01T23:58:00.000Z"),
                    Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                )
            )
        ).setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.DESCENDING)),
                15
            )
        )
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),
        makeRow(query, "2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        makeRow(query, "2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        makeRow(query, "2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        makeRow(query, "2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),

        makeRow(query, "2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L),
        makeRow(query, "2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "limit-pushdown");
  }

  @Test
  public void testVirtualColumnFilterOnInnerQuery()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.DESCENDING)),
                12
            )
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(
            new MultipleIntervalSegmentSpec(
                ImmutableList.of(
                    Intervals.of("2011-04-01T00:00:00.000Z/2011-04-01T23:58:00.000Z"),
                    Intervals.of("2011-04-02T00:00:00.000Z/2011-04-03T00:00:00.000Z")
                )
            )
        ).setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "v",
                "case_searched(idx > 1000, 1, 0)",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimFilter(
            new BoundDimFilter(
                "v",
                "0",
                null,
                true,
                false,
                null,
                null,
                StringComparators.NUMERIC
            )
        )
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.DESCENDING)),
                15
            )
        )
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        makeRow(query, "2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        makeRow(query, "2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "virtual column filter on inner query");
  }

  @Test
  public void testRejectForceLimitPushDownWithHaving()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Cannot force limit push down when a having spec is present.");

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setDimensions(new DefaultDimensionSpec(QueryRunnerTestHelper.MARKET_DIMENSION, "marketalias"))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("marketalias", OrderByColumnSpec.Direction.DESCENDING)),
                2
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .overrideContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .setHavingSpec(new GreaterThanHavingSpec("rows", 10))
        .build();
    query.isApplyLimitPushDown();
  }

  @Test
  public void testTypeConversionWithMergingChainedExecutionRunner()
  {
    // Cannot vectorize due to extraction dimension spec.
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("quality", "alias"),
            new ExtractionDimensionSpec("quality", "qualityLen", ColumnType.LONG, StrlenExtractionFn.instance())
        )
        .setDimFilter(new SelectorDimFilter("quality", "technology", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "alias", "technology", "qualityLen", 10L, "rows", 2L, "idx", 156L),
        makeRow(query, "2011-04-02", "alias", "technology", "qualityLen", 10L, "rows", 2L, "idx", 194L)
    );

    ChainedExecutionQueryRunner ceqr = new ChainedExecutionQueryRunner(
        DirectQueryProcessingPool.INSTANCE,
        (query1, future) -> {
          return;
        },
        ImmutableList.of(runner, runner)
    );

    QueryRunner<ResultRow> mergingRunner = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(ceqr));

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, mergingRunner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "type-conversion");
  }

  @Test
  public void testGroupByNoMatchingPrefilter()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("quality", "quality")
        )
        .setDimFilter(new SelectorDimFilter("market", "spot", null, null))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new FilteredAggregatorFactory(
                new LongSumAggregatorFactory("index", "index"),
                new NotDimFilter(new SelectorDimFilter("longNumericNull", null, null))
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .setLimit(1)
        .build();

    List<ResultRow> expectedResults = ImmutableList.of(
        makeRow(query, "2011-04-01", "quality", "automotive", "rows", 1L, "index", 135L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByOnNullableLong()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("longNumericNull", "nullable", ColumnType.LONG)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setLimit(5)
        .build();

    List<ResultRow> expectedResults;
    if (NullHandling.sqlCompatible()) {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "nullable", null, "rows", 6L),
          makeRow(query, "2011-04-01", "nullable", 10L, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 20L, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 40L, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 50L, "rows", 6L)
      );
    } else {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "nullable", 0L, "rows", 6L),
          makeRow(query, "2011-04-01", "nullable", 10L, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 20L, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 40L, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 50L, "rows", 6L)
      );
    }

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByOnNullableDouble()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("doubleNumericNull", "nullable", ColumnType.DOUBLE)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setLimit(5)
        .build();

    List<ResultRow> expectedResults;
    if (NullHandling.sqlCompatible()) {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "nullable", null, "rows", 6L),
          makeRow(query, "2011-04-01", "nullable", 10.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 20.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 40.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 50.0, "rows", 6L)
      );
    } else {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "nullable", 0.0, "rows", 6L),
          makeRow(query, "2011-04-01", "nullable", 10.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 20.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 40.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 50.0, "rows", 6L)
      );
    }

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
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
        .build();

    List<ResultRow> expectedResults;
    if (NullHandling.sqlCompatible()) {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "nullable", null, "rows", 6L),
          makeRow(query, "2011-04-01", "nullable", 10.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 20.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 40.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 50.0, "rows", 6L)
      );
    } else {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "nullable", 0.0, "rows", 6L),
          makeRow(query, "2011-04-01", "nullable", 10.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 20.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 40.0, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 50.0, "rows", 6L)
      );
    }

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByOnNullableFloat()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(
            new DefaultDimensionSpec("floatNumericNull", "nullable", ColumnType.FLOAT)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setLimit(5)
        .build();

    List<ResultRow> expectedResults;
    if (NullHandling.sqlCompatible()) {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "nullable", null, "rows", 6L),
          makeRow(query, "2011-04-01", "nullable", 10.0f, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 20.0f, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 40.0f, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 50.0f, "rows", 6L)
      );
    } else {
      expectedResults = Arrays.asList(
          makeRow(query, "2011-04-01", "nullable", 0.0f, "rows", 6L),
          makeRow(query, "2011-04-01", "nullable", 10.0f, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 20.0f, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 40.0f, "rows", 2L),
          makeRow(query, "2011-04-01", "nullable", 50.0f, "rows", 6L)
      );
    }

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
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
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "v", 10000000L, "rows", 2L, "twosum", 6L),
        makeRow(query, "2011-04-01", "v", 12100000L, "rows", 2L, "twosum", 6L),
        makeRow(query, "2011-04-01", "v", 14400000L, "rows", 2L, "twosum", 6L),
        makeRow(query, "2011-04-01", "v", 16900000L, "rows", 2L, "twosum", 6L),
        makeRow(query, "2011-04-01", "v", 19600000L, "rows", 6L, "twosum", 18L)
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByOnVirtualColumnTimeFloor()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "v",
                "timestamp_floor(__time, 'P1D')",
                ColumnType.LONG,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(
            new DefaultDimensionSpec("v", "v", ColumnType.LONG)
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .setLimit(5)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(query, "2011-04-01", "v", 1301616000000L, "rows", 13L), // 04-01
        makeRow(query, "2011-04-01", "v", 1301702400000L, "rows", 13L)  // 04-02
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }


  @Test
  public void testGroupByWithExpressionAggregator()
  {
    // expression agg not yet vectorized
    cannotVectorize();
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            new ExpressionLambdaAggregatorFactory(
                "rows",
                Collections.emptySet(),
                null,
                "0",
                null,
                false,
                false,
                false,
                "__acc + 1",
                "__acc + rows",
                null,
                null,
                null,
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionLambdaAggregatorFactory(
                "idx",
                ImmutableSet.of("index"),
                null,
                "0.0",
                null,
                null,
                false,
                false,
                "__acc + index",
                null,
                null,
                null,
                null,
                TestExprMacroTable.INSTANCE
            )
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
            1L,
            "idx",
            135.88510131835938d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            118.57034
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            158.747224
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            120.134704
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2871.8866900000003d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            121.58358d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2900.798647d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            78.622547d
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            119.922742d
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            147.42593d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            112.987027d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            166.016049d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            113.446008d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2448.830613d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            114.290141d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2506.415148d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            97.387433d
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            126.411364d
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByWithExpressionAggregatorWithComplex()
  {
    cannotVectorize();
    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(Collections.emptyList())
        .setAggregatorSpecs(
            new CardinalityAggregatorFactory(
                "car",
                ImmutableList.of(new DefaultDimensionSpec("quality", "quality")),
                false
            ),
            new ExpressionLambdaAggregatorFactory(
                "carExpr",
                ImmutableSet.of("quality"),
                null,
                "hyper_unique()",
                null,
                null,
                false,
                false,
                "hyper_unique_add(quality, __acc)",
                "hyper_unique_add(carExpr, __acc)",
                null,
                "hyper_unique_estimate(o)",
                null,
                TestExprMacroTable.INSTANCE
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "1970-01-01", "car", QueryRunnerTestHelper.UNIQUES_9, "carExpr", QueryRunnerTestHelper.UNIQUES_9)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-cardinality");
  }

  @Test
  public void testGroupByWithExpressionAggregatorWithComplexOnSubquery()
  {
    final GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    final GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .setDimensions(Collections.emptyList())
        .setAggregatorSpecs(
            new CardinalityAggregatorFactory(
                "car",
                ImmutableList.of(new DefaultDimensionSpec("quality", "quality")),
                false
            ),
            new ExpressionLambdaAggregatorFactory(
                "carExpr",
                ImmutableSet.of("quality"),
                null,
                "hyper_unique()",
                null,
                null,
                false,
                false,
                "hyper_unique_add(quality, __acc)",
                null,
                null,
                "hyper_unique_estimate(o)",
                null,
                TestExprMacroTable.INSTANCE
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(query, "1970-01-01", "car", QueryRunnerTestHelper.UNIQUES_9, "carExpr", QueryRunnerTestHelper.UNIQUES_9)
    );
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-cardinality");
  }

  @Test
  public void testGroupByWithExpressionAggregatorWithArrays()
  {
    // expression agg not yet vectorized
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            new ExpressionLambdaAggregatorFactory(
                "rows",
                Collections.emptySet(),
                null,
                "0",
                null,
                false,
                false,
                false,
                "__acc + 1",
                "__acc + rows",
                null,
                null,
                null,
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionLambdaAggregatorFactory(
                "idx",
                ImmutableSet.of("index"),
                null,
                "0.0",
                null,
                true,
                false,
                false,
                "__acc + index",
                null,
                null,
                null,
                null,
                TestExprMacroTable.INSTANCE
            ),
            new ExpressionLambdaAggregatorFactory(
                "array_agg_distinct",
                ImmutableSet.of(QueryRunnerTestHelper.MARKET_DIMENSION),
                "acc",
                "[]",
                null,
                null,
                true,
                false,
                "array_set_add(acc, market)",
                "array_set_add_all(acc, array_agg_distinct)",
                null,
                null,
                null,
                TestExprMacroTable.INSTANCE
            )
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
            1L,
            "idx",
            135.88510131835938d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            118.57034,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            158.747224,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            120.134704,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2871.8866900000003d,
            "array_agg_distinct",
            new String[]{"spot", "total_market", "upfront"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            121.58358d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2900.798647d,
            "array_agg_distinct",
            new String[]{"spot", "total_market", "upfront"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            78.622547d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            119.922742d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            147.42593d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            112.987027d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            166.016049d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            113.446008d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2448.830613d,
            "array_agg_distinct",
            new String[]{"spot", "total_market", "upfront"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            114.290141d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2506.415148d,
            "array_agg_distinct",
            new String[]{"spot", "total_market", "upfront"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            97.387433d,
            "array_agg_distinct",
            new String[]{"spot"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            126.411364d,
            "array_agg_distinct",
            new String[]{"spot"}
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByExpressionAggregatorArrayMultiValue()
  {
    // expression agg not yet vectorized
    cannotVectorize();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            new ExpressionLambdaAggregatorFactory(
                "array_agg_distinct",
                ImmutableSet.of(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION),
                "acc",
                "[]",
                null,
                null,
                true,
                false,
                "array_set_add(acc, placementish)",
                "array_set_add_all(acc, array_agg_distinct)",
                null,
                null,
                null,
                TestExprMacroTable.INSTANCE
            )
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = Arrays.asList(
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "automotive",
            "array_agg_distinct",
            new String[]{"a", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "business",
            "array_agg_distinct",
            new String[]{"b", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "entertainment",
            "array_agg_distinct",
            new String[]{"e", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "health",
            "array_agg_distinct",
            new String[]{"h", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "mezzanine",
            "array_agg_distinct",
            new String[]{"m", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "news",
            "array_agg_distinct",
            new String[]{"n", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "premium",
            "array_agg_distinct",
            new String[]{"p", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "technology",
            "array_agg_distinct",
            new String[]{"preferred", "t"}
        ),
        makeRow(
            query,
            "2011-04-01",
            "alias",
            "travel",
            "array_agg_distinct",
            new String[]{"preferred", "t"}
        ),

        makeRow(
            query,
            "2011-04-02",
            "alias",
            "automotive",
            "array_agg_distinct",
            new String[]{"a", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "business",
            "array_agg_distinct",
            new String[]{"b", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "entertainment",
            "array_agg_distinct",
            new String[]{"e", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "health",
            "array_agg_distinct",
            new String[]{"h", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "mezzanine",
            "array_agg_distinct",
            new String[]{"m", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "news",
            "array_agg_distinct",
            new String[]{"n", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "premium",
            "array_agg_distinct",
            new String[]{"p", "preferred"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "technology",
            "array_agg_distinct",
            new String[]{"preferred", "t"}
        ),
        makeRow(
            query,
            "2011-04-02",
            "alias",
            "travel",
            "array_agg_distinct",
            new String[]{"preferred", "t"}
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByFloatMaxExpressionVsVirtualColumn()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("nil", "nil", ColumnType.STRING))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "v0",
                "\"floatNumericNull\"",
                ColumnType.FLOAT,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(
            new FloatMinAggregatorFactory(
                "min",
                "floatNumericNull"
            ),
            new FloatMinAggregatorFactory(
                "minExpression",
                null,
                "\"floatNumericNull\"",
                TestExprMacroTable.INSTANCE
            ),
            new FloatMinAggregatorFactory(
                "minVc",
                "v0"
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "nil",
            null,
            "min",
            NullHandling.replaceWithDefault() ? 0.0 : 10.0,
            "minExpression",
            NullHandling.replaceWithDefault() ? 0.0 : 10.0,
            "minVc",
            NullHandling.replaceWithDefault() ? 0.0 : 10.0
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByFloatMinExpressionVsVirtualColumnWithNonFloatInputButMatchingVirtualColumnType()
  {
    // SQL should never plan anything like this, the virtual column would be inferred to be a string type, and
    // would try to make a string min aggregator, which would throw an exception at the time of this comment since
    // it doesn't exist...
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("nil", "nil", ColumnType.STRING))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "v0",
                "\"placement\"",
                ColumnType.FLOAT,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(
            new FloatMinAggregatorFactory(
                "minExpression",
                null,
                "\"placement\"",
                TestExprMacroTable.INSTANCE
            ),
            new FloatMinAggregatorFactory(
                "minVc",
                "v0"
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "nil",
            null,
            "minExpression",
            NullHandling.replaceWithDefault() ? Float.POSITIVE_INFINITY : null,
            "minVc",
            NullHandling.defaultFloatValue()
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByFloatMinExpressionVsVirtualColumnWithExplicitStringVirtualColumnTypedInput()
  {
    cannotVectorize();
    // SQL should never plan anything like this, where the virtual column type mismatches the aggregator type
    // but it still works ok
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("nil", "nil", ColumnType.STRING))
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "v0",
                "\"placement\"",
                ColumnType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setAggregatorSpecs(
            new FloatMinAggregatorFactory(
                "min",
                "placement"
            ),
            new FloatMinAggregatorFactory(
                "minExpression",
                null,
                "\"placement\"",
                TestExprMacroTable.INSTANCE
            ),
            new FloatMinAggregatorFactory(
                "minVc",
                "v0"
            )
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        makeRow(
            query,
            "2011-04-01",
            "nil",
            null,
            "min",
            Float.POSITIVE_INFINITY,
            "minExpression",
            NullHandling.replaceWithDefault() ? Float.POSITIVE_INFINITY : null,
            "minVc",
            Float.POSITIVE_INFINITY
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testSummaryrowForEmptyInput()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimFilter(new SelectorDimFilter("placementish", "xxa", null))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
            new DoubleSumAggregatorFactory("idxDouble", "index")
        )
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "idx * 2", null, null, TestExprMacroTable.INSTANCE)))
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = ImmutableList.of(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            0L,
            "idx",
            NullHandling.replaceWithDefault() ? 0L : null,
            "idxFloat",
            NullHandling.replaceWithDefault() ? 0.0 : null,
            "idxDouble",
            NullHandling.replaceWithDefault() ? 0.0 : null,
            "post",
            NullHandling.replaceWithDefault() ? 0L : null
        )
    );

    StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQueryWithEmitter(
        factory,
        originalRunner,
        query,
        serviceEmitter
    );
    serviceEmitter.verifyEmitted("query/wait/time", ImmutableMap.of("vectorized", vectorize), 1);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testSummaryrowFilteredByHaving()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimFilter(new SelectorDimFilter("placementish", "xxa", null))
        .setHavingSpec(new GreaterThanHavingSpec("rows", 99L))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
            new DoubleSumAggregatorFactory("idxDouble", "index")
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = ImmutableList.of();

    StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQueryWithEmitter(
        factory,
        originalRunner,
        query,
        serviceEmitter
    );
    serviceEmitter.verifyEmitted("query/wait/time", ImmutableMap.of("vectorized", vectorize), 1);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }


  @Test
  public void testSummaryrowForEmptySubqueryInput()
  {
    GroupByQuery subquery = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimFilter(new SelectorDimFilter("placementish", "xxa", null))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    GroupByQuery query = makeQueryBuilder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
            new DoubleSumAggregatorFactory("idxDouble", "index")
        )
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN)
        .build();

    List<ResultRow> expectedResults = ImmutableList.of(
        makeRow(
            query,
            "2011-04-01",
            "rows",
            0L,
            "idx",
            NullHandling.replaceWithDefault() ? 0L : null,
            "idxFloat",
            NullHandling.replaceWithDefault() ? 0.0 : null,
            "idxDouble",
            NullHandling.replaceWithDefault() ? 0.0 : null
        )
    );

    StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQueryWithEmitter(
        factory,
        originalRunner,
        query,
        serviceEmitter
    );
    serviceEmitter.verifyEmitted("query/wait/time", ImmutableMap.of("vectorized", vectorize), 1);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }


  @Test
  public void testSummaryrowForEmptyInputByDay()
  {
    GroupByQuery query = makeQueryBuilder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimFilter(new SelectorDimFilter("placementish", "xxa", null))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            new LongSumAggregatorFactory("idx", "index"),
            new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
            new DoubleSumAggregatorFactory("idxDouble", "index")
        )
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    List<ResultRow> expectedResults = ImmutableList.of();

    StubServiceEmitter serviceEmitter = new StubServiceEmitter("", "");
    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQueryWithEmitter(
        factory,
        originalRunner,
        query,
        serviceEmitter
    );
    serviceEmitter.verifyEmitted("query/wait/time", ImmutableMap.of("vectorized", vectorize), 1);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  private static ResultRow makeRow(final GroupByQuery query, final String timestamp, final Object... vals)
  {
    return GroupByQueryRunnerTestHelper.createExpectedRow(query, timestamp, vals);
  }

  private static ResultRow makeRow(final GroupByQuery query, final DateTime timestamp, final Object... vals)
  {
    return GroupByQueryRunnerTestHelper.createExpectedRow(query, timestamp, vals);
  }

  private static List<ResultRow> makeRows(
      final GroupByQuery query,
      final String[] columnNames,
      final Object[]... values
  )
  {
    return GroupByQueryRunnerTestHelper.createExpectedRows(query, columnNames, values);
  }

  /**
   * Use this method instead of makeQueryBuilder() to make sure the context is set properly. Also, avoid
   * setContext in tests. Only use overrideContext.
   */
  private GroupByQuery.Builder makeQueryBuilder()
  {
    return GroupByQuery.builder().overrideContext(makeContext());
  }

  /**
   * Use this method instead of makeQueryBuilder() to make sure the context is set properly. Also, avoid
   * setContext in tests. Only use overrideContext.
   */
  private GroupByQuery.Builder makeQueryBuilder(final GroupByQuery query)
  {
    return new GroupByQuery.Builder(query).overrideContext(makeContext());
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
