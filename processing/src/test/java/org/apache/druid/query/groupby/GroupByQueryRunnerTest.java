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
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.apache.druid.collections.CloseableDefaultBlockingPool;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.DurationGranularity;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.guava.MergeSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.ChainedExecutionQueryRunner;
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
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatSumAggregatorFactory;
import org.apache.druid.query.aggregation.JavaScriptAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
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
import org.apache.druid.query.extraction.StringFormatExtractionFn;
import org.apache.druid.query.extraction.StrlenExtractionFn;
import org.apache.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.druid.query.filter.AndDimFilter;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ExtractionDimFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.JavaScriptDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.RegexDimFilter;
import org.apache.druid.query.filter.SearchQueryDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.having.BaseHavingSpec;
import org.apache.druid.query.groupby.having.DimFilterHavingSpec;
import org.apache.druid.query.groupby.having.DimensionSelectorHavingSpec;
import org.apache.druid.query.groupby.having.EqualToHavingSpec;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
import org.apache.druid.query.groupby.having.OrHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.LimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.search.ContainsSearchQuerySpec;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@RunWith(Parameterized.class)
public class GroupByQueryRunnerTest
{
  public static final ObjectMapper DEFAULT_MAPPER = TestHelper.makeSmileMapper();
  private static final DruidProcessingConfig DEFAULT_PROCESSING_CONFIG = new DruidProcessingConfig()
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

  private static final Closer resourceCloser = Closer.create();

  private final QueryRunner<Row> runner;
  private final GroupByQueryRunnerFactory factory;
  private final GroupByQueryConfig config;

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public static List<GroupByQueryConfig> testConfigs()
  {
    final GroupByQueryConfig v1Config = new GroupByQueryConfig()
    {
      @Override
      public String toString()
      {
        return "v1";
      }

      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V1;
      }
    };
    final GroupByQueryConfig v1SingleThreadedConfig = new GroupByQueryConfig()
    {
      @Override
      public boolean isSingleThreaded()
      {
        return true;
      }

      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V1;
      }

      @Override
      public String toString()
      {
        return "v1SingleThreaded";
      }
    };
    final GroupByQueryConfig v2Config = new GroupByQueryConfig()
    {
      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
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
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
      }

      @Override
      public int getBufferGrouperMaxSize()
      {
        return 2;
      }

      @Override
      public long getMaxOnDiskStorage()
      {
        return 10L * 1024 * 1024;
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
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
      }

      @Override
      public long getMaxMergingDictionarySize()
      {
        return 400;
      }

      @Override
      public long getMaxOnDiskStorage()
      {
        return 10L * 1024 * 1024;
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
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
      }

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

    v1Config.setMaxIntermediateRows(10000);
    v1SingleThreadedConfig.setMaxIntermediateRows(10000);

    return ImmutableList.of(
        v1Config,
        v1SingleThreadedConfig,
        v2Config,
        v2SmallBufferConfig,
        v2SmallDictionaryConfig,
        v2ParallelCombineConfig
    );
  }

  public static Pair<GroupByQueryRunnerFactory, Closer> makeQueryRunnerFactory(
      final GroupByQueryConfig config
  )
  {
    return makeQueryRunnerFactory(DEFAULT_MAPPER, config, DEFAULT_PROCESSING_CONFIG);
  }

  public static Pair<GroupByQueryRunnerFactory, Closer> makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config
  )
  {
    return makeQueryRunnerFactory(mapper, config, DEFAULT_PROCESSING_CONFIG);
  }

  public static Pair<GroupByQueryRunnerFactory, Closer> makeQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final DruidProcessingConfig processingConfig
  )
  {
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final CloseableStupidPool<ByteBuffer> bufferPool = new CloseableStupidPool<>(
        "GroupByQueryEngine-bufferPool",
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocateDirect(processingConfig.intermediateComputeSizeBytes());
          }
        }
    );
    final CloseableDefaultBlockingPool<ByteBuffer> mergeBufferPool = new CloseableDefaultBlockingPool<>(
        new Supplier<ByteBuffer>()
        {
          @Override
          public ByteBuffer get()
          {
            return ByteBuffer.allocateDirect(processingConfig.intermediateComputeSizeBytes());
          }
        },
        processingConfig.getNumMergeBuffers()
    );
    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER,
            bufferPool
        ),
        new GroupByStrategyV2(
            processingConfig,
            configSupplier,
            bufferPool,
            mergeBufferPool,
            mapper,
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(
        strategySelector,
        QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
    );
    final Closer closer = Closer.create();
    closer.register(bufferPool);
    closer.register(mergeBufferPool);
    return Pair.of(
        new GroupByQueryRunnerFactory(
            strategySelector,
            toolChest
        ),
        closer
    );
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : testConfigs()) {
      final Pair<GroupByQueryRunnerFactory, Closer> factoryAndCloser = makeQueryRunnerFactory(config);
      final GroupByQueryRunnerFactory factory = factoryAndCloser.lhs;
      resourceCloser.register(factoryAndCloser.rhs);
      for (QueryRunner<Row> runner : QueryRunnerTestHelper.makeQueryRunners(factory)) {
        final String testName = StringUtils.format(
            "config=%s, runner=%s",
            config.toString(),
            runner.toString()
        );
        constructors.add(new Object[]{testName, config, factory, runner});
      }
    }

    return constructors;
  }

  @AfterClass
  public static void teardown() throws IOException
  {
    resourceCloser.close();
  }

  public GroupByQueryRunnerTest(
      String testName,
      GroupByQueryConfig config,
      GroupByQueryRunnerFactory factory,
      QueryRunner runner
  )
  {
    this.config = config;
    this.factory = factory;
    this.runner = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
            new DoubleSumAggregatorFactory("idxDouble", "index")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByOnMissingColumn()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("nonexistent0", "alias0"),
            new ExtractionDimensionSpec("nonexistent1", "alias1", new StringFormatExtractionFn("foo"))
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias0", null,
            "alias1", "foo",
            "rows", 26L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "missing-column");
  }

  @Test
  public void testGroupByWithStringPostAggregator()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setPostAggregatorSpecs(
            ImmutableList.of(
                new ExpressionPostAggregator("post", "alias + 'x'", null, TestExprMacroTable.INSTANCE)
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(
                    new OrderByColumnSpec("post", OrderByColumnSpec.Direction.DESCENDING)
                ),
                Integer.MAX_VALUE
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "string-postAgg");
  }

  @Test
  public void testGroupByWithStringVirtualColumn()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "vc",
                "quality + 'x'",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(new DefaultDimensionSpec("vc", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotivex", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "businessx", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainmentx",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "healthx", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzaninex", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "newsx", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premiumx", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technologyx", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travelx", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotivex", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "businessx", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainmentx",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "healthx", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzaninex", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "newsx", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premiumx", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technologyx", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travelx", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "virtual-column");
  }

  @Test
  public void testGroupByWithDurationGranularity()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new DurationGranularity(86400L, 0L))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "duration-granularity");
  }

  @Test
  public void testGroupByWithOutputNameCollisions()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("[alias] already defined");

    GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("alias", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();
  }

  @Test
  public void testGroupByWithSortDimsFirst()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      return;
    }
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of("sortByDimsFirst", true))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "sort-by-dimensions-first");
  }

  @Test
  public void testGroupByWithChunkPeriod()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setContext(ImmutableMap.of("chunkPeriod", "P1D"))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 282L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 230L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 324L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 233L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 5317L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 235L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 5405L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 175L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 245L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "chunk-period");
  }

  @Test
  public void testGroupByNoAggregators()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel"),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology"),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel")
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "no-aggs");
  }

  @Test
  public void testMultiValueDimension()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("placementish", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 2L, "idx", 282L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "b", "rows", 2L, "idx", 230L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "e", "rows", 2L, "idx", 324L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "h", "rows", 2L, "idx", 233L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "m", "rows", 6L, "idx", 5317L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "n", "rows", 2L, "idx", 235L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "p", "rows", 6L, "idx", 5405L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "preferred", "rows", 26L, "idx", 12446L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "t", "rows", 4L, "idx", 420L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "multi-value-dim");
  }

  @Test
  public void testTwoMultiValueDimensions()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimFilter(new SelectorDimFilter("placementish", "a", null))
        .setDimensions(
            new DefaultDimensionSpec("placementish", "alias"),
            new DefaultDimensionSpec("placementish", "alias2")
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "two-multi-value-dims");
  }

  @Test
  public void testMultipleDimensionsOneOfWhichIsMultiValue1()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("placementish", "alias"),
            new DefaultDimensionSpec("quality", "quality")
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "one-multi-value-dim");
  }

  @Test
  public void testMultipleDimensionsOneOfWhichIsMultiValueDifferentOrder()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("quality", "quality"),
            new DefaultDimensionSpec("placementish", "alias")
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "one-multi-value-dim-different-order");
  }

  @Test
  public void testGroupByMaxRowsLimitContextOverride()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of("maxResults", 1))
        .build();

    List<Row> expectedResults = null;
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(ResourceLimitExceededException.class);
    } else {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-01",
              "alias",
              "entertainment",
              "rows",
              1L,
              "idx",
              158L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-02",
              "alias",
              "entertainment",
              "rows",
              1L,
              "idx",
              166L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
      );
    }

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "override-maxResults");
  }

  @Test
  public void testGroupByTimeoutContextOverride()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 60000))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "override-timeout");
  }

  @Test
  public void testGroupByMaxOnDiskStorageContextOverride()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of("maxOnDiskStorage", 0, "bufferGrouperMaxSize", 1))
        .build();

    List<Row> expectedResults = null;
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      expectedException.expect(ResourceLimitExceededException.class);
      expectedException.expectMessage("Not enough aggregation buffer space to execute this query");
    } else {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-01",
              "alias",
              "entertainment",
              "rows",
              1L,
              "idx",
              158L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-02",
              "alias",
              "entertainment",
              "rows",
              1L,
              "idx",
              166L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
      );
    }

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "overide-maxOnDiskStorage");
  }

  @Test
  public void testNotEnoughDictionarySpaceThroughContextOverride()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of("maxOnDiskStorage", 0, "maxMergingDictionarySize", 1))
        .build();

    List<Row> expectedResults = null;
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      expectedException.expect(ResourceLimitExceededException.class);
      expectedException.expectMessage("Not enough dictionary space to execute this query");
    } else {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-01",
              "alias",
              "entertainment",
              "rows",
              1L,
              "idx",
              158L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-02",
              "alias",
              "entertainment",
              "rows",
              1L,
              "idx",
              166L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
      );
    }

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "dictionary-space");
  }

  @Test
  public void testNotEnoughDiskSpaceThroughContextOverride()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of("maxOnDiskStorage", 1, "maxMergingDictionarySize", 1))
        .build();

    List<Row> expectedResults = null;
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      expectedException.expect(ResourceLimitExceededException.class);
      if (config.getMaxOnDiskStorage() > 0) {
        // The error message always mentions disk if you have spilling enabled (maxOnDiskStorage > 0)
        expectedException.expectMessage("Not enough disk space to execute this query");
      } else {
        expectedException.expectMessage("Not enough dictionary space to execute this query");
      }
    } else {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-01",
              "alias",
              "entertainment",
              "rows",
              1L,
              "idx",
              158L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-02",
              "alias",
              "entertainment",
              "rows",
              1L,
              "idx",
              166L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
      );
    }

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "disk-space");
  }

  @Test
  public void testSubqueryWithOuterMaxOnDiskStorageContextOverride()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.ASCENDING)),
                null
            )
        )
        .setContext(
            ImmutableMap.of(
                "maxOnDiskStorage", Integer.MAX_VALUE,
                "bufferGrouperMaxSize", Integer.MAX_VALUE
            )
        )
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new ArrayList<>()).setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setContext(ImmutableMap.of("maxOnDiskStorage", 0, "bufferGrouperMaxSize", 0))
        .build();

    // v1 strategy throws an exception for this query because it tries to merge the noop outer
    // and default inner limit specs, then apply the resulting spec to the outer query, which
    // fails because the inner limit spec refers to columns that don't exist in the outer
    // query. I'm not sure why it does this.

    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(ISE.class);
      expectedException.expectMessage("Unknown column in order clause");
      GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    } else {
      expectedException.expect(ResourceLimitExceededException.class);
      expectedException.expectMessage("Not enough aggregation buffer space to execute this query");
      GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    }
  }

  @Test
  public void testGroupByWithRebucketRename()
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
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "rebucket-rename");
  }


  @Test
  public void testGroupByWithSimpleRenameRetainMissingNonInjective()
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
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), true, null, false, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
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
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), true, null, true, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
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
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, "MISSING", true, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
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
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, true, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive0", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business0", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine0", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news0", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium0", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology0", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel0", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive0", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business0", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment0",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine0", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news0", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium0", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology0", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel0", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "simple-rename");
  }

  @Test
  public void testGroupByWithUniques()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.qualityUniques)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "uniques");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithUniquesAndPostAggWithSameName()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new HyperUniquesAggregatorFactory(
            "quality_uniques",
            "quality_uniques"
        ))
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator("quality_uniques", "quality_uniques")
            )
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "quality_uniques",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "unique-postagg-same-name");
  }

  @Test
  public void testGroupByWithCardinality()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.qualityCardinality)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "cardinality",
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "cardinality");
  }

  @Test
  public void testGroupByWithFirstLast()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(
            new LongFirstAggregatorFactory("first", "index"),
            new LongLastAggregatorFactory("last", "index")
        )
        .setGranularity(QueryRunnerTestHelper.monthGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-01-01", "market", "spot", "first", 100L, "last", 155L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-01",
            "market",
            "total_market",
            "first",
            1000L,
            "last",
            1127L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-01-01", "market", "upfront", "first", 800L, "last", 943L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-02-01", "market", "spot", "first", 132L, "last", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-02-01",
            "market",
            "total_market",
            "first",
            1203L,
            "last",
            1292L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-02-01",
            "market",
            "upfront",
            "first",
            1667L,
            "last",
            1101L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-01", "market", "spot", "first", 153L, "last", 125L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-03-01",
            "market",
            "total_market",
            "first",
            1124L,
            "last",
            1366L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-03-01",
            "market",
            "upfront",
            "first",
            1166L,
            "last",
            1063L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "market", "spot", "first", 135L, "last", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "market",
            "total_market",
            "first",
            1314L,
            "last",
            1029L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "market", "upfront", "first", 1447L, "last", 780L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "first-last-aggs");
  }

  @Test
  public void testGroupByWithNoResult()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.emptyInterval)
        .setDimensions(new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            QueryRunnerTestHelper.indexLongSum,
            QueryRunnerTestHelper.qualityCardinality,
            new LongFirstAggregatorFactory("first", "index"),
            new LongLastAggregatorFactory("last", "index")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = ImmutableList.of();
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertEquals(expectedResults, results);
  }

  @Test
  public void testGroupByWithNullProducingDimExtractionFn()
  {
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
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimensions(new ExtractionDimensionSpec("quality", "alias", nullExtractionFn))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", null, "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "b", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "e", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "h", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "n", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "p", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "t", "rows", 2L, "idx", 197L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", null, "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "a", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "b", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "e", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "h", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "n", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "p", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "t", "rows", 2L, "idx", 223L)
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

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimensions(new ExtractionDimensionSpec("quality", "alias", emptyStringExtractionFn))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "b", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "e", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "h", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "n", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "p", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "t", "rows", 2L, "idx", 197L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "a", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "b", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "e", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "h", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "n", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "p", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "t", "rows", 2L, "idx", 223L)
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

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setInterval("2011-03-31T00:00:00-07:00/2011-04-02T00:00:00-07:00")
                                     .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                                     .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory(
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

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            135L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            118L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            120L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2870L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            121L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2900L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            78L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-03-31", tz),
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            119L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "automotive",
            "rows",
            1L,
            "idx",
            147L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "business",
            "rows",
            1L,
            "idx",
            112L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "entertainment",
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "health",
            "rows",
            1L,
            "idx",
            113L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idx",
            2447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "news",
            "rows",
            1L,
            "idx",
            114L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "premium",
            "rows",
            3L,
            "idx",
            2505L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "technology",
            "rows",
            1L,
            "idx",
            97L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            new DateTime("2011-04-01", tz),
            "alias",
            "travel",
            "rows",
            1L,
            "idx",
            126L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "timezone");
  }

  @Test
  public void testMergeResults()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery fullQuery = builder.build();
    final GroupByQuery allGranQuery = builder.copy().setGranularity(Granularities.ALL).build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    Map<String, Object> context = new HashMap<>();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(QueryPlus.wrap(fullQuery), context), "merged");

    List<Row> allGranExpectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    TestHelper.assertExpectedObjects(
        allGranExpectedResults,
        mergedRunner.run(QueryPlus.wrap(allGranQuery), context),
        "merged"
    );
  }

  @Test
  public void testMergeResultsWithLimit()
  {
    for (int limit = 1; limit < 20; ++limit) {
      doTestMergeResultsWithValidLimit(limit);
    }
  }

  private void doTestMergeResultsWithValidLimit(final int limit)
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimit(limit);

    final GroupByQuery fullQuery = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);

    Map<String, Object> context = new HashMap<>();
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit),
        mergeRunner.run(QueryPlus.wrap(fullQuery), context),
        StringUtils.format("limit: %d", limit)
    );
  }

  @Test
  public void testMergeResultsAcrossMultipleDaysWithLimitAndOrderBy()
  {
    final int limit = 14;
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(Granularities.DAY)
        .setLimit(limit)
        .addOrderByColumn("idx", OrderByColumnSpec.Direction.DESCENDING);

    GroupByQuery fullQuery = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);

    Map<String, Object> context = new HashMap<>();
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit),
        mergeRunner.run(QueryPlus.wrap(fullQuery), context),
        StringUtils.format("limit: %d", limit)
    );
  }

  @Test
  public void testMergeResultsAcrossMultipleDaysWithLimitAndOrderByUsingMathExpressions()
  {
    final int limit = 14;
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval(QueryRunnerTestHelper.firstToThird)
        .setVirtualColumns(
            new ExpressionVirtualColumn(
                "expr",
                "index * 2 + indexMin / 10",
                ValueType.FLOAT,
                TestExprMacroTable.INSTANCE
            )
        )
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "expr"))
        .setGranularity(Granularities.DAY)
        .setLimit(limit)
        .addOrderByColumn("idx", OrderByColumnSpec.Direction.DESCENDING);

    GroupByQuery fullQuery = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 6090L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 6030L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 333L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 285L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 255L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 252L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 251L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 248L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 165L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 5262L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 5141L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 348L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 309L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 265L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);

    Map<String, Object> context = new HashMap<>();
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, limit),
        mergeRunner.run(QueryPlus.wrap(fullQuery), context),
        StringUtils.format("limit: %d", limit)
    );
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMergeResultsWithNegativeLimit()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
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

    final Comparator<Row> idxComparator =
        new Comparator<Row>()
        {
          @Override
          public int compare(Row o1, Row o2)
          {
            return Float.compare(o1.getMetric("idx").floatValue(), o2.getMetric("idx").floatValue());
          }
        };

    Comparator<Row> rowsIdxComparator =
        new Comparator<Row>()
        {

          @Override
          public int compare(Row o1, Row o2)
          {
            int value = Float.compare(o1.getMetric("rows").floatValue(), o2.getMetric("rows").floatValue());
            if (value != 0) {
              return value;
            }

            return idxComparator.compare(o1, o2);
          }
        };

    List<Row> allResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    List<List<Row>> expectedResults = Lists.newArrayList(
        Ordering.from(idxComparator).sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).sortedCopy(allResults),
        Ordering.from(idxComparator).reverse().sortedCopy(allResults),
        Ordering.from(rowsIdxComparator).reverse().sortedCopy(allResults)
    );

    for (int i = 0; i < orderBySpecs.length; ++i) {
      doTestMergeResultsWithOrderBy(orderBySpecs[i], expectedResults.get(i));
    }
  }

  private void doTestMergeResultsWithOrderBy(LimitSpec orderBySpec, List<Row> expectedResults)
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setLimitSpec(orderBySpec);

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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

    Map<String, Object> context = new HashMap<>();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(QueryPlus.wrap(fullQuery), context), "merged");
  }

  @Test
  public void testGroupByOrderLimit()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn("rows")
        .addOrderByColumn("alias", OrderByColumnSpec.Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L)
    );

    Map<String, Object> context = new HashMap<>();
    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query), context), "no-limit");

    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build()), context),
        "limited"
    );

    // Now try it with an expression based aggregator.
    List<AggregatorFactory> aggregatorSpecs = Arrays.asList(
        QueryRunnerTestHelper.rowsCount,
        new DoubleSumAggregatorFactory("idx", null, "index / 2 + indexMin", TestExprMacroTable.INSTANCE)
    );
    builder.setLimit(Integer.MAX_VALUE).setAggregatorSpecs(aggregatorSpecs);

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
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
        mergeRunner.run(QueryPlus.wrap(builder.build()), context),
        "no-limit"
    );
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build()), context),
        "limited"
    );

    // Now try it with an expression virtual column.
    ExpressionVirtualColumn expressionVirtualColumn = new ExpressionVirtualColumn(
        "expr",
        "index / 2 + indexMin",
        ValueType.FLOAT,
        TestExprMacroTable.INSTANCE
    );
    List<AggregatorFactory> aggregatorSpecs2 = Arrays.asList(
        QueryRunnerTestHelper.rowsCount,
        new DoubleSumAggregatorFactory("idx", "expr")
    );
    builder.setLimit(Integer.MAX_VALUE).setVirtualColumns(expressionVirtualColumn).setAggregatorSpecs(aggregatorSpecs2);

    TestHelper.assertExpectedObjects(
        expectedResults,
        mergeRunner.run(QueryPlus.wrap(builder.build()), context),
        "no-limit"
    );
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build()), context),
        "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit2()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn("rows", OrderByColumnSpec.Direction.DESCENDING)
        .addOrderByColumn("alias", OrderByColumnSpec.Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L)
    );

    Map<String, Object> context = new HashMap<>();
    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query), context), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build()), context),
        "limited"
    );
  }

  @Test
  public void testGroupByWithOrderLimit3()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new DoubleSumAggregatorFactory("idx", "index"))
        .addOrderByColumn("idx", OrderByColumnSpec.Direction.DESCENDING)
        .addOrderByColumn("alias", OrderByColumnSpec.Direction.DESCENDING)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    GroupByQuery query = builder.build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
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

    Map<String, Object> context = new HashMap<>();
    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query), context), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build()), context),
        "limited"
    );
  }

  @Test
  public void testGroupByOrderLimitNumeric()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
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

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 2L, "idx", 216L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    Map<String, Object> context = new HashMap<>();
    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query), context), "no-limit");
    TestHelper.assertExpectedObjects(
        Iterables.limit(expectedResults, 5),
        mergeRunner.run(QueryPlus.wrap(builder.setLimit(5).build()), context),
        "limited"
    );
  }

  @Test
  public void testGroupByWithSameCaseOrdering()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("marketalias", OrderByColumnSpec.Direction.DESCENDING)),
                3
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "upfront",
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "total_market",
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "spot",
            "rows",
            837L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithOrderLimit4()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            QueryRunnerTestHelper.marketDimension
        ))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(QueryRunnerTestHelper.marketDimension, OrderByColumnSpec.Direction.DESCENDING)
                ),
                3
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "market", "upfront", "rows", 186L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "total_market",
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01T00:00:00.000Z", "market", "spot", "rows", 837L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithOrderOnHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            QueryRunnerTestHelper.marketDimension
        ))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(QueryRunnerTestHelper.uniqueMetric, OrderByColumnSpec.Direction.DESCENDING)
                ),
                3
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.qualityUniques)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                    QueryRunnerTestHelper.uniqueMetric
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_9
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "upfront",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_2
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "total_market",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_2
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithHavingOnHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            QueryRunnerTestHelper.marketDimension
        ))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(QueryRunnerTestHelper.uniqueMetric, OrderByColumnSpec.Direction.DESCENDING)
                ),
                3
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(QueryRunnerTestHelper.uniqueMetric, 8))
        .setAggregatorSpecs(QueryRunnerTestHelper.qualityUniques)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                    QueryRunnerTestHelper.uniqueMetric
                )
            )
        )
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithHavingOnFinalizedHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            QueryRunnerTestHelper.marketDimension
        ))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                3
            )
        )
        .setHavingSpec(new GreaterThanHavingSpec(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, 8))
        .setAggregatorSpecs(QueryRunnerTestHelper.qualityUniques)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                    QueryRunnerTestHelper.uniqueMetric
                )
            )
        )
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_9
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithLimitOnFinalizedHyperUnique()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            QueryRunnerTestHelper.marketDimension
        ))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(
                    new OrderByColumnSpec(
                        QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ),
                3
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.qualityUniques)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new HyperUniqueFinalizingPostAggregator(
                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                    QueryRunnerTestHelper.uniqueMetric
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "spot",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_9,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_9
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "upfront",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_2
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "market",
            "total_market",
            QueryRunnerTestHelper.uniqueMetric,
            QueryRunnerTestHelper.UNIQUES_2,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
            QueryRunnerTestHelper.UNIQUES_2
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testGroupByWithAlphaNumericDimensionOrder()
  {
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

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", null, StringComparators.ALPHANUMERIC)),
                null
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health0000", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health09", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health20", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health55", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health105", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health999", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel47", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel123", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel555", "rows", 1L, "idx", 119L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health0000", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health09", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health20", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health55", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health105", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health999", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel47", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel123", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel555", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "alphanumeric-dimension-order");
  }

  @Test
  public void testGroupByWithLookupAndLimitAndSortByDimsFirst()
  {
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

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird).setDimensions(new ExtractionDimensionSpec(
            "quality",
            "alias",
            new LookupExtractionFn(new MapLookupExtractor(map, false), false, null, false, false)
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", null, StringComparators.ALPHANUMERIC)),
                11
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of("sortByDimsFirst", true))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "1", "rows", 1L, "idx", 119L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "1", "rows", 1L, "idx", 126L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "2", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "2", "rows", 1L, "idx", 97L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "3", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "3", "rows", 3L, "idx", 2505L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "4", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "4", "rows", 1L, "idx", 114L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "5", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "5", "rows", 3L, "idx", 2447L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "6", "rows", 1L, "idx", 120L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "lookup-limit");
  }

  @Ignore
  @Test
  // This is a test to verify per limit groupings, but Druid currently does not support this functionality. At a point
  // in time when Druid does support this, we can re-evaluate this test.
  public void testLimitPerGrouping()
  {
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.dayGran).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            QueryRunnerTestHelper.marketDimension
        ))
        .setInterval(QueryRunnerTestHelper.firstToThird)
        // Using a limitSpec here to achieve a per group limit is incorrect.
        // Limit is applied on the overall results.
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("rows", OrderByColumnSpec.Direction.DESCENDING)),
                2
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01T00:00:00.000Z", "market", "spot", "rows", 9L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02T00:00:00.000Z", "market", "spot", "rows", 9L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
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
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "index",
            4420L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4420L + 1L)
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "index",
            4416L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4416L + 1L)
        )
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("index", "index"))
        .setPostAggregatorSpecs(ImmutableList.of(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(new GreaterThanHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric, 1000L))
            )
        );

    final GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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

    Map<String, Object> context = new HashMap<>();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(QueryPlus.wrap(fullQuery), context), "merged");
  }

  @Test
  public void testGroupByWithOrderLimitHavingSpec()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-01-25/2011-01-28")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new DoubleSumAggregatorFactory("index", "index"))
        .setGranularity(Granularities.ALL)
        .setHavingSpec(new GreaterThanHavingSpec("index", 310L))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("index", OrderByColumnSpec.Direction.ASCENDING)),
                5
            )
        );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "business",
            "rows",
            3L,
            "index",
            312.38165283203125
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "news",
            "rows",
            3L,
            "index",
            312.7834167480469
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "technology",
            "rows",
            3L,
            "index",
            324.6412353515625
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "travel",
            "rows",
            3L,
            "index",
            393.36322021484375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-01-25",
            "alias",
            "health",
            "rows",
            3L,
            "index",
            511.2996826171875
        )
    );

    GroupByQuery fullQuery = builder.build();
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery);
    TestHelper.assertExpectedObjects(
        expectedResults,
        results,
        "order-limit-havingspec"
    );
  }

  @Test
  public void testPostAggHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "index",
            4420L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4420L + 1L)
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "index",
            4416L,
            QueryRunnerTestHelper.addRowsIndexConstantMetric,
            (double) (6L + 4416L + 1L)
        )
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("index", "index"))
        .setPostAggregatorSpecs(ImmutableList.of(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec(QueryRunnerTestHelper.addRowsIndexConstantMetric, 1000L)
                )
            )
        );

    final GroupByQuery fullQuery = builder.build();
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        "postagg-havingspec"
    );
  }


  @Test
  public void testHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
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
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        "havingspec"
    );
  }

  @Test
  public void testDimFilterHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

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

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(havingSpec);

    final GroupByQuery fullQuery = builder.build();
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

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
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

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(havingSpec);

    final GroupByQuery fullQuery = builder.build();
    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, fullQuery),
        "extractionfn-havingspec"
    );
  }

  @Test
  public void testMergedHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 2L, "idx", 217L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 6L, "idx", 4416L)
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
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

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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

    Map<String, Object> context = new HashMap<>();
    TestHelper.assertExpectedObjects(expectedResults, mergedRunner.run(QueryPlus.wrap(fullQuery), context), "merged");
  }

  @Test
  public void testMergedPostAggHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "business",
            "rows",
            2L,
            "idx",
            217L,
            "rows_times_10",
            20.0
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            6L,
            "idx",
            4420L,
            "rows_times_10",
            60.0
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            6L,
            "idx",
            4416L,
            "rows_times_10",
            60.0
        )
    );

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
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

    GroupByQuery fullQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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

    Map<String, Object> context = new HashMap<>();
    // add an extra layer of merging, simulate broker forwarding query to historical
    TestHelper.assertExpectedObjects(
        expectedResults,
        factory.getToolchest().postMergeQueryDecoration(
            factory.getToolchest().mergeResults(
                factory.getToolchest().preMergeQueryDecoration(mergedRunner)
            )
        ).run(QueryPlus.wrap(fullQuery), context),
        "merged"
    );

    fullQuery = fullQuery.withPostAggregatorSpecs(
        Collections.singletonList(
            new ExpressionPostAggregator("rows_times_10", "rows * 10.0", null, TestExprMacroTable.INSTANCE)
        )
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        factory.getToolchest().postMergeQueryDecoration(
            factory.getToolchest().mergeResults(
                factory.getToolchest().preMergeQueryDecoration(mergedRunner)
            )
        ).run(QueryPlus.wrap(fullQuery), context),
        "merged"
    );
  }

  @Test
  public void testCustomAggregatorHavingSpec()
  {
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            1L,
            "idxDouble",
            135.885094d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idxDouble",
            158.747224d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idxDouble",
            2871.8866900000003d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            3L,
            "idxDouble",
            2900.798647d
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            1L,
            "idxDouble",
            147.425935d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            1L,
            "idxDouble",
            166.016049d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "mezzanine",
            "rows",
            3L,
            "idxDouble",
            2448.830613d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            3L,
            "idxDouble",
            2506.415148d
        )
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new TestBigDecimalSumAggregatorFactory("idxDouble", "index")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new EqualToHavingSpec("rows", 3L),
                    new GreaterThanHavingSpec("idxDouble", 135.00d)
                )
            )
        )
        .build();

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query),
        "custom-havingspec"
    );
  }

  @Test
  public void testGroupByWithRegEx()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimFilter(new RegexDimFilter("quality", "auto.*", null))
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "rows", 2L)
    );

    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    Map<String, Object> context = new HashMap<>();
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query), context), "no-limit");
  }

  @Test
  public void testGroupByWithNonexistentDimension()
  {
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .addDimension("billy")
        .addDimension("quality").setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null));

    final GroupByQuery query = builder.build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            "quality",
            "automotive",
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "business", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            "quality",
            "entertainment",
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "health", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "mezzanine", "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "news", "rows", 2L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "premium", "rows", 6L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "billy",
            null,
            "quality",
            "technology",
            "rows",
            2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "billy", null, "quality", "travel", "rows", 2L)
    );

    Map<String, Object> context = new HashMap<>();
    QueryRunner<Row> mergeRunner = factory.getToolchest().mergeResults(runner);
    TestHelper.assertExpectedObjects(expectedResults, mergeRunner.run(QueryPlus.wrap(query), context), "no-limit");
  }

  // A subquery identical to the query should yield identical results
  @Test
  public void testIdenticalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(new LongSumAggregatorFactory("rows", "rows"), new LongSumAggregatorFactory("idx", "idx"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "sub-query");
  }

  @Test
  public void testSubqueryWithMultipleIntervalsInOuterQuery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-multiple-intervals");
  }

  @Test
  public void testSubqueryWithMultipleIntervalsInOuterQueryAndChunkPeriod()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of("chunkPeriod", "P1D"))
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-multiple-intervals");
  }

  @Test
  public void testSubqueryWithExtractionFnInOuterQuery()
  {
    //https://github.com/apache/incubator-druid/issues/2556

    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "a", "rows", 13L, "idx", 6619L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "a", "rows", 13L, "idx", 5827L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-extractionfn");
  }

  @Test
  public void testDifferentGroupingSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new DoubleMaxAggregatorFactory("idx", "idx"),
            new DoubleMaxAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
        new String[]{"__time", "rows", "idx", "indexMaxPlusTen"},
        new Object[]{"2011-04-01", 9L, 2900.0, 2930.0},
        new Object[]{"2011-04-02", 9L, 2505.0, 2535.0}
    );

    TestHelper.assertExpectedObjects(
        expectedResults,
        GroupByQueryRunnerTestHelper.runQuery(factory, runner, query), "subquery"
    );

    subquery = new GroupByQuery.Builder(subquery)
        .setVirtualColumns(
            new ExpressionVirtualColumn("expr", "-index + 100", ValueType.FLOAT, TestExprMacroTable.INSTANCE)
        )
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "expr"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .build();
    query = (GroupByQuery) query.withDataSource(new QueryDataSource(subquery));

    expectedResults = GroupByQueryRunnerTestHelper.createExpectedRows(
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
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            new DoubleMaxAggregatorFactory("idx1", "idx"),
            new DoubleMaxAggregatorFactory("idx2", "idx"),
            new DoubleMaxAggregatorFactory("idx3", "post_agg"),
            new DoubleMaxAggregatorFactory("idx4", "post_agg")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "idx1", 2900.0, "idx2", 2900.0,
                                                       "idx3", 5800.0, "idx4", 5800.0
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx1", 2505.0, "idx2", 2505.0,
                                                       "idx3", 5010.0, "idx4", 5010.0
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-multiple-aggs");
  }


  @Test
  public void testDifferentGroupingSubqueryWithFilter()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "idx", 2900.0),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-filter");
  }

  @Test
  public void testDifferentIntervalSubquery()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.secondOnly)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "idx", 2505.0)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-different-intervals");
  }

  @Test
  public void testGroupByTimeExtractionNamedUnderUnderTime()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage(
        "'__time' cannot be used as an output name for dimensions, aggregators, or post-aggregators."
    );

    GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(
            new DefaultDimensionSpec("market", "market"),
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                ColumnHolder.TIME_COLUMN_NAME,
                new TimeFormatExtractionFn("EEEE", null, null, null, false)
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .setPostAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(QueryRunnerTestHelper.allGran)
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

    GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "__time"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
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
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.emptyInterval)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertFalse(results.iterator().hasNext());
  }

  @Test
  public void testSubqueryWithPostAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx_subagg", "index"))
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-postaggs");
  }

  @Test
  public void testSubqueryWithPostAggregatorsAndHaving()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "quality",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx_subagg", "index"))
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
            new BaseHavingSpec()
            {
              @Override
              public boolean eval(Row row)
              {
                return (row.getMetric("idx_subpostagg").floatValue() < 3800);
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-postaggs");
  }

  @Test
  public void testSubqueryWithMultiColumnAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new JavaScriptDimFilter(
            "market",
            "function(dim){ return true; }",
            null,
            JavaScriptConfig.getEnabledInstance()
        ))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
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
            new BaseHavingSpec()
            {
              @Override
              public boolean eval(Row row)
              {
                return (row.getMetric("idx_subpostagg").floatValue() < 3800);
              }
            }
        )
        .addOrderByColumn("alias")
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-multi-aggs");
  }

  @Test
  public void testSubqueryWithOuterFilterAggregator()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final DimFilter filter = new SelectorDimFilter("market", "spot", null);
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(Collections.emptyList())
        .setAggregatorSpecs(new FilteredAggregatorFactory(QueryRunnerTestHelper.rowsCount, filter))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01", "rows", 837L)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-filter-agg");
  }

  @Test
  public void testSubqueryWithOuterTimeFilter()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
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
    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(Collections.emptyList())
        .setDimFilter(firstDaysFilter)
        .setAggregatorSpecs(new FilteredAggregatorFactory(QueryRunnerTestHelper.rowsCount, fridayFilter))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-02-01", "rows", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-02-02", "rows", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-02-03", "rows", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-01", "rows", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-02", "rows", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-03", "rows", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "rows", 13L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "rows", 0L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-03", "rows", 0L)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-time-filter");
  }

  @Test
  public void testSubqueryWithContextTimeout()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new ArrayList<>()).setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setContext(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 10000))
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "count", 18L)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-timeout");
  }

  @Test
  public void testSubqueryWithOuterVirtualColumns()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setVirtualColumns(new ExpressionVirtualColumn("expr", "1", ValueType.FLOAT, TestExprMacroTable.INSTANCE))
        .setDimensions(new ArrayList<>()).setAggregatorSpecs(new LongSumAggregatorFactory("count", "expr"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "count", 18L)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-virtual");
  }

  @Test
  public void testSubqueryWithOuterCardinalityAggregator()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(Collections.emptyList())
        .setAggregatorSpecs(new CardinalityAggregatorFactory(
            "car",
            ImmutableList.of(new DefaultDimensionSpec("quality", "quality")),
            false
        ))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow("1970-01-01", "car", QueryRunnerTestHelper.UNIQUES_9)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-cardinality");
  }

  @Test
  public void testSubqueryWithOuterCountAggregator()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setLimitSpec(
            new DefaultLimitSpec(
                ImmutableList.of(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.ASCENDING)),
                null
            )
        )
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new ArrayList<>()).setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    // v1 strategy throws an exception for this query because it tries to merge the noop outer
    // and default inner limit specs, then apply the resulting spec to the outer query, which
    // fails because the inner limit spec refers to columns that don't exist in the outer
    // query. I'm not sure why it does this.

    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(ISE.class);
      expectedException.expectMessage("Unknown column in order clause");
      GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    } else {
      List<Row> expectedResults = Collections.singletonList(
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "count", 18L)
      );
      Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
      TestHelper.assertExpectedObjects(expectedResults, results, "subquery-count-agg");
    }
  }

  @Test
  public void testSubqueryWithOuterDimJavascriptAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(new JavaScriptAggregatorFactory(
            "js_agg",
            Arrays.asList("index", "market"),
            "function(current, index, dim){return current + index + dim.length;}",
            "function(){return 0;}",
            "function(a,b){return a + b;}",
            JavaScriptConfig.getEnabledInstance()
        ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "js_agg", 139D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "business", "js_agg", 122D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "entertainment", "js_agg", 162D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "health", "js_agg", 124D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "mezzanine", "js_agg", 2893D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "news", "js_agg", 125D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "premium", "js_agg", 2923D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "technology", "js_agg", 82D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "travel", "js_agg", 123D),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "automotive", "js_agg", 151D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "business", "js_agg", 116D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "entertainment", "js_agg", 170D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "health", "js_agg", 117D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "mezzanine", "js_agg", 2470D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "news", "js_agg", 118D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "premium", "js_agg", 2528D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "technology", "js_agg", 101D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "travel", "js_agg", 130D)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-javascript");
  }

  @Test
  public void testSubqueryWithOuterJavascriptAggregators()
  {
    final GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("market", "market"), new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("index", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    final GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "quality"))
        .setAggregatorSpecs(new JavaScriptAggregatorFactory(
            "js_agg",
            Arrays.asList("index", "rows"),
            "function(current, index, rows){return current + index + rows;}",
            "function(){return 0;}",
            "function(a,b){return a + b;}",
            JavaScriptConfig.getEnabledInstance()
        ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "automotive", "js_agg", 136D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "business", "js_agg", 119D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "entertainment", "js_agg", 159D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "health", "js_agg", 121D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "mezzanine", "js_agg", 2873D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "news", "js_agg", 122D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "premium", "js_agg", 2903D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "technology", "js_agg", 79D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "quality", "travel", "js_agg", 120D),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "automotive", "js_agg", 148D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "business", "js_agg", 113D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "entertainment", "js_agg", 167D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "health", "js_agg", 114D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "mezzanine", "js_agg", 2450D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "news", "js_agg", 115D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "premium", "js_agg", 2508D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "technology", "js_agg", 98D),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "quality", "travel", "js_agg", 127D)
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-javascript");
  }

  @Test
  public void testSubqueryWithHyperUniques()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new HyperUniquesAggregatorFactory("quality_uniques", "quality_uniques")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"))
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("rows", "rows"),
            new LongSumAggregatorFactory("idx", "idx"),
            new HyperUniquesAggregatorFactory("uniq", "quality_uniques")
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-hyperunique");
  }

  @Test
  public void testSubqueryWithHyperUniquesPostAggregator()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new ArrayList<>())
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new HyperUniquesAggregatorFactory("quality_uniques_inner", "quality_uniques")
        )
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new FieldAccessPostAggregator("quality_uniques_inner_post", "quality_uniques_inner")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
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
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-hyperunique");
  }

  @Test
  public void testSubqueryWithFirstLast()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            new LongFirstAggregatorFactory("innerfirst", "index"),
            new LongLastAggregatorFactory("innerlast", "index")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setContext(ImmutableMap.of("finalize", true))
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(Collections.emptyList())
        .setAggregatorSpecs(
            new LongFirstAggregatorFactory("first", "innerfirst"),
            new LongLastAggregatorFactory("last", "innerlast")
        )
        .setGranularity(QueryRunnerTestHelper.monthGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-01-01", "first", 100L, "last", 943L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-02-01", "first", 132L, "last", 1101L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-03-01", "first", 153L, "last", 1063L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "first", 135L, "last", 780L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subquery-firstlast");
  }

  @Test
  public void testGroupByWithSubtotalsSpec()
  {
    if (!config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      return;
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
                new DoubleSumAggregatorFactory("idxDouble", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias"),
            ImmutableList.of("market"),
            ImmutableList.of()
        ))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            643.043177,
            "idxFloat",
            643.043212890625,
            "rows",
            5L,
            "idx",
            640L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "total_market",
            "idxDouble",
            1314.839715,
            "idxFloat",
            1314.8397,
            "rows",
            1L,
            "idx",
            1314L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "upfront",
            "idxDouble",
            1447.34116,
            "idxFloat",
            1447.3412,
            "rows",
            1L,
            "idx",
            1447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            266.090949,
            "idxFloat",
            266.0909423828125,
            "rows",
            2L,
            "idx",
            265L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "total_market",
            "idxDouble",
            1522.043733,
            "idxFloat",
            1522.0437,
            "rows",
            1L,
            "idx",
            1522L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "upfront",
            "idxDouble",
            1234.247546,
            "idxFloat",
            1234.2476,
            "rows",
            1L,
            "idx",
            1234L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            198.545289,
            "idxFloat",
            198.5452880859375,
            "rows",
            2L,
            "idx",
            197L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            650.806953,
            "idxFloat",
            650.8069458007812,
            "rows",
            5L,
            "idx",
            648L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "total_market",
            "idxDouble",
            1193.556278,
            "idxFloat",
            1193.5563,
            "rows",
            1L,
            "idx",
            1193L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "upfront",
            "idxDouble",
            1144.342401,
            "idxFloat",
            1144.3424,
            "rows",
            1L,
            "idx",
            1144L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            249.591647,
            "idxFloat",
            249.59164428710938,
            "rows",
            2L,
            "idx",
            249L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "total_market",
            "idxDouble",
            1321.375057,
            "idxFloat",
            1321.375,
            "rows",
            1L,
            "idx",
            1321L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "upfront",
            "idxDouble",
            1049.738585,
            "idxFloat",
            1049.7385,
            "rows",
            1L,
            "idx",
            1049L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            223.798797,
            "idxFloat",
            223.79879760742188,
            "rows",
            2L,
            "idx",
            223L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            6626.151575318359,
            "idxFloat",
            6626.152f,
            "rows",
            13L,
            "idx",
            6619L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            5833.209713,
            "idxFloat",
            5833.209f,
            "rows",
            13L,
            "idx",
            5827L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal");
  }

  @Test
  public void testGroupByWithSubtotalsSpecWithLongDimensionColumn()
  {
    if (!config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      return;
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("qualityLong", "ql", ValueType.LONG),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
                new DoubleSumAggregatorFactory("idxDouble", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("ql"),
            ImmutableList.of("market"),
            ImmutableList.of()
        ))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            135.885094,
            "idxFloat",
            135.8851,
            "ql",
            1000L,
            "rows",
            1L,
            "idx",
            135L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            118.57034,
            "idxFloat",
            118.57034,
            "ql",
            1100L,
            "rows",
            1L,
            "idx",
            118L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            158.747224,
            "idxFloat",
            158.74722,
            "ql",
            1200L,
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            120.134704,
            "idxFloat",
            120.134705,
            "ql",
            1300L,
            "rows",
            1L,
            "idx",
            120L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            2871.8866900000003,
            "idxFloat",
            2871.88671875,
            "ql",
            1400L,
            "rows",
            3L,
            "idx",
            2870L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            121.583581,
            "idxFloat",
            121.58358,
            "ql",
            1500L,
            "rows",
            1L,
            "idx",
            121L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            2900.798647,
            "idxFloat",
            2900.798583984375,
            "ql",
            1600L,
            "rows",
            3L,
            "idx",
            2900L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            78.622547,
            "idxFloat",
            78.62254,
            "ql",
            1700L,
            "rows",
            1L,
            "idx",
            78L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            119.922742,
            "idxFloat",
            119.922745,
            "ql",
            1800L,
            "rows",
            1L,
            "idx",
            119L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            147.425935,
            "idxFloat",
            147.42593,
            "ql",
            1000L,
            "rows",
            1L,
            "idx",
            147L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            112.987027,
            "idxFloat",
            112.98703,
            "ql",
            1100L,
            "rows",
            1L,
            "idx",
            112L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            166.016049,
            "idxFloat",
            166.01605,
            "ql",
            1200L,
            "rows",
            1L,
            "idx",
            166L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            113.446008,
            "idxFloat",
            113.44601,
            "ql",
            1300L,
            "rows",
            1L,
            "idx",
            113L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            2448.830613,
            "idxFloat",
            2448.83056640625,
            "ql",
            1400L,
            "rows",
            3L,
            "idx",
            2447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            114.290141,
            "idxFloat",
            114.29014,
            "ql",
            1500L,
            "rows",
            1L,
            "idx",
            114L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            2506.415148,
            "idxFloat",
            2506.4150390625,
            "ql",
            1600L,
            "rows",
            3L,
            "idx",
            2505L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            97.387433,
            "idxFloat",
            97.387436,
            "ql",
            1700L,
            "rows",
            1L,
            "idx",
            97L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            126.411364,
            "idxFloat",
            126.41136,
            "ql",
            1800L,
            "rows",
            1L,
            "idx",
            126L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            643.043177,
            "idxFloat",
            643.043212890625,
            "rows",
            5L,
            "idx",
            640L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "total_market",
            "idxDouble",
            1314.839715,
            "idxFloat",
            1314.8397,
            "rows",
            1L,
            "idx",
            1314L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "upfront",
            "idxDouble",
            1447.34116,
            "idxFloat",
            1447.3412,
            "rows",
            1L,
            "idx",
            1447L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            266.090949,
            "idxFloat",
            266.0909423828125,
            "rows",
            2L,
            "idx",
            265L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "total_market",
            "idxDouble",
            1522.043733,
            "idxFloat",
            1522.0437,
            "rows",
            1L,
            "idx",
            1522L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "upfront",
            "idxDouble",
            1234.247546,
            "idxFloat",
            1234.2476,
            "rows",
            1L,
            "idx",
            1234L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            198.545289,
            "idxFloat",
            198.5452880859375,
            "rows",
            2L,
            "idx",
            197L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            650.806953,
            "idxFloat",
            650.8069458007812,
            "rows",
            5L,
            "idx",
            648L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "total_market",
            "idxDouble",
            1193.556278,
            "idxFloat",
            1193.5563,
            "rows",
            1L,
            "idx",
            1193L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "upfront",
            "idxDouble",
            1144.342401,
            "idxFloat",
            1144.3424,
            "rows",
            1L,
            "idx",
            1144L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            249.591647,
            "idxFloat",
            249.59164428710938,
            "rows",
            2L,
            "idx",
            249L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "total_market",
            "idxDouble",
            1321.375057,
            "idxFloat",
            1321.375,
            "rows",
            1L,
            "idx",
            1321L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "upfront",
            "idxDouble",
            1049.738585,
            "idxFloat",
            1049.7385,
            "rows",
            1L,
            "idx",
            1049L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            223.798797,
            "idxFloat",
            223.79879760742188,
            "rows",
            2L,
            "idx",
            223L
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            6626.151569,
            "idxFloat",
            6626.1513671875,
            "rows",
            13L,
            "idx",
            6619L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02T00:00:00.000Z",
            "idxDouble",
            5833.209717999999,
            "idxFloat",
            5833.20849609375,
            "rows",
            13L,
            "idx",
            5827L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);

    for (Row row : results) {
      System.out.println(row);
    }
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal-long-dim");
  }

  @Test
  public void testGroupByWithSubtotalsSpecWithOrderLimit()
  {
    if (!config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      return;
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.newArrayList(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("market", "market")
        ))
        .setAggregatorSpecs(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                new LongSumAggregatorFactory("idx", "index"),
                new FloatSumAggregatorFactory("idxFloat", "indexFloat"),
                new DoubleSumAggregatorFactory("idxDouble", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setSubtotalsSpec(ImmutableList.of(
            ImmutableList.of("alias"),
            ImmutableList.of("market"),
            ImmutableList.of()
        ))
        .addOrderByColumn("idxDouble")
        .setLimit(1)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "market",
            "spot",
            "idxDouble",
            198.545289,
            "idxFloat",
            198.5452880859375,
            "rows",
            2L,
            "idx",
            197L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01T00:00:00.000Z",
            "idxDouble",
            6626.151575318359,
            "idxFloat",
            6626.152f,
            "rows",
            13L,
            "idx",
            6619L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "subtotal-order-limit");
  }

  @Test
  public void testGroupByWithTimeColumn()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
            QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
            QueryRunnerTestHelper.__timeLongSum
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "rows",
            26L,
            "ntimestamps",
            13.0,
            "sumtime",
            33843139200000L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "time");
  }

  @Test
  public void testGroupByTimeExtraction()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setDimensions(
            new DefaultDimensionSpec("market", "market"),
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null, null, false)
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .setPostAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimFilter(
            new OrDimFilter(
                Arrays.asList(
                    new SelectorDimFilter("market", "spot", null),
                    new SelectorDimFilter("market", "upfront", null)
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "time-extraction");
  }


  @Test
  public void testGroupByTimeExtractionWithNulls()
  {
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

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.fullOnIntervalSpec)
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
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, QueryRunnerTestHelper.indexDoubleSum)
        .setPostAggregatorSpecs(Collections.singletonList(QueryRunnerTestHelper.addRowsIndexConstant))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimFilter(
            new OrDimFilter(
                Arrays.asList(
                    new SelectorDimFilter("market", "spot", null),
                    new SelectorDimFilter("market", "upfront", null)
                )
            )
        )
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "time-extraction");
  }

  @Test
  public void testBySegmentResults()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<>(
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass<>(
            Collections.singletonList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ),
            QueryRunnerTestHelper.segmentId.toString(),
            Intervals.of("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = new ArrayList<>();
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
        theRunner.run(QueryPlus.wrap(fullQuery), new HashMap<>()),
        "bySegment"
    );
    exec.shutdownNow();
  }


  @Test
  public void testBySegmentResultsUnOptimizedDimextraction()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<>(
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass<>(
            Collections.singletonList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine0",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ),
            QueryRunnerTestHelper.segmentId.toString(),
            Intervals.of("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
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
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = new ArrayList<>();
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
        theRunner.run(QueryPlus.wrap(fullQuery), new HashMap<>()),
        "bySegment"
    );
    exec.shutdownNow();
  }

  @Test
  public void testBySegmentResultsOptimizedDimextraction()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<>(
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass<>(
            Collections.singletonList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine0",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ),
            QueryRunnerTestHelper.segmentId.toString(),
            Intervals.of("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
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
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(new SelectorDimFilter("quality", "mezzanine", null))
        .setContext(ImmutableMap.of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = new ArrayList<>();
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
        theRunner.run(QueryPlus.wrap(fullQuery), new HashMap<>()),
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

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                                     .setAggregatorSpecs(
                                         QueryRunnerTestHelper.rowsCount,
                                         new LongSumAggregatorFactory("idx", "index")
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(new OrDimFilter(dimFilters))
                                     .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 1L, "idx", 147L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "business", "rows", 1L, "idx", 112L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 1L, "idx", 166L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "health", "rows", 1L, "idx", 113L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
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
    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                                     .setAggregatorSpecs(
                                         QueryRunnerTestHelper.rowsCount,
                                         new LongSumAggregatorFactory("idx", "index")
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(new ExtractionDimFilter("quality", "", lookupExtractionFn, null))
                                     .build();

    List<Row> expectedResults;

    if (NullHandling.replaceWithDefault()) {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L)
      );
    } else {
      // Only empty string should match, nulls will not match
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
          GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L)
      );
    }

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "dim-extraction");
  }

  @Test
  public void testGroupByWithExtractionDimFilterWhenSearchValueNotInTheMap()
  {
    Map<String, String> extractionMap = new HashMap<>();
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                                     .setAggregatorSpecs(
                                         QueryRunnerTestHelper.rowsCount,
                                         new LongSumAggregatorFactory("idx", "index")
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(
                                         new ExtractionDimFilter("quality", "NOT_THERE", lookupExtractionFn, null)
                                     )
                                     .build();
    List<Row> expectedResults = Collections.emptyList();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
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

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
                                     .setAggregatorSpecs(
                                         QueryRunnerTestHelper.rowsCount,
                                         new LongSumAggregatorFactory("idx", "index")
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(
                                         new ExtractionDimFilter(
                                             "null_column",
                                             "REPLACED_VALUE",
                                             lookupExtractionFn,
                                             null
                                         )
                                     ).build();

    List<Row> expectedResults = Arrays
        .asList(
            GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
            GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
        );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
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
    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                                     .setAggregatorSpecs(new FilteredAggregatorFactory(
                                         QueryRunnerTestHelper.rowsCount,
                                         filter
                                     ), new FilteredAggregatorFactory(
                                         new LongSumAggregatorFactory("idx", "index"),
                                         filter
                                     ))
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "automotive",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "business",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "entertainment",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "health",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "premium",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "technology",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "travel",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),

        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "automotive",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "business",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "entertainment",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "health",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "technology",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "travel",
            "rows",
            0L,
            "idx",
            NullHandling.defaultLongValue()
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
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
    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(new DefaultDimensionSpec("quality", "alias"))
                                     .setAggregatorSpecs(
                                         QueryRunnerTestHelper.rowsCount,
                                         new LongSumAggregatorFactory("idx", "index")
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(
                                         new ExtractionDimFilter(
                                             "quality",
                                             "newsANDmezzanine",
                                             lookupExtractionFn,
                                             null
                                         )
                                     )
                                     .build();
    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 3L, "idx", 2447L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 1L, "idx", 114L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-dim-filter");
  }


  @Test
  public void testGroupByWithExtractionDimFilterNullDims()
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

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .setDimFilter(new ExtractionDimFilter("null_column", "EMPTY", lookupExtractionFn, null))
        .build();
    List<Row> expectedResults = Arrays
        .asList(
            GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
            GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
        );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-dim-filter");
  }

  @Test
  public void testBySegmentResultsWithAllFiltersWithExtractionFns()
  {
    int segmentCount = 32;
    Result<BySegmentResultValue> singleSegmentResult = new Result<>(
        DateTimes.of("2011-01-12T00:00:00.000Z"),
        new BySegmentResultValueClass<>(
            Collections.singletonList(
                GroupByQueryRunnerTestHelper.createExpectedRow(
                    "2011-04-01",
                    "alias",
                    "mezzanine",
                    "rows",
                    6L,
                    "idx",
                    4420L
                )
            ),
            QueryRunnerTestHelper.segmentId.toString(),
            Intervals.of("2011-04-02T00:00:00.000Z/2011-04-04T00:00:00.000Z")
        )
    );
    List<Result> bySegmentResults = new ArrayList<>();
    for (int i = 0; i < segmentCount; i++) {
      bySegmentResults.add(singleSegmentResult);
    }

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

    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setDimFilter(superFilter)
        .setContext(ImmutableMap.of("bySegment", true));
    final GroupByQuery fullQuery = builder.build();
    QueryToolChest toolChest = factory.getToolchest();

    List<QueryRunner<Row>> singleSegmentRunners = new ArrayList<>();
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
        theRunner.run(QueryPlus.wrap(fullQuery), new HashMap<>()),
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
    superFilterList.add(new JavaScriptDimFilter(
        "null_column",
        jsFn,
        extractionFn,
        JavaScriptConfig.getEnabledInstance()
    ));
    DimFilter superFilter = new AndDimFilter(superFilterList);

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource(QueryRunnerTestHelper.dataSource)
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
                                     .setDimensions(new DefaultDimensionSpec("null_column", "alias"))
                                     .setAggregatorSpecs(
                                         QueryRunnerTestHelper.rowsCount,
                                         new LongSumAggregatorFactory("idx", "index")
                                     )
                                     .setGranularity(QueryRunnerTestHelper.dayGran)
                                     .setDimFilter(superFilter).build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", null, "rows", 13L, "idx", 6619L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", null, "rows", 13L, "idx", 5827L)
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction");
  }

  @Test
  public void testGroupByCardinalityAggWithExtractionFn()
  {
    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("market", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new CardinalityAggregatorFactory(
            "numVals",
            ImmutableList.of(new ExtractionDimensionSpec(
                QueryRunnerTestHelper.qualityDimension,
                QueryRunnerTestHelper.qualityDimension,
                helloFn
            )),
            false
        ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "spot",
            "rows",
            9L,
            "numVals",
            1.0002442201269182d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "total_market",
            "rows",
            2L,
            "numVals",
            1.0002442201269182d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "upfront",
            "rows",
            2L,
            "numVals",
            1.0002442201269182d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "spot",
            "rows",
            9L,
            "numVals",
            1.0002442201269182d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "total_market",
            "rows",
            2L,
            "numVals",
            1.0002442201269182d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "upfront",
            "rows",
            2L,
            "numVals",
            1.0002442201269182d
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "cardinality-agg");
  }

  @Test
  public void testGroupByCardinalityAggOnFloat()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("market", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new CardinalityAggregatorFactory(
            "numVals",
            ImmutableList.of(new DefaultDimensionSpec(
                QueryRunnerTestHelper.indexMetric,
                QueryRunnerTestHelper.indexMetric
            )),
            false
        ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "spot",
            "rows",
            9L,
            "numVals",
            8.015665809687173d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "total_market",
            "rows",
            2L,
            "numVals",
            2.000977198748901d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "upfront",
            "rows",
            2L,
            "numVals",
            2.000977198748901d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "spot",
            "rows",
            9L,
            "numVals",
            9.019833517963864d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "total_market",
            "rows",
            2L,
            "numVals",
            2.000977198748901d
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "upfront",
            "rows",
            2L,
            "numVals",
            2.000977198748901d
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "cardinality-agg");
  }

  @Test
  public void testGroupByLongColumn()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("qualityLong", "ql_alias", ValueType.LONG))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "ql_alias",
            OrderByColumnSpec.Direction.ASCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    Assert.assertEquals(
        Functions.<Sequence<Row>>identity(),
        query.getLimitSpec().build(
            query.getDimensions(),
            query.getAggregatorSpecs(),
            query.getPostAggregatorSpecs(),
            query.getGranularity(),
            query.getContextSortByDimsFirst()
        )
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "ql_alias",
            1200L,
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "ql_alias",
            1200L,
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long");
  }

  @Test
  public void testGroupByLongColumnDescending()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("qualityLong", "ql_alias", ValueType.LONG))
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "ql_alias",
            OrderByColumnSpec.Direction.DESCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    Assert.assertNotEquals(
        Functions.<Sequence<Row>>identity(),
        query.getLimitSpec().build(
            query.getDimensions(),
            query.getAggregatorSpecs(),
            query.getPostAggregatorSpecs(),
            query.getGranularity(),
            query.getContextSortByDimsFirst()
        )
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "ql_alias",
            1700L,
            "rows",
            2L,
            "idx",
            175L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "ql_alias",
            1200L,
            "rows",
            2L,
            "idx",
            324L
        )
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long");
  }

  @Test
  public void testGroupByLongColumnWithExFn()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }

    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new ExtractionDimensionSpec("qualityLong", "ql_alias", jsExtractionFn))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "ql_alias",
            "super-1200",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "ql_alias",
            "super-1200",
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-extraction");
  }

  @Test
  public void testGroupByLongTimeColumn()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("__time", "time_alias", ValueType.LONG))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "time_alias",
            1301616000000L,
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "time_alias",
            1301702400000L,
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long");
  }

  @Test
  public void testGroupByLongTimeColumnWithExFn()
  {
    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new ExtractionDimensionSpec("__time", "time_alias", jsExtractionFn))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "time_alias",
            "super-1301616000000",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "time_alias",
            "super-1301702400000",
            "rows",
            1L,
            "idx",
            166L
        )
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-extraction");
  }

  @Test
  public void testGroupByFloatColumn()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("index", "index_alias", ValueType.FLOAT))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "index_alias",
            OrderByColumnSpec.Direction.ASCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    Assert.assertEquals(
        Functions.<Sequence<Row>>identity(),
        query.getLimitSpec().build(
            query.getDimensions(),
            query.getAggregatorSpecs(),
            query.getPostAggregatorSpecs(),
            query.getGranularity(),
            query.getContextSortByDimsFirst()
        )
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "index_alias",
            158.747224f,
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "index_alias",
            166.016049f,
            "rows",
            1L,
            "idx",
            166L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "float");
  }

  @Test
  public void testGroupByFloatColumnDescending()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("qualityFloat", "qf_alias", ValueType.FLOAT))
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "qf_alias",
            OrderByColumnSpec.Direction.DESCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    Assert.assertNotEquals(
        Functions.<Sequence<Row>>identity(),
        query.getLimitSpec().build(
            query.getDimensions(),
            query.getAggregatorSpecs(),
            query.getPostAggregatorSpecs(),
            query.getGranularity(),
            query.getContextSortByDimsFirst()
        )
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "qf_alias",
            17000.0f,
            "rows",
            2L,
            "idx",
            175L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "qf_alias",
            12000.0f,
            "rows",
            2L,
            "idx",
            324L
        )
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "float");
  }

  @Test
  public void testGroupByDoubleColumnDescending()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("qualityDouble", "alias", ValueType.DOUBLE))
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .addOrderByColumn(new OrderByColumnSpec(
            "alias",
            OrderByColumnSpec.Direction.DESCENDING,
            StringComparators.NUMERIC
        ))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    Assert.assertNotEquals(
        Functions.<Sequence<Row>>identity(),
        query.getLimitSpec().build(
            query.getDimensions(),
            query.getAggregatorSpecs(),
            query.getPostAggregatorSpecs(),
            query.getGranularity(),
            query.getContextSortByDimsFirst()
        )
    );

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            17000.0d,
            "rows",
            2L,
            "idx",
            175L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            12000.0d,
            "rows",
            2L,
            "idx",
            324L
        )
    );
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "double");
  }

  @Test
  public void testGroupByFloatColumnWithExFn()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }

    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new ExtractionDimensionSpec("index", "index_alias", jsExtractionFn))
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults;

    expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "index_alias",
            "super-158.747224",
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "index_alias",
            "super-166.016049",
            "rows",
            1L,
            "idx",
            166L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "float");
  }

  @Test
  public void testGroupByWithHavingSpecOnLongAndFloat()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("market", "alias"),
            new DefaultDimensionSpec("qualityLong", "ql_alias", ValueType.LONG),
            new DefaultDimensionSpec("__time", "time_alias", ValueType.LONG),
            new DefaultDimensionSpec("index", "index_alias", ValueType.FLOAT)
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
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
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias", "total_market",
            "time_alias", 1301616000000L,
            "index_alias", 1314.8397,
            "ql_alias", 1400L,
            "rows", 1L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "havingspec-long-float");
  }

  @Test
  public void testGroupByLongAndFloatOutputAsString()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("qualityLong", "ql_alias"),
            new DefaultDimensionSpec("qualityFloat", "qf_alias")
        )
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "long-float");
  }

  @Test
  public void testGroupByNumericStringsAsNumeric()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 does not support dimension selectors with unknown cardinality.");
    }

    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("qualityLong", "ql_alias"),
            new DefaultDimensionSpec("qualityFloat", "qf_alias"),
            new DefaultDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, "time_alias")
        )
        .setDimFilter(new SelectorDimFilter("quality", "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery outerQuery = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("time_alias", "time_alias2", ValueType.LONG),
            new DefaultDimensionSpec("ql_alias", "ql_alias_long", ValueType.LONG),
            new DefaultDimensionSpec("qf_alias", "qf_alias_float", ValueType.FLOAT),
            new DefaultDimensionSpec("ql_alias", "ql_alias_float", ValueType.FLOAT)
        ).setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "time_alias2", 1301616000000L,
            "ql_alias_long", 1200L,
            "qf_alias_float", 12000.0,
            "ql_alias_float", 1200.0,
            "count", 1L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "time_alias2", 1301702400000L,
            "ql_alias_long", 1200L,
            "qf_alias_float", 12000.0,
            "ql_alias_float", 1200.0,
            "count", 1L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "numeric-string");
  }

  @Test
  public void testGroupByNumericStringsAsNumericWithDecoration()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    // rows with `technology` have `170000` in the qualityNumericString field
    RegexFilteredDimensionSpec regexSpec = new RegexFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityNumericString", "ql", ValueType.LONG),
        "170000"
    );

    ListFilteredDimensionSpec listFilteredSpec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityNumericString", "qf", ValueType.FLOAT),
        Sets.newHashSet("170000"),
        true
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(regexSpec, listFilteredSpec)
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .addOrderByColumn("ql")
        .build();

    List<Row> expectedResults;
    // "entertainment" rows are excluded by the decorated specs, they become empty rows
    expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "ql", NullHandling.defaultLongValue(),
            "qf", NullHandling.defaultDoubleValue(),
            "count", 2L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "ql", 170000L,
            "qf", 170000.0,
            "count", 2L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "numeric-string");
  }

  @Test
  public void testGroupByDecorationOnNumerics()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    RegexFilteredDimensionSpec regexSpec = new RegexFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityLong", "ql", ValueType.LONG),
        "1700"
    );

    ListFilteredDimensionSpec listFilteredSpec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityFloat", "qf", ValueType.FLOAT),
        Sets.newHashSet("17000.0"),
        true
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(regexSpec, listFilteredSpec)
        .setDimFilter(new InDimFilter("quality", Arrays.asList("entertainment", "technology"), null))
        .setAggregatorSpecs(new CountAggregatorFactory("count"))
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();
    List<Row> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-01",
              "ql", 0L,
              "qf", 0.0,
              "count", 2L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-01",
              "ql", 1700L,
              "qf", 17000.0,
              "count", 2L
          )
      );
    } else {
      expectedResults = Arrays.asList(
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-01",
              "ql", null,
              "qf", null,
              "count", 2L
          ),
          GroupByQueryRunnerTestHelper.createExpectedRow(
              "2011-04-01",
              "ql", 1700L,
              "qf", 17000.0,
              "count", 2L
          )
      );
    }

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "numeric");
  }

  @Test
  public void testGroupByNestedWithInnerQueryNumerics()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("quality", "alias"),
            new DefaultDimensionSpec("qualityLong", "ql_alias", ValueType.LONG),
            new DefaultDimensionSpec("qualityFloat", "qf_alias", ValueType.FLOAT)
        )
        .setDimFilter(
            new InDimFilter(
                "quality",
                Collections.singletonList("entertainment"),
                null
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery outerQuery = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("ql_alias", "quallong", ValueType.LONG),
            new DefaultDimensionSpec("qf_alias", "qualfloat", ValueType.FLOAT)
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
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "quallong", 1200L,
            "qualfloat", 12000.0,
            "ql_alias_sum", 2400L,
            "qf_alias_sum", 24000.0
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "numerics");
  }

  @Test
  public void testGroupByNestedWithInnerQueryNumericsWithLongTime()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery subQuery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("market", "alias"),
            new DefaultDimensionSpec("__time", "time_alias", ValueType.LONG),
            new DefaultDimensionSpec("index", "index_alias", ValueType.FLOAT)
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    GroupByQuery outerQuery = GroupByQuery
        .builder()
        .setDataSource(subQuery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("alias", "market"),
            new DefaultDimensionSpec("time_alias", "time_alias2", ValueType.LONG)
        )
        .setAggregatorSpecs(
            new LongMaxAggregatorFactory("time_alias_max", "time_alias"),
            new DoubleMaxAggregatorFactory("index_alias_max", "index_alias")
        )
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "market", "spot",
            "time_alias2", 1301616000000L,
            "time_alias_max", 1301616000000L,
            "index_alias_max", 158.74722290039062
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "market", "spot",
            "time_alias2", 1301702400000L,
            "time_alias_max", 1301702400000L,
            "index_alias_max", 166.01605224609375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "market", "total_market",
            "time_alias2", 1301616000000L,
            "time_alias_max", 1301616000000L,
            "index_alias_max", 1522.043701171875
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "market", "total_market",
            "time_alias2", 1301702400000L,
            "time_alias_max", 1301702400000L,
            "index_alias_max", 1321.375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "market", "upfront",
            "time_alias2", 1301616000000L,
            "time_alias_max", 1301616000000L,
            "index_alias_max", 1447.3411865234375
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "market", "upfront",
            "time_alias2", 1301702400000L,
            "time_alias_max", 1301702400000L,
            "index_alias_max", 1144.3424072265625
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "numerics");
  }

  @Test
  public void testGroupByStringOutputAsLong()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    ExtractionFn strlenFn = StrlenExtractionFn.instance();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new ExtractionDimensionSpec(
            QueryRunnerTestHelper.qualityDimension,
            "alias",
            ValueType.LONG,
            strlenFn
        ))
        .setDimFilter(new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "entertainment", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            13L,
            "rows",
            1L,
            "idx",
            158L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            13L,
            "rows",
            1L,
            "idx",
            166L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "string-long");
  }

  @Test
  public void testGroupByWithAggsOnNumericDimensions()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setDimFilter(new SelectorDimFilter("quality", "technology", null))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.rowsCount,
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
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
        GroupByQueryRunnerTestHelper.createExpectedRow(
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

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "numeric-dims");
  }

  @Test
  public void testGroupByNestedOuterExtractionFnOnFloatInner()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    String jsFn = "function(obj) { return obj; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"), new ExtractionDimensionSpec(
            "qualityFloat",
            "qf_inner",
            ValueType.FLOAT,
            jsExtractionFn
        ))
        .setDimFilter(new SelectorDimFilter("quality", "technology", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery outerQuery = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"), new ExtractionDimensionSpec(
            "qf_inner",
            "qf_outer",
            ValueType.FLOAT,
            jsExtractionFn
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias", "technology",
            "qf_outer", 17000.0f,
            "rows", 2L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-fn");
  }

  @Test
  public void testGroupByNestedDoubleTimeExtractionFnWithLongOutputTypes()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("quality", "alias"),
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                "time_day",
                ValueType.LONG,
                new TimeFormatExtractionFn(null, null, null, Granularities.DAY, true)
            )
        )
        .setDimFilter(new SelectorDimFilter("quality", "technology", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery outerQuery = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(new DefaultDimensionSpec("alias", "alias"), new ExtractionDimensionSpec(
            "time_day",
            "time_week",
            ValueType.LONG,
            new TimeFormatExtractionFn(null, null, null, Granularities.WEEK, true)
        )).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias", "technology",
            "time_week", 1301270400000L,
            "rows", 2L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, outerQuery);
    TestHelper.assertExpectedObjects(expectedResults, results, "extraction-fn");
  }

  @Test
  public void testGroupByLimitPushDown()
  {
    if (!config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      return;
    }
    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec(
                    "marketalias",
                    OrderByColumnSpec.Direction.DESCENDING
                )),
                2
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "upfront",
            "rows",
            186L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias",
            "total_market",
            "rows",
            186L
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "order-limit");
  }

  @Test
  public void testMergeResultsWithLimitPushDown()
  {
    if (!config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      return;
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.DESCENDING)),
                5
            )
        )
        .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .setGranularity(Granularities.ALL);

    final GroupByQuery allGranQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus<Row> queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus<Row> queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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
    List<Row> allGranExpectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 2L, "idx", 177L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "news", "rows", 2L, "idx", 221L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L)
    );

    TestHelper.assertExpectedObjects(
        allGranExpectedResults,
        mergedRunner.run(QueryPlus.wrap(allGranQuery), context),
        "merged"
    );
  }

  @Test
  public void testMergeResultsWithLimitPushDownSortByAgg()
  {
    if (!config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      return;
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("idx", OrderByColumnSpec.Direction.DESCENDING)),
                5
            )
        )
        .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .setGranularity(Granularities.ALL);

    final GroupByQuery allGranQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus<Row> queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus<Row> queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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

    List<Row> allGranExpectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "mezzanine", "rows", 6L, "idx", 4420L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 6L, "idx", 4416L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "entertainment", "rows", 2L, "idx", 319L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "automotive", "rows", 2L, "idx", 269L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 2L, "idx", 243L)
    );

    Iterable<Row> results = mergedRunner.run(QueryPlus.wrap(allGranQuery), context).toList();
    TestHelper.assertExpectedObjects(allGranExpectedResults, results, "merged");
  }

  @Test
  public void testMergeResultsWithLimitPushDownSortByDimDim()
  {
    if (!config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      return;
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"), new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec("alias", OrderByColumnSpec.Direction.DESCENDING),
                    new OrderByColumnSpec("market", OrderByColumnSpec.Direction.DESCENDING)
                ),
                5
            )
        )
        .setContext(
            ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true)
        )
        .setGranularity(Granularities.ALL);

    final GroupByQuery allGranQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus<Row> queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus<Row> queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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

    List<Row> allGranExpectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "travel",
            "market",
            "spot",
            "rows",
            2L,
            "idx",
            243L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "technology",
            "market",
            "spot",
            "rows",
            2L,
            "idx",
            177L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "market",
            "upfront",
            "rows",
            2L,
            "idx",
            1817L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "market",
            "total_market",
            "rows",
            2L,
            "idx",
            2342L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "market",
            "spot",
            "rows",
            2L,
            "idx",
            257L
        )
    );

    Iterable<Row> results = mergedRunner.run(QueryPlus.wrap(allGranQuery), context).toList();
    TestHelper.assertExpectedObjects(allGranExpectedResults, results, "merged");
  }

  @Test
  public void testMergeResultsWithLimitPushDownSortByDimAggDim()
  {
    if (!config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V2)) {
      return;
    }
    GroupByQuery.Builder builder = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"), new DefaultDimensionSpec("market", "market"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
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
        .setContext(
            ImmutableMap.of(
                GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN,
                true
            )
        )
        .setGranularity(Granularities.ALL);

    final GroupByQuery allGranQuery = builder.build();

    QueryRunner mergedRunner = factory.getToolchest().mergeResults(
        new QueryRunner<Row>()
        {
          @Override
          public Sequence<Row> run(QueryPlus<Row> queryPlus, Map<String, Object> responseContext)
          {
            // simulate two daily segments
            final QueryPlus<Row> queryPlus1 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-02/2011-04-03")))
            );
            final QueryPlus<Row> queryPlus2 = queryPlus.withQuerySegmentSpec(
                new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.of("2011-04-03/2011-04-04")))
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

    List<Row> allGranExpectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "travel",
            "market",
            "spot",
            "rows",
            2L,
            "idx",
            243L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "technology",
            "market",
            "spot",
            "rows",
            2L,
            "idx",
            177L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "market",
            "total_market",
            "rows",
            2L,
            "idx",
            2342L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "market",
            "upfront",
            "rows",
            2L,
            "idx",
            1817L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "premium",
            "market",
            "spot",
            "rows",
            2L,
            "idx",
            257L
        )
    );

    Iterable<Row> results = mergedRunner.run(QueryPlus.wrap(allGranQuery), context).toList();
    TestHelper.assertExpectedObjects(allGranExpectedResults, results, "merged");
  }

  @Test
  public void testGroupByLimitPushDownPostAggNotSupported()
  {
    expectedException.expect(UnsupportedOperationException.class);
    expectedException.expectMessage("Limit push down when sorting by a post aggregator is not supported.");

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec(
                    "constant",
                    OrderByColumnSpec.Direction.DESCENDING
                )),
                2
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setPostAggregatorSpecs(
            Collections.singletonList(new ConstantPostAggregator("constant", 1))
        )
        .setContext(
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
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.emptyInterval)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec(
                    "alias",
                    OrderByColumnSpec.Direction.DESCENDING
                )),
                5
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(subquery)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setAggregatorSpecs(new DoubleMaxAggregatorFactory("idx", "idx"))
        .setLimitSpec(new DefaultLimitSpec(null, 5))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    Assert.assertFalse(results.iterator().hasNext());
  }


  @Test
  public void testSubqueryWithMultipleIntervalsInOuterQueryWithLimitPushDown()
  {
    GroupByQuery subquery = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
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
            QueryRunnerTestHelper.rowsCount,
            new LongSumAggregatorFactory("idx", "index"),
            new LongSumAggregatorFactory("indexMaxPlusTen", "indexMaxPlusTen")
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    GroupByQuery query = GroupByQuery
        .builder()
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
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "travel", "rows", 1L, "idx", 119L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "technology", "rows", 1L, "idx", 78L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "premium", "rows", 3L, "idx", 2900L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "news", "rows", 1L, "idx", 121L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "mezzanine", "rows", 3L, "idx", 2870L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "health", "rows", 1L, "idx", 120L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "entertainment", "rows", 1L, "idx", 158L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "business", "rows", 1L, "idx", 118L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-01", "alias", "automotive", "rows", 1L, "idx", 135L),

        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "travel", "rows", 1L, "idx", 126L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "technology", "rows", 1L, "idx", 97L),
        GroupByQueryRunnerTestHelper.createExpectedRow("2011-04-02", "alias", "premium", "rows", 3L, "idx", 2505L)
    );

    // Subqueries are handled by the ToolChest
    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "limit-pushdown");
  }

  @Test
  public void testRejectForceLimitPushDownWithHaving()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Cannot force limit push down when a having spec is present.");

    GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(new DefaultDimensionSpec(QueryRunnerTestHelper.marketDimension, "marketalias"))
        .setInterval(QueryRunnerTestHelper.fullOnIntervalSpec)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("marketalias", OrderByColumnSpec.Direction.DESCENDING)),
                2
            )
        )
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount)
        .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, true))
        .setHavingSpec(new GreaterThanHavingSpec("rows", 10))
        .build();
  }

  @Test
  public void testTypeConversionWithMergingChainedExecutionRunner()
  {
    if (config.getDefaultStrategy().equals(GroupByStrategySelector.STRATEGY_V1)) {
      expectedException.expect(UnsupportedOperationException.class);
      expectedException.expectMessage("GroupBy v1 only supports dimensions with an outputType of STRING.");
    }

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(
            new DefaultDimensionSpec("quality", "alias"),
            new ExtractionDimensionSpec("quality", "qualityLen", ValueType.LONG, StrlenExtractionFn.instance())
        )
        .setDimFilter(new SelectorDimFilter("quality", "technology", null))
        .setAggregatorSpecs(QueryRunnerTestHelper.rowsCount, new LongSumAggregatorFactory("idx", "index"))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    List<Row> expectedResults = Arrays.asList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-01",
            "alias",
            "technology",
            "qualityLen",
            10L,
            "rows",
            2L,
            "idx",
            156L
        ),
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "2011-04-02",
            "alias",
            "technology",
            "qualityLen",
            10L,
            "rows",
            2L,
            "idx",
            194L
        )
    );

    ChainedExecutionQueryRunner ceqr = new ChainedExecutionQueryRunner(
        Execs.directExecutor(),
        (query1, future) -> {
          return;
        },
        ImmutableList.<QueryRunner<Row>>of(runner, runner)
    );

    QueryRunner<Row> mergingRunner = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(ceqr));

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, mergingRunner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "type-conversion");
  }
}
