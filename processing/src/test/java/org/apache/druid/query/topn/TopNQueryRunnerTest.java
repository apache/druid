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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.druid.collections.CloseableStupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.js.JavaScriptConfig;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.Result;
import org.apache.druid.query.TestQueryRunners;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMaxAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMaxAggregatorFactory;
import org.apache.druid.query.aggregation.FloatMinAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import org.apache.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import org.apache.druid.query.aggregation.first.LongFirstAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.last.FloatLastAggregatorFactory;
import org.apache.druid.query.aggregation.last.LongLastAggregatorFactory;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.dimension.ListFilteredDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
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
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.lookup.LookupExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ValueType;
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
import java.util.stream.Collectors;

/**
 */
@RunWith(Parameterized.class)
public class TopNQueryRunnerTest extends InitializedNullHandlingTest
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

    List<QueryRunner<Result<TopNResultValue>>> retVal = new ArrayList<>();
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                defaultPool,
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                customPool,
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
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
  private final boolean duplicateSingleAggregatorQueries;
  private final List<AggregatorFactory> commonAggregators;


  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public TopNQueryRunnerTest(
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
                        new DoubleFirstAggregatorFactory("first", "index")
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
  public void testFullOnTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
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
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
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

  @Test
  public void testTopNOnMissingColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("nonexistentColumn", "alias"))
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new CountAggregatorFactory("rows"))
        .build();

    final HashMap<String, Object> resultMap = new HashMap<>();
    resultMap.put("alias", null);
    resultMap.put("rows", 1209L);

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(Collections.<Map<String, Object>>singletonList(resultMap))
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOnMissingColumnWithExtractionFn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new ExtractionDimensionSpec("nonexistentColumn", "alias", new StringFormatExtractionFn("theValue")))
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new CountAggregatorFactory("rows"))
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
                    ImmutableMap.<String, Object>builder()
                        .put("alias", "theValue")
                        .put("rows", 1209L)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNOverPostAggs()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT_METRIC)
        .threshold(4)
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
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNOverPostAggsOnDimension()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric("dimPostAgg")
        .threshold(4)
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
        .postAggregators(
            new ExpressionPostAggregator(
                "dimPostAgg",
                "market + 'x'",
                null,
                TestExprMacroTable.INSTANCE
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
                        .put("dimPostAgg", "upfrontx")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market")
                        .put("dimPostAgg", "total_marketx")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "spot")
                        .put("dimPostAgg", "spotx")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNOverUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.UNIQUE_METRIC)
        .threshold(3)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 59.02102279663086D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 545.9906005859375D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverMissingUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.UNIQUE_METRIC)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new HyperUniquesAggregatorFactory("uniques", "missingUniques"))
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("uniques", 0d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("uniques", 0d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("uniques", 0d)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverHyperUniqueFinalizingPostAggregator()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(QueryRunnerTestHelper.QUALITY_UNIQUES)
        .postAggregators(
            new HyperUniqueFinalizingPostAggregator(
                QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                QueryRunnerTestHelper.UNIQUE_METRIC
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, QueryRunnerTestHelper.UNIQUES_9)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC, QueryRunnerTestHelper.UNIQUES_9)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, QueryRunnerTestHelper.UNIQUES_2)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC, QueryRunnerTestHelper.UNIQUES_2)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, QueryRunnerTestHelper.UNIQUES_2)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC, QueryRunnerTestHelper.UNIQUES_2)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverHyperUniqueExpression()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(QueryRunnerTestHelper.QUALITY_UNIQUES)
        .postAggregators(
            new ExpressionPostAggregator(
                QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                "uniques + 1",
                null,
                TestExprMacroTable.INSTANCE
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, QueryRunnerTestHelper.UNIQUES_9)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                            QueryRunnerTestHelper.UNIQUES_9 + 1
                        )
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, QueryRunnerTestHelper.UNIQUES_2)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                            QueryRunnerTestHelper.UNIQUES_2 + 1
                        )
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, QueryRunnerTestHelper.UNIQUES_2)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                            QueryRunnerTestHelper.UNIQUES_2 + 1
                        )
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverHyperUniqueExpressionRounded()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(QueryRunnerTestHelper.QUALITY_UNIQUES_ROUNDED)
        .postAggregators(
            new ExpressionPostAggregator(
                QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                "uniques + 1",
                null,
                TestExprMacroTable.INSTANCE
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, 9L)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC, 10L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, 2L)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC, 3L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put(QueryRunnerTestHelper.UNIQUE_METRIC, 2L)
                        .put(QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC, 3L)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverFirstLastAggregator()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.MONTH_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric("last")
        .threshold(3)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            new LongFirstAggregatorFactory("first", "index"),
            new LongLastAggregatorFactory("last", "index")
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1000L)
                        .put("last", 1127L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 800L)
                        .put("last", 943L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 100L)
                        .put("last", 155L)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-02-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1203L)
                        .put("last", 1292L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1667L)
                        .put("last", 1101L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 132L)
                        .put("last", 114L)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-03-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1124L)
                        .put("last", 1366L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1166L)
                        .put("last", 1063L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 153L)
                        .put("last", 125L)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1314L)
                        .put("last", 1029L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1447L)
                        .put("last", 780L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 135L)
                        .put("last", 120L)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverFirstLastFloatAggregatorUsingDoubleColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.MONTH_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric("last")
        .threshold(3)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            new FloatFirstAggregatorFactory("first", "index"),
            new FloatLastAggregatorFactory("last", "index")
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1000f)
                        .put("last", 1127.23095703125f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 800f)
                        .put("last", 943.4971923828125f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 100f)
                        .put("last", 155.7449493408203f)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-02-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1203.4656f)
                        .put("last", 1292.5428466796875f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1667.497802734375f)
                        .put("last", 1101.918212890625f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 132.123779296875f)
                        .put("last", 114.2845687866211f)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-03-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1124.2014f)
                        .put("last", 1366.4476f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1166.1411f)
                        .put("last", 1063.2012f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 153.05994f)
                        .put("last", 125.83968f)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1314.8397f)
                        .put("last", 1029.057f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1447.3412)
                        .put("last", 780.272)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 135.8851f)
                        .put("last", 120.290344f)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverFirstLastFloatAggregatorUsingFloatColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.MONTH_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric("last")
        .threshold(3)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            new FloatFirstAggregatorFactory("first", "indexFloat"),
            new FloatLastAggregatorFactory("last", "indexFloat")
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1000f)
                        .put("last", 1127.23095703125f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 800f)
                        .put("last", 943.4971923828125f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 100f)
                        .put("last", 155.7449493408203f)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-02-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1203.4656f)
                        .put("last", 1292.5428466796875f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1667.497802734375f)
                        .put("last", 1101.918212890625f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 132.123779296875f)
                        .put("last", 114.2845687866211f)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-03-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1124.2014f)
                        .put("last", 1366.4476f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1166.1411f)
                        .put("last", 1063.2012f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 153.05994f)
                        .put("last", 125.83968f)
                        .build()
                )
            )
        ),
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("first", 1314.8397f)
                        .put("last", 1029.057f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("first", 1447.3412)
                        .put("last", 780.272)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("first", 135.8851f)
                        .put("last", 120.290344f)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }



  @Test
  public void testTopNBySegment()
  {

    final HashMap<String, Object> specialContext = new HashMap<String, Object>();
    specialContext.put("bySegment", "true");
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .context(specialContext)
        .build();


    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "addRowsIndexConstant", 5356.814783D,
                        "index", 5351.814783D,
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "uniques", QueryRunnerTestHelper.UNIQUES_2,
                        "rows", 4L
                    ),
                    ImmutableMap.of(
                        "addRowsIndexConstant", 4880.669692D,
                        "index", 4875.669692D,
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "uniques", QueryRunnerTestHelper.UNIQUES_2,
                        "rows", 4L
                    ),
                    ImmutableMap.of(
                        "addRowsIndexConstant", 2250.876812D,
                        "index", 2231.876812D,
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "uniques", QueryRunnerTestHelper.UNIQUES_9,
                        "rows", 18L
                    )
                )
            )
        )
    );

    Sequence<Result<TopNResultValue>> results = runWithMerge(query);
    List<Result<BySegmentTopNResultValue>> resultList = results
        .map((Result<TopNResultValue> input) -> {
          // Stupid type erasure
          Object val = input.getValue();
          if (val instanceof BySegmentResultValue) {
            BySegmentResultValue bySegVal = (BySegmentResultValue) val;
            return new Result<>(
                input.getTimestamp(),
                new BySegmentTopNResultValue(
                    Lists.transform(
                        bySegVal.getResults(),
                        res -> {
                          if (Preconditions.checkNotNull(res) instanceof Result) {
                            Result theResult = (Result) res;
                            Object resVal = theResult.getValue();
                            if (resVal instanceof TopNResultValue) {
                              return new Result<>(theResult.getTimestamp(), (TopNResultValue) resVal);
                            }
                          }
                          throw new IAE("Bad input: [%s]", res);
                        }
                    ),
                    bySegVal.getSegmentId(),
                    bySegVal.getInterval()
                )
            );
          }
          throw new ISE("Bad type");
        })
        .toList();
    Result<BySegmentTopNResultValue> result = resultList.get(0);
    TestHelper.assertExpectedResults(expectedResults, result.getValue().getResults());
  }

  @Test
  public void testTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNByUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(new NumericTopNMetricSpec("uniques"))
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
                        "market", "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        "market", "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        "market", "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithOrFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", "upfront", "spot")
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithOrFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", "upfront")
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine")
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 2L,
                        "index", 2591.68359375D,
                        "addRowsIndexConstant", 2594.68359375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 2L,
                        "index", 2508.39599609375D,
                        "addRowsIndexConstant", 2511.39599609375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 2L,
                        "index", 220.63774871826172D,
                        "addRowsIndexConstant", 223.63774871826172D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithFilter2OneDay()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine")
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(
            new MultipleIntervalSegmentSpec(
                Collections.singletonList(Intervals.of("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"))
            )
        )
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 1L,
                        "index", new Float(1447.341160).doubleValue(),
                        "addRowsIndexConstant", new Float(1449.341160).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 1L,
                        "index", new Float(1314.839715).doubleValue(),
                        "addRowsIndexConstant", new Float(1316.839715).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 1L,
                        "index", new Float(109.705815).doubleValue(),
                        "addRowsIndexConstant", new Float(111.705815).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentFilterInOr()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market", "upfront", "billyblank")
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "billyblank")
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();
    assertExpectedResults(
        Collections.singletonList(
            new Result<>(DateTimes.of("2011-04-01T00:00:00.000Z"), new TopNResultValue(Collections.emptyList()))
        ),
        query
    );
  }

  @Test
  public void testTopNWithNonExistentFilterMultiDim()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.MARKET_DIMENSION, "billyblank", null),
        new SelectorDimFilter(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine", null)
    );
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(andDimFilter)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();
    assertExpectedResults(
        Collections.singletonList(
            new Result<>(DateTimes.of("2011-04-01T00:00:00.000Z"), new TopNResultValue(Collections.emptyList()))
        ),
        query
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "m")
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    assertExpectedResults(
        runWithMerge(
            new TopNQueryBuilder()
                .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                .granularity(QueryRunnerTestHelper.ALL_GRAN)
                .filters(QueryRunnerTestHelper.QUALITY_DIMENSION, "mezzanine")
                .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
                .metric(QueryRunnerTestHelper.INDEX_METRIC)
                .threshold(4)
                .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                .aggregators(commonAggregators)
                .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                .build()
        ).toList(),
        query
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "m", "a", "b")
        .dimension(QueryRunnerTestHelper.QUALITY_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    assertExpectedResults(
        runWithMerge(
            new TopNQueryBuilder()
                .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                .granularity(QueryRunnerTestHelper.ALL_GRAN)
                .filters(
                    QueryRunnerTestHelper.QUALITY_DIMENSION,
                    "mezzanine",
                    "automotive",
                    "business"
                )
                .dimension(QueryRunnerTestHelper.QUALITY_DIMENSION)
                .metric(QueryRunnerTestHelper.INDEX_METRIC)
                .threshold(4)
                .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                .aggregators(commonAggregators)
                .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                .build()
        ).toList(),
        query
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter3()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a")
        .dimension(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    final List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "placementish", "a",
                        "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        "placementish", "preferred",
                        "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithMultiValueDimFilter4()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "a", "b")
        .dimension(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    final List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "placementish", "preferred",
                        "rows", 4L,
                        "index", 514.868408203125D,
                        "addRowsIndexConstant", 519.868408203125D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        "placementish",
                        "a", "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        "placementish", "b",
                        "rows", 2L,
                        "index", 231.557373046875D,
                        "addRowsIndexConstant", 234.557373046875D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithMultiValueDimFilter5()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION, "preferred")
        .dimension(QueryRunnerTestHelper.PLACEMENTISH_DIMENSION)
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    final List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "placementish", "preferred",
                        "rows", 26L,
                        "index", 12459.361190795898D,
                        "addRowsIndexConstant", 12486.361190795898D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        "placementish", "p",
                        "rows", 6L,
                        "index", 5407.213653564453D,
                        "addRowsIndexConstant", 5414.213653564453D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        "placementish", "m",
                        "rows", 6L,
                        "index", 5320.717338562012D,
                        "addRowsIndexConstant", 5327.717338562012D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.of(
                        "placementish", "t",
                        "rows", 4L,
                        "index", 422.3440856933594D,
                        "addRowsIndexConstant", 427.3440856933594D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentDimension()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(1)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    QueryRunnerTestHelper.orderedMap(
                        "doesn't exist", null,
                        "rows", 26L,
                        "index", 12459.361190795898D,
                        "addRowsIndexConstant", 12486.361190795898D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentDimensionAndActualFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    QueryRunnerTestHelper.orderedMap(
                        "doesn't exist", null,
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNonExistentDimensionAndNonExistentFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters("doesn't exist", null)
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(1)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    QueryRunnerTestHelper.orderedMap(
                        "doesn't exist", null,
                        "rows", 26L,
                        "index", 12459.361190795898D,
                        "addRowsIndexConstant", 12486.361190795898D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographic()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicNoAggregators()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(new DimensionTopNMetricSpec("", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot"
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market"
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront"
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(new DimensionTopNMetricSpec("spot", StringComparators.LEXICOGRAPHIC))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicWithNonExistingPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(new DimensionTopNMetricSpec("t", StringComparators.LEXICOGRAPHIC))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNInvertedLexicographicWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("upfront", StringComparators.LEXICOGRAPHIC)))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNInvertedLexicographicWithNonExistingPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("u", StringComparators.LEXICOGRAPHIC)))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testTopNDimExtractionToOne()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new JavaScriptExtractionFn("function(f) { return \"POTATO\"; }", false, JavaScriptConfig.getEnabledInstance())
            )
        )
        .metric("rows")
        .threshold(10)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
                    ImmutableMap.of(
                        "addRowsIndexConstant", 504542.5071372986D,
                        "index", 503332.5071372986D,
                        QueryRunnerTestHelper.MARKET_DIMENSION, "POTATO",
                        "uniques", QueryRunnerTestHelper.UNIQUES_9,
                        "rows", 1209L
                    )
                )
            )
        )
    );
    List<Result<TopNResultValue>> list = runWithMerge(query).toList();
    Assert.assertEquals(list.size(), 1);
    Assert.assertEquals("Didn't merge results", list.get(0).getValue().getValue().size(), 1);
    TestHelper.assertExpectedResults(expectedResults, list, "Failed to match");
  }

  @Test
  public void testTopNDimExtractionTimeToOneLong()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                "t",
                ValueType.LONG,
                new JavaScriptExtractionFn(
                    "function(f) { return \"42\"; }",
                    false,
                    JavaScriptConfig.getEnabledInstance()
                )
            )
        )
        .metric("rows")
        .threshold(10)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
                    ImmutableMap.of(
                        "addRowsIndexConstant", 504542.5071372986D,
                        "index", 503332.5071372986D,
                        "t", 42L,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9,
                        "rows", 1209L
                    )
                )
            )
        )
    );
    List<Result<TopNResultValue>> list = runWithMerge(query).toList();
    Assert.assertEquals(list.size(), 1);
    Assert.assertEquals("Didn't merge results", list.get(0).getValue().getValue().size(), 1);
    TestHelper.assertExpectedResults(expectedResults, list, "Failed to match");
  }

  @Test
  public void testTopNCollapsingDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.QUALITY_DIMENSION,
                QueryRunnerTestHelper.QUALITY_DIMENSION,
                new RegexDimExtractionFn(".(.)", false, null)
            )
        )
        .metric("index")
        .threshold(2)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            QueryRunnerTestHelper.ROWS_COUNT,
            QueryRunnerTestHelper.INDEX_DOUBLE_SUM
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.QUALITY_DIMENSION, "e",
                        "rows", 558L,
                        "index", 246645.1204032898,
                        "addRowsIndexConstant", 247204.1204032898
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.QUALITY_DIMENSION, "r",
                        "rows", 372L,
                        "index", 222051.08961486816,
                        "addRowsIndexConstant", 222424.08961486816
                    )
                )
            )
        )
    );

    assertExpectedResults(expectedResults, query);

    query = query.withAggregatorSpecs(
        Arrays.asList(
            QueryRunnerTestHelper.ROWS_COUNT,
            new DoubleSumAggregatorFactory("index", null, "-index + 100", ExprMacroTable.nil())
        )
    );

    expectedResults = Collections.singletonList(
        TopNQueryRunnerTestHelper.createExpectedRows(
            "2011-01-12T00:00:00.000Z",
            new String[] {QueryRunnerTestHelper.QUALITY_DIMENSION, "rows", "index", "addRowsIndexConstant"},
            Arrays.asList(
                new Object[] {"n", 93L, -2786.4727909999997, -2692.4727909999997},
                new Object[] {"u", 186L, -3949.824348000002, -3762.824348000002}
            )
        )
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "s",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "u",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNDimExtractionNoAggregators()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric(new LexicographicTopNMetricSpec(QueryRunnerTestHelper.MARKET_DIMENSION))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "s"
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "t"
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "u"
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNDimExtractionFastTopNOptimalWithReplaceMissing()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.of(
                            "spot", "2spot0",
                            "total_market", "1total_market0",
                            "upfront", "3upfront0"
                        ),
                        false
                    ), false, "MISSING", true,
                    false
                )
            )
        )
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "2spot0",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "3upfront0",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testTopNDimExtractionFastTopNUnOptimalWithReplaceMissing()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.of(
                            "spot", "2spot0",
                            "total_market", "1total_market0",
                            "upfront", "3upfront0"
                        ),
                        false
                    ), false, "MISSING", false,
                    false
                )
            )
        )
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "2spot0",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "3upfront0",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  // Test a "direct" query
  public void testTopNDimExtractionFastTopNOptimal()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.of(
                            "spot", "2spot0",
                            "total_market", "1total_market0",
                            "upfront", "3upfront0"
                        ),
                        false
                    ), true, null, true,
                    false
                )
            )
        )
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "2spot0",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "3upfront0",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  // Test query path that must rebucket the data
  public void testTopNDimExtractionFastTopNUnOptimal()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.of(
                            "spot",
                            "spot0",
                            "total_market",
                            "total_market0",
                            "upfront",
                            "upfront0"
                        ),
                        false
                    ), true, null, false,
                    false
                )
            )
        )
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot0",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market0",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront0",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtractionOptimalNamespace()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.of(
                            "spot",
                            "2spot",
                            "total_market",
                            "3total_market",
                            "upfront",
                            "1upfront"
                        ),
                        false
                    ), true, null, true,
                    false
                )
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "1upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "2spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "3total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtractionUnOptimalNamespace()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.of(
                            "spot",
                            "2spot",
                            "total_market",
                            "3total_market",
                            "upfront",
                            "1upfront"
                        ),
                        false
                    ), true, null, false,
                    false
                )
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "1upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "2spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "3total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testTopNLexicographicDimExtractionOptimalNamespaceWithRunner()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new LookupExtractionFn(
                    new MapLookupExtractor(
                        ImmutableMap.of(
                            "spot",
                            "2spot",
                            "total_market",
                            "3total_market",
                            "upfront",
                            "1upfront"
                        ),
                        false
                    ), true, null, true,
                    false
                )
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "1upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "2spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "3total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "s",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "u",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testInvertedTopNLexicographicDimExtraction2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new RegexDimExtractionFn("..(.)", false, null)
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "o",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "f",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtractionWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric(new DimensionTopNMetricSpec("s", StringComparators.LEXICOGRAPHIC))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "u",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNLexicographicDimExtractionWithSortingPreservedAndPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new DimExtractionFn()
                {
                  @Override
                  public byte[] getCacheKey()
                  {
                    return new byte[0];
                  }

                  @Override
                  public String apply(String value)
                  {
                    return value.substring(0, 1);
                  }

                  @Override
                  public boolean preservesOrdering()
                  {
                    return true;
                  }

                  @Override
                  public ExtractionType getExtractionType()
                  {
                    return ExtractionType.MANY_TO_ONE;
                  }
                }
            )
        )
        .metric(new DimensionTopNMetricSpec("s", StringComparators.LEXICOGRAPHIC))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "u",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testInvertedTopNLexicographicDimExtractionWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("u", StringComparators.LEXICOGRAPHIC)))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "s",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testInvertedTopNLexicographicDimExtractionWithPreviousStop2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                new RegexDimExtractionFn("..(.)", false, null)
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("p", StringComparators.LEXICOGRAPHIC)))
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "o",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "f",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNWithNullProducingDimExtractionFn()
  {
    final ExtractionFn nullStringDimExtraction = new DimExtractionFn()
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{(byte) 0xFF};
      }

      @Override
      public String apply(String dimValue)
      {
        return "total_market".equals(dimValue) ? null : dimValue;
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

    final TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                nullStringDimExtraction
            )
        )
        .build();


    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    QueryRunnerTestHelper.orderedMap(
                        QueryRunnerTestHelper.MARKET_DIMENSION, null,
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    assertExpectedResults(expectedResults, query);

  }

  @Test
  /**
   * This test exists only to show what the current behavior is and not necessarily to define that this is
   * correct behavior.  In fact, the behavior when returning the empty string from a DimExtractionFn is, by
   * contract, undefined, so this can do anything.
   */
  public void testTopNWithEmptyStringProducingDimExtractionFn()
  {
    final ExtractionFn emptyStringDimExtraction = new DimExtractionFn()
    {
      @Override
      public byte[] getCacheKey()
      {
        return new byte[]{(byte) 0xFF};
      }

      @Override
      public String apply(String dimValue)
      {
        return "total_market".equals(dimValue) ? "" : dimValue;
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

    final TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.MARKET_DIMENSION,
                QueryRunnerTestHelper.MARKET_DIMENSION,
                emptyStringDimExtraction
            )
        )
        .build();


    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    QueryRunnerTestHelper.orderedMap(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testInvertedTopNQuery()
  {
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
            .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec(QueryRunnerTestHelper.INDEX_METRIC)))
            .threshold(3)
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
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNQueryByComplexMetric()
  {
    ImmutableList<DimensionSpec> aggregatorDimensionSpecs = ImmutableList.of(new DefaultDimensionSpec(
        QueryRunnerTestHelper.QUALITY_DIMENSION,
        QueryRunnerTestHelper.QUALITY_DIMENSION
    ));
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
            .metric(new NumericTopNMetricSpec("numVals"))
            .threshold(10)
            .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
            .aggregators(duplicateAggregators(
                new CardinalityAggregatorFactory("numVals", aggregatorDimensionSpecs, false),
                new CardinalityAggregatorFactory("numVals1", aggregatorDimensionSpecs, false)
            ))
            .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                withDuplicateResults(
                    Arrays.<Map<String, Object>>asList(
                        ImmutableMap.of(
                            "market", "spot",
                            "numVals", 9.019833517963864d
                        ),
                        ImmutableMap.of(
                            "market", "total_market",
                            "numVals", 2.000977198748901d
                        ),
                        ImmutableMap.of(
                            "market", "upfront",
                            "numVals", 2.000977198748901d
                        )
                    ),
                    "numVals",
                    "numVals1"
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNQueryCardinalityAggregatorWithExtractionFn()
  {
    String helloJsFn = "function(str) { return 'hello' }";
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getEnabledInstance());

    DimensionSpec dimSpec = new ExtractionDimensionSpec(QueryRunnerTestHelper.MARKET_DIMENSION,
                                                        QueryRunnerTestHelper.MARKET_DIMENSION,
                                                        helloFn);

    ImmutableList<DimensionSpec> aggregatorDimensionSpecs = ImmutableList.of(new ExtractionDimensionSpec(
        QueryRunnerTestHelper.QUALITY_DIMENSION,
        QueryRunnerTestHelper.QUALITY_DIMENSION,
        helloFn
    ));
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
            .granularity(QueryRunnerTestHelper.ALL_GRAN)
            .dimension(dimSpec)
            .metric(new NumericTopNMetricSpec("numVals"))
            .threshold(10)
            .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
            .aggregators(duplicateAggregators(
                new CardinalityAggregatorFactory("numVals", aggregatorDimensionSpecs, false),
                new CardinalityAggregatorFactory("numVals1", aggregatorDimensionSpecs, false)
            ))
            .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                withDuplicateResults(
                    Collections.singletonList(
                        ImmutableMap.of(
                            "market", "hello",
                            "numVals", 1.0002442201269182d
                        )
                    ),
                    "numVals",
                    "numVals1"
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNDependentPostAgg()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
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
        .postAggregators(
            QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT,
            QueryRunnerTestHelper.DEPENDENT_POST_AGG,
            QueryRunnerTestHelper.hyperUniqueFinalizingPostAgg
        )
        .build();

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
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
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
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.061029D)
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
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put(
                            QueryRunnerTestHelper.HYPER_UNIQUE_FINALIZING_POST_AGG_METRIC,
                            QueryRunnerTestHelper.UNIQUES_9 + 1.0
                        )
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNBySegmentResults()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
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
        .postAggregators(
            QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT,
            QueryRunnerTestHelper.DEPENDENT_POST_AGG
        )
        .context(ImmutableMap.of("finalize", true, "bySegment", true))
        .build();
    TopNResultValue topNResult = new TopNResultValue(
        Arrays.<Map<String, Object>>asList(
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.MARKET_DIMENSION, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
        )
    );

    @SuppressWarnings("unused") // TODO: fix this test
    List<Result<BySegmentResultValueClass<Result<TopNResultValue>>>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new BySegmentResultValueClass<>(
                Collections.singletonList(new Result<>(DateTimes.of("2011-01-12T00:00:00.000Z"), topNResult)),
                QueryRunnerTestHelper.SEGMENT_ID.toString(),
                Intervals.of("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z")
            )
        )
    );
    Sequence<Result<TopNResultValue>> results = runWithMerge(query);
    for (Result<TopNResultValue> result : results.toList()) {
      Assert.assertEquals(result.getValue(), result.getValue()); // TODO: fix this test
    }
  }

  @Test
  public void testTopNWithTimeColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(
            QueryRunnerTestHelper.ROWS_COUNT,
            QueryRunnerTestHelper.JS_COUNT_IF_TIME_GREATER_THAN,
            QueryRunnerTestHelper.TIME_LONG_SUM
        )
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric("ntimestamps")
        .threshold(3)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "market", "spot",
                        "rows",
                        18L,
                        "ntimestamps",
                        9.0,
                        "sumtime",
                        23429865600000L
                    ),
                    ImmutableMap.of(
                        "market", "total_market",
                        "rows",
                        4L,
                        "ntimestamps",
                        2.0,
                        "sumtime",
                        5206636800000L
                    ),
                    ImmutableMap.of(
                        "market", "upfront",
                        "rows",
                        4L,
                        "ntimestamps",
                        2.0,
                        "sumtime",
                        5206636800000L
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNTimeExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(
            new ExtractionDimensionSpec(
                ColumnHolder.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null, null, false)
            )
        )
        .metric("index")
        .threshold(2)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            QueryRunnerTestHelper.ROWS_COUNT,
            QueryRunnerTestHelper.INDEX_DOUBLE_SUM
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.of(
                        "dayOfWeek", "Wednesday",
                        "rows", 182L,
                        "index", 76010.28100585938,
                        "addRowsIndexConstant", 76193.28100585938
                    ),
                    ImmutableMap.of(
                        "dayOfWeek", "Thursday",
                        "rows", 182L,
                        "index", 75203.26300811768,
                        "addRowsIndexConstant", 75386.26300811768
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverNullDimension()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
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

    Map<String, Object> map = new HashMap<>();
    map.put("null_column", null);
    map.put("rows", 1209L);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.061029D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverNullDimensionWithFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("null_column")
        .filters(
            new SelectorDimFilter("null_column", null, null)
        )
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
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

    Map<String, Object> map = new HashMap<>();
    map.put("null_column", null);
    map.put("rows", 1209L);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.061029D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverPartialNullDimension()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(Granularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.UNIQUE_METRIC)
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .build();

    Map<String, Object> map = new HashMap<>();
    map.put("partial_null_column", null);
    map.put("rows", 22L);
    map.put("index", 7583.691513061523D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map,
                    ImmutableMap.<String, Object>of(
                        "partial_null_column", "value",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverPartialNullDimensionWithFilterOnNullValue()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(Granularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.UNIQUE_METRIC)
        .filters(new SelectorDimFilter("partial_null_column", null, null))
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .build();

    Map<String, Object> map = new HashMap<>();
    map.put("partial_null_column", null);
    map.put("rows", 22L);
    map.put("index", 7583.691513061523D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNOverPartialNullDimensionWithFilterOnNOTNullValue()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(Granularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.UNIQUE_METRIC)
        .filters(new SelectorDimFilter("partial_null_column", "value", null))
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(commonAggregators)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    ImmutableMap.<String, Object>of(
                        "partial_null_column", "value",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testAlphaNumericTopNWithNullPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(new DimensionTopNMetricSpec(null, StringComparators.ALPHANUMERIC))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.SECOND_ONLY)
        .aggregators(duplicateAggregators(
            QueryRunnerTestHelper.ROWS_COUNT,
            new CountAggregatorFactory("rows1")
        ))
        .build();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                withDuplicateResults(
                    Arrays.asList(
                        ImmutableMap.of(
                            "market", "spot",
                            "rows", 9L
                        ),
                        ImmutableMap.of(
                            "market", "total_market",
                            "rows", 2L
                        )
                    ),
                    "rows",
                    "rows1"
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }

  @Test
  public void testNumericDimensionTopNWithNullPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.SECOND_ONLY)
        .aggregators(duplicateAggregators(
            QueryRunnerTestHelper.ROWS_COUNT,
            new CountAggregatorFactory("rows1")
        ))
        .build();
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                withDuplicateResults(
                    Arrays.asList(
                        ImmutableMap.of(
                            "market", "spot",
                            "rows", 9L
                        ),
                        ImmutableMap.of(
                            "market", "total_market",
                            "rows", 2L
                        )
                    ),
                    "rows",
                    "rows1"
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query)));
  }


  @Test
  public void testTopNWithExtractionFilter()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("spot", "spot0");
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    TopNQuery query = new TopNQueryBuilder().dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                            .granularity(QueryRunnerTestHelper.ALL_GRAN)
                                            .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
                                            .metric("rows")
                                            .threshold(3)
                                            .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                            .aggregators(commonAggregators)
                                            .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT)
                                            .filters(
                                                new ExtractionDimFilter(
                                                    QueryRunnerTestHelper.MARKET_DIMENSION,
                                                    "spot0",
                                                    lookupExtractionFn,
                                                    null
                                                )
                                            )
                                            .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
                    ImmutableMap.of(
                        QueryRunnerTestHelper.MARKET_DIMENSION, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );

    assertExpectedResults(expectedResults, query);
    // Assert the optimization path as well
    final Sequence<Result<TopNResultValue>> retval = runWithPreMergeAndMerge(query);
    TestHelper.assertExpectedResults(expectedResults, retval);
  }

  @Test
  public void testTopNWithExtractionFilterAndFilteredAggregatorCaseNoExistingValue()
  {
    Map<String, String> extractionMap = new HashMap<>();

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn;
    if (NullHandling.replaceWithDefault()) {
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);
      extractionMap.put("", "NULL");
    } else {
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, "NULL", true, false);
    }
    DimFilter extractionFilter = new ExtractionDimFilter("null_column", "NULL", lookupExtractionFn, null);
    TopNQueryBuilder topNQueryBuilder = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new FilteredAggregatorFactory(
                            new DoubleMaxAggregatorFactory("maxIndex", "index"),
                            extractionFilter
                        ),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT);
    TopNQuery topNQueryWithNULLValueExtraction = topNQueryBuilder
        .filters(extractionFilter)
        .build();

    Map<String, Object> map = new HashMap<>();
    map.put("null_column", null);
    map.put("rows", 1209L);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.061029D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, topNQueryWithNULLValueExtraction);
  }

  private Sequence<Result<TopNResultValue>> runWithPreMergeAndMerge(TopNQuery query)
  {
    return runWithMerge(query, ResponseContext.createEmpty());
  }

  @Test
  public void testTopNWithExtractionFilterNoExistingValue()
  {
    Map<String, String> extractionMap = new HashMap<>();

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn;
    if (NullHandling.replaceWithDefault()) {
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
      extractionMap.put("", "NULL");
    } else {
      extractionMap.put("", "NOT_USED");
      lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, "NULL", true, true);
    }
    DimFilter extractionFilter = new ExtractionDimFilter("null_column", "NULL", lookupExtractionFn, null);
    TopNQueryBuilder topNQueryBuilder = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new FilteredAggregatorFactory(
                            new DoubleMaxAggregatorFactory("maxIndex", "index"),
                            extractionFilter
                        ),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT);
    TopNQuery topNQueryWithNULLValueExtraction = topNQueryBuilder
        .filters(extractionFilter)
        .build();

    Map<String, Object> map = new HashMap<>();
    map.put("null_column", null);
    map.put("rows", 1209L);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.061029D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, topNQueryWithNULLValueExtraction);
    // Assert the optimization path as well
    final Sequence<Result<TopNResultValue>> retval = runWithPreMergeAndMerge(topNQueryWithNULLValueExtraction);
    TestHelper.assertExpectedResults(expectedResults, retval);
  }

  @Test
  public void testFullOnTopNFloatColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec(QueryRunnerTestHelper.INDEX_METRIC, "index_alias", ValueType.FLOAT))
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1000.0f)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 2000.0D)
                        .put("rows", 2L)
                        .put("addRowsIndexConstant", 2003.0D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1000.0D)
                        .put("minIndex", 1000.0D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1870.061029f)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 1870.061029D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1872.06103515625D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 1870.061029D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1862.737933f)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 1862.737933D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1864.7379150390625D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 1862.737933D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1743.92175f)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 1743.92175D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1745.9217529296875D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 1743.92175D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNFloatColumnWithExFn()
  {
    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new ExtractionDimensionSpec(QueryRunnerTestHelper.INDEX_METRIC, "index_alias", jsExtractionFn))
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", "super-1000")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 2000.0D)
                        .put("rows", 2L)
                        .put("addRowsIndexConstant", 2003.0D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1000.0D)
                        .put("minIndex", 1000.0D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", "super-1870.061029")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 1870.061029D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1872.06103515625D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 1870.061029D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", "super-1862.737933")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 1862.737933D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1864.7379150390625D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 1862.737933D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", "super-1743.92175")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 1743.92175D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1745.9217529296875D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 1743.92175D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNFloatColumnAsString()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("qualityFloat", "qf_alias"))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("qf_alias", "14000.0")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qf_alias", "16000.0")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qf_alias", "10000.0")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qf_alias", "12000.0")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNLongColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("qualityLong", "ql_alias", ValueType.LONG))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1400L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1600L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1200L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNLongVirtualColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("ql_expr", "ql_alias", ValueType.LONG))
        .metric("maxIndex")
        .threshold(4)
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
        .virtualColumns(new ExpressionVirtualColumn("ql_expr", "qualityLong", ValueType.LONG, ExprMacroTable.nil()))
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1400L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1600L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1200L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNStringVirtualColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .virtualColumns(
            new ExpressionVirtualColumn(
                "vc",
                "market + ' ' + market",
                ValueType.STRING,
                TestExprMacroTable.INSTANCE
            )
        )
        .dimension("vc")
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
                        "vc", "spot spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.of(
                        "vc", "total_market total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.of(
                        "vc", "upfront upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNLongColumnWithExFn()
  {
    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new ExtractionDimensionSpec("qualityLong", "ql_alias", jsExtractionFn))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "super-1400")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "super-1600")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "super-1000")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "super-1200")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNLongColumnAsString()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("qualityLong", "ql_alias"))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "1400")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "1600")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "1000")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "1200")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNNumericStringColumnAsLong()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("qualityNumericString", "qns_alias", ValueType.LONG))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 140000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 160000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 100000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 120000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNNumericStringColumnAsFloat()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("qualityNumericString", "qns_alias", ValueType.FLOAT))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 140000.0f)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 160000.0f)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 100000.0f)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 120000.0f)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNLongTimeColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, "time_alias", ValueType.LONG))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", 1296345600000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 5497.331253051758D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 5511.331253051758D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 97.02391052246094D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", 1298678400000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 6541.463027954102D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6555.463027954102D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 83.099365234375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", 1301529600000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 6814.467971801758D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6828.467971801758D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1734.27490234375D)
                        .put("minIndex", 93.39083862304688D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", 1294876800000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 6077.949111938477D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6091.949111938477D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1689.0128173828125D)
                        .put("minIndex", 94.87471008300781D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testSortOnDoubleAsLong()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("index", "index_alias", ValueType.LONG))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 59L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 67L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 68L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 69L)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testSortOnTimeAsLong()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("__time", "__time_alias", ValueType.LONG))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("__time_alias", DateTimes.of("2011-01-12T00:00:00.000Z").getMillis())
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("__time_alias", DateTimes.of("2011-01-13T00:00:00.000Z").getMillis())
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("__time_alias", DateTimes.of("2011-01-14T00:00:00.000Z").getMillis())
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("__time_alias", DateTimes.of("2011-01-15T00:00:00.000Z").getMillis())
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testSortOnStringAsDouble()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("market", "alias", ValueType.DOUBLE))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    final Map<String, Object> nullAliasMap = new HashMap<>();
    nullAliasMap.put("alias", null);

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(Collections.singletonList(nullAliasMap))
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testSortOnDoubleAsDouble()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("index", "index_alias", ValueType.DOUBLE))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 59.021022d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 59.266595d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 67.73117d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 68.573162d)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNLongTimeColumnWithExFn()
  {
    String jsFn = "function(str) { return 'super-' + str; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new ExtractionDimensionSpec(ColumnHolder.TIME_COLUMN_NAME, "time_alias", jsExtractionFn))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", "super-1296345600000")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 5497.331253051758D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 5511.331253051758D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 97.02391052246094D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", "super-1298678400000")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 6541.463027954102D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6555.463027954102D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 83.099365234375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", "super-1301529600000")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 6814.467971801758D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6828.467971801758D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1734.27490234375D)
                        .put("minIndex", 93.39083862304688D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", "super-1294876800000")
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 6077.949111938477D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6091.949111938477D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1689.0128173828125D)
                        .put("minIndex", 94.87471008300781D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNDimExtractionAllNulls()
  {
    String jsFn = "function(str) { return null; }";
    ExtractionFn jsExtractionFn = new JavaScriptExtractionFn(jsFn, false, JavaScriptConfig.getEnabledInstance());

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new ExtractionDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            QueryRunnerTestHelper.MARKET_DIMENSION,
            jsExtractionFn
        ))
        .metric(QueryRunnerTestHelper.INDEX_METRIC)
        .threshold(4)
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

    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put(QueryRunnerTestHelper.MARKET_DIMENSION, null);
    expectedMap.put("rows", 1209L);
    expectedMap.put("index", 503332.5071372986D);
    expectedMap.put("addRowsIndexConstant", 504542.5071372986D);
    expectedMap.put("uniques", 9.019833517963864);
    expectedMap.put("maxIndex", 1870.061029D);
    expectedMap.put("minIndex", 59.02102279663086D);

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Collections.singletonList(
                    expectedMap
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNStringOutputAsLong()
  {
    ExtractionFn strlenFn = StrlenExtractionFn.instance();

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new ExtractionDimensionSpec(QueryRunnerTestHelper.QUALITY_DIMENSION, "alias", ValueType.LONG, strlenFn))
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("alias", 9L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("alias", 7L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("alias", 10L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 20479.497562408447D)
                        .put("rows", 186L)
                        .put("addRowsIndexConstant", 20666.497562408447D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 59.02102279663086D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("alias", 13L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNNumericStringColumnWithDecoration()
  {
    ListFilteredDimensionSpec filteredSpec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityNumericString", "qns_alias", ValueType.LONG),
        Sets.newHashSet("120000", "140000", "160000"),
        true
    );

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(filteredSpec)
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 140000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 160000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 120000L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNDecorationOnNumeric()
  {
    ListFilteredDimensionSpec filteredSpec = new ListFilteredDimensionSpec(
        new DefaultDimensionSpec("qualityLong", "ql_alias", ValueType.LONG),
        Sets.newHashSet("1200", "1400", "1600"),
        true
    );

    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(filteredSpec)
        .metric("maxIndex")
        .threshold(4)
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

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1400L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1600L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1200L)
                        .put(QueryRunnerTestHelper.INDEX_METRIC, 12086.472791D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12180.472791D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 193.787574D)
                        .put("minIndex", 84.710523D)
                        .build()
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testFullOnTopNWithAggsOnNumericDims()
  {
    List<Pair<AggregatorFactory, List<?>>> aggregations = new ArrayList<>();
    aggregations.add(new Pair<>(
        QueryRunnerTestHelper.ROWS_COUNT,
        Longs.asList(186L, 186L, 837L)
    ));
    Pair<AggregatorFactory, List<?>> indexAggregation = new Pair<>(
        QueryRunnerTestHelper.INDEX_DOUBLE_SUM,
        Doubles.asList(215679.82879638672D, 192046.1060180664D, 95606.57232284546D)
    );
    aggregations.add(indexAggregation);
    aggregations.add(new Pair<>(
        QueryRunnerTestHelper.QUALITY_UNIQUES,
        Doubles.asList(QueryRunnerTestHelper.UNIQUES_2, QueryRunnerTestHelper.UNIQUES_2, QueryRunnerTestHelper.UNIQUES_9)
    ));
    aggregations.add(new Pair<>(
        new DoubleMaxAggregatorFactory("maxIndex", "index"),
        Doubles.asList(1743.92175D, 1870.061029D, 277.273533D)
    ));
    aggregations.add(new Pair<>(
        new DoubleMinAggregatorFactory("minIndex", "index"),
        Doubles.asList(792.3260498046875D, 545.9906005859375D, 59.02102279663086D)
    ));
    aggregations.add(new Pair<>(
        new LongSumAggregatorFactory("qlLong", "qualityLong"),
        Longs.asList(279000L, 279000L, 1171800L)
    ));
    aggregations.add(new Pair<>(
        new DoubleSumAggregatorFactory("qlFloat", "qualityLong"),
        Doubles.asList(279000.0, 279000.0, 1171800.0)
    ));
    aggregations.add(new Pair<>(
        new DoubleSumAggregatorFactory("qfFloat", "qualityFloat"),
        Doubles.asList(2790000.0, 2790000.0, 11718000.0)
    ));
    aggregations.add(new Pair<>(
        new LongSumAggregatorFactory("qfLong", "qualityFloat"),
        Longs.asList(2790000L, 2790000L, 11718000L)
    ));

    List<List<Pair<AggregatorFactory, List<?>>>> aggregationCombinations = new ArrayList<>();
    for (Pair<AggregatorFactory, List<?>> aggregation : aggregations) {
      aggregationCombinations.add(Collections.singletonList(aggregation));
    }
    aggregationCombinations.add(aggregations);

    for (List<Pair<AggregatorFactory, List<?>>> aggregationCombination : aggregationCombinations) {
      boolean hasIndexAggregator = aggregationCombination.stream().anyMatch(agg -> "index".equals(agg.lhs.getName()));
      boolean hasRowsAggregator = aggregationCombination.stream().anyMatch(agg -> "rows".equals(agg.lhs.getName()));
      TopNQueryBuilder queryBuilder = new TopNQueryBuilder()
          .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
          .granularity(QueryRunnerTestHelper.ALL_GRAN)
          .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
          .threshold(4)
          .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
          .aggregators(aggregationCombination.stream().map(agg -> agg.lhs).collect(Collectors.toList()));
      String metric;
      if (hasIndexAggregator) {
        metric = "index";
      } else {
        metric = aggregationCombination.get(0).lhs.getName();
      }
      queryBuilder.metric(metric);
      if (hasIndexAggregator && hasRowsAggregator) {
        queryBuilder.postAggregators(QueryRunnerTestHelper.ADD_ROWS_INDEX_CONSTANT);
      }
      TopNQuery query = queryBuilder.build();

      ImmutableMap.Builder<String, Object> row1 = ImmutableMap.<String, Object>builder()
          .put(QueryRunnerTestHelper.MARKET_DIMENSION, "total_market");
      ImmutableMap.Builder<String, Object> row2 = ImmutableMap.<String, Object>builder()
          .put(QueryRunnerTestHelper.MARKET_DIMENSION, "upfront");
      ImmutableMap.Builder<String, Object> row3 = ImmutableMap.<String, Object>builder()
          .put(QueryRunnerTestHelper.MARKET_DIMENSION, "spot");
      if (hasIndexAggregator && hasRowsAggregator) {
        row1.put("addRowsIndexConstant", 215866.82879638672D);
        row2.put("addRowsIndexConstant", 192233.1060180664D);
        row3.put("addRowsIndexConstant", 96444.57232284546D);
      }
      aggregationCombination.forEach(agg -> {
        row1.put(agg.lhs.getName(), agg.rhs.get(0));
        row2.put(agg.lhs.getName(), agg.rhs.get(1));
        row3.put(agg.lhs.getName(), agg.rhs.get(2));
      });
      List<ImmutableMap<String, Object>> rows = Lists.newArrayList(row1.build(), row2.build(), row3.build());
      rows.sort((r1, r2) -> ((Comparable) r2.get(metric)).compareTo(r1.get(metric)));
      List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
          new Result<>(
              DateTimes.of("2011-01-12T00:00:00.000Z"),
              new TopNResultValue(rows)
          )
      );
      assertExpectedResults(expectedResults, query);
    }
  }


  @Test
  public void testFullOnTopNBoundFilterAndLongSumMetric()
  {
    // this tests the stack overflow issue from https://github.com/apache/druid/issues/4628
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION, "Market")
        .filters(new BoundDimFilter(
            QueryRunnerTestHelper.INDEX_METRIC,
            "0",
            "46.64980229268867",
            true,
            true,
            false,
            null,
            StringComparators.NUMERIC
        ))
        .metric("Count")
        .threshold(5)
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .aggregators(new LongSumAggregatorFactory("Count", "qualityLong"))
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(Collections.emptyList())
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  /**
   * Regression test for https://github.com/apache/druid/issues/5132
   */
  @Test
  public void testTopNWithNonBitmapFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .filters(new BoundDimFilter(
            ColumnHolder.TIME_COLUMN_NAME,
            "0",
            String.valueOf(Long.MAX_VALUE),
            true,
            true,
            false,
            null,
            StringComparators.NUMERIC
        ))
        .dimension(QueryRunnerTestHelper.MARKET_DIMENSION)
        .metric("count")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .aggregators(new DoubleSumAggregatorFactory("count", "qualityDouble"))
        .build();

    // Don't check results, just the fact that the query could complete
    Assert.assertNotNull(runWithMerge(query).toList());
  }

  @Test
  public void test_topN_orderByLongNumericColumnWithNulls_returnsDescendingResults()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("index", "index_alias", ValueType.LONG))
        .metric(new NumericTopNMetricSpec("longNumericNull"))
        .threshold(10000)
        .aggregators(new LongSumAggregatorFactory("longNumericNull", "longNumericNull"))
        .intervals(QueryRunnerTestHelper.SECOND_ONLY)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 97L)
                        .put("longNumericNull", 80L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 135L)
                        .put("longNumericNull", 70L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1049L)
                        .put("longNumericNull", 70L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1321L)
                        .put("longNumericNull", 70L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 110L)
                        .put("longNumericNull", 50L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1144L)
                        .put("longNumericNull", 50L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1193L)
                        .put("longNumericNull", 50L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 113L)
                        .put("longNumericNull", 40L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 112L)
                        .put("longNumericNull", 20L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 147L)
                        .put("longNumericNull", 10L)
                        .build(),
                    makeRowWithNulls("index_alias", 114L, "longNumericNull", NullHandling.defaultLongValue()),
                    makeRowWithNulls("index_alias", 126L, "longNumericNull", NullHandling.defaultLongValue()),
                    makeRowWithNulls("index_alias", 166L, "longNumericNull", NullHandling.defaultLongValue())
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void test_topN_orderByFloatNumericColumnWithNulls_returnsDescendingResults()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("index", "index_alias", ValueType.LONG))
        .metric(new NumericTopNMetricSpec("floatNumericNull"))
        .threshold(10000)
        .aggregators(new LongSumAggregatorFactory("floatNumericNull", "floatNumericNull"))
        .intervals(QueryRunnerTestHelper.SECOND_ONLY)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 97L)
                        .put("floatNumericNull", 80f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 135L)
                        .put("floatNumericNull", 70f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1049L)
                        .put("floatNumericNull", 70f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1321L)
                        .put("floatNumericNull", 70f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 110L)
                        .put("floatNumericNull", 50f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1144L)
                        .put("floatNumericNull", 50f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1193L)
                        .put("floatNumericNull", 50f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 113L)
                        .put("floatNumericNull", 40f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 112L)
                        .put("floatNumericNull", 20f)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 147L)
                        .put("floatNumericNull", 10f)
                        .build(),
                    makeRowWithNulls("index_alias", 114L, "floatNumericNull", NullHandling.defaultFloatValue()),
                    makeRowWithNulls("index_alias", 126L, "floatNumericNull", NullHandling.defaultFloatValue()),
                    makeRowWithNulls("index_alias", 166L, "floatNumericNull", NullHandling.defaultFloatValue())
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void test_topN_orderByDoubleNumericColumnWithNulls_returnsDescendingResults()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("index", "index_alias", ValueType.LONG))
        .metric(new NumericTopNMetricSpec("doubleNumericNull"))
        .threshold(10000)
        .aggregators(new LongSumAggregatorFactory("doubleNumericNull", "doubleNumericNull"))
        .intervals(QueryRunnerTestHelper.SECOND_ONLY)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 97L)
                        .put("doubleNumericNull", 80d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 135L)
                        .put("doubleNumericNull", 70d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1049L)
                        .put("doubleNumericNull", 70d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1321L)
                        .put("doubleNumericNull", 70d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 110L)
                        .put("doubleNumericNull", 50d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1144L)
                        .put("doubleNumericNull", 50d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1193L)
                        .put("doubleNumericNull", 50d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 113L)
                        .put("doubleNumericNull", 40d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 112L)
                        .put("doubleNumericNull", 20d)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 147L)
                        .put("doubleNumericNull", 10d)
                        .build(),
                    makeRowWithNulls("index_alias", 114L, "doubleNumericNull", NullHandling.defaultDoubleValue()),
                    makeRowWithNulls("index_alias", 126L, "doubleNumericNull", NullHandling.defaultDoubleValue()),
                    makeRowWithNulls("index_alias", 166L, "doubleNumericNull", NullHandling.defaultDoubleValue())
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }


  @Test
  public void testAggregateOnLongNumericNull()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("longNumericNull", "dim", ValueType.LONG))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(10000)
        .aggregators(new CountAggregatorFactory("count"))
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    makeRowWithNulls("dim", NullHandling.defaultLongValue(), "count", 279L),
                    makeRowWithNulls("dim", 10L, "count", 93L),
                    makeRowWithNulls("dim", 20L, "count", 93L),
                    makeRowWithNulls("dim", 40L, "count", 93L),
                    makeRowWithNulls("dim", 50L, "count", 279L),
                    makeRowWithNulls("dim", 70L, "count", 279L),
                    makeRowWithNulls("dim", 80L, "count", 93L)
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testAggregateOnDoubleNumericNull()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("doubleNumericNull", "dim", ValueType.DOUBLE))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(10000)
        .aggregators(new CountAggregatorFactory("count"))
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    makeRowWithNulls("dim", NullHandling.defaultDoubleValue(), "count", 279L),
                    makeRowWithNulls("dim", 10.0, "count", 93L),
                    makeRowWithNulls("dim", 20.0, "count", 93L),
                    makeRowWithNulls("dim", 40.0, "count", 93L),
                    makeRowWithNulls("dim", 50.0, "count", 279L),
                    makeRowWithNulls("dim", 70.0, "count", 279L),
                    makeRowWithNulls("dim", 80.0, "count", 93L)
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testAggregateOnFloatNumericNull()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .granularity(QueryRunnerTestHelper.ALL_GRAN)
        .dimension(new DefaultDimensionSpec("floatNumericNull", "dim", ValueType.FLOAT))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(10000)
        .aggregators(new CountAggregatorFactory("count"))
        .intervals(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
        .build();

    List<Result<TopNResultValue>> expectedResults = Collections.singletonList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    makeRowWithNulls("dim", NullHandling.defaultFloatValue(), "count", 279L),
                    makeRowWithNulls("dim", 10.0f, "count", 93L),
                    makeRowWithNulls("dim", 20.0f, "count", 93L),
                    makeRowWithNulls("dim", 40.0f, "count", 93L),
                    makeRowWithNulls("dim", 50.0f, "count", 279L),
                    makeRowWithNulls("dim", 70.0f, "count", 279L),
                    makeRowWithNulls("dim", 80.0f, "count", 93L)
                )
            )
        )
    );
    assertExpectedResults(expectedResults, query);
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
}
