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

package io.druid.query.topn;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.druid.collections.StupidPool;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.js.JavaScriptConfig;
import io.druid.math.expr.ExprMacroTable;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.FloatMaxAggregatorFactory;
import io.druid.query.aggregation.FloatMinAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.first.DoubleFirstAggregatorFactory;
import io.druid.query.aggregation.first.FloatFirstAggregatorFactory;
import io.druid.query.aggregation.first.LongFirstAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.last.FloatLastAggregatorFactory;
import io.druid.query.aggregation.last.LongLastAggregatorFactory;
import io.druid.query.aggregation.post.ExpressionPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.dimension.ListFilteredDimensionSpec;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.extraction.StringFormatExtractionFn;
import io.druid.query.extraction.StrlenExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestHelper;
import io.druid.segment.column.Column;
import io.druid.segment.column.ValueType;
import io.druid.segment.virtual.ExpressionVirtualColumn;
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
public class TopNQueryRunnerTest
{
  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
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
        params[6] = QueryRunnerTestHelper.commonDoubleAggregators;
        Object[] params2 = Arrays.copyOf(params, 7);
        params2[6] = QueryRunnerTestHelper.commonFloatAggregators;
        parameters.add(params);
        parameters.add(params2);
      }
    }
    return parameters;
  }

  public static List<QueryRunner<Result<TopNResultValue>>> queryRunners() throws IOException
  {
    List<QueryRunner<Result<TopNResultValue>>> retVal = Lists.newArrayList();
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                TestQueryRunners.getPool(),
                new TopNQueryQueryToolChest(
                    new TopNQueryConfig(),
                    QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
                ),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                new StupidPool<ByteBuffer>(
                    "TopNQueryRunnerFactory-bufferPool",
                    () -> ByteBuffer.allocate(20000)
                ),
                new TopNQueryQueryToolChest(
                    new TopNQueryConfig(),
                    QueryRunnerTestHelper.sameThreadIntervalChunkingQueryRunnerDecorator()
                ),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
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

  private Sequence<Result<TopNResultValue>> runWithMerge(
      TopNQuery query
  )
  {
    return runWithMerge(query, ImmutableMap.<String, Object>of());
  }

  private Sequence<Result<TopNResultValue>> runWithMerge(
      TopNQuery query, Map<String, Object> context
  )
  {
    final TopNQueryQueryToolChest chest = new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.emptyInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
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
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.92175D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.061029D)
                                .put("minIndex", 545.9906005859375D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "spot")
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
                          query.withAggregatorSpecs(Lists.<AggregatorFactory>newArrayList(Iterables.concat(
                              QueryRunnerTestHelper.commonFloatAggregators,
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("nonexistentColumn", "alias"))
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(Collections.singletonList(new CountAggregatorFactory("rows")))
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new ExtractionDimensionSpec("nonexistentColumn", "alias", new StringFormatExtractionFn("theValue")))
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(Collections.singletonList(new CountAggregatorFactory("rows")))
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.addRowsIndexConstantMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.92175D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.061029D)
                                .put("minIndex", 545.9906005859375D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "spot")
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric("dimPostAgg")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
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
            ImmutableList.of(
                new ExpressionPostAggregator(
                    "dimPostAgg",
                    "market + 'x'",
                    null,
                    TestExprMacroTable.INSTANCE
                )
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "upfront")
                        .put("dimPostAgg", "upfrontx")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "total_market")
                        .put("dimPostAgg", "total_marketx")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "spot")
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(new HyperUniquesAggregatorFactory("uniques", "missingUniques"))
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put("uniques", 0)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put("uniques", 0)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put("uniques", 0)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.qualityUniques)
        )
        .postAggregators(
            Arrays.<PostAggregator>asList(new HyperUniqueFinalizingPostAggregator(
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                QueryRunnerTestHelper.uniqueMetric
            ))
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put(QueryRunnerTestHelper.uniqueMetric, QueryRunnerTestHelper.UNIQUES_9)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, QueryRunnerTestHelper.UNIQUES_9)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put(QueryRunnerTestHelper.uniqueMetric, QueryRunnerTestHelper.UNIQUES_2)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, QueryRunnerTestHelper.UNIQUES_2)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put(QueryRunnerTestHelper.uniqueMetric, QueryRunnerTestHelper.UNIQUES_2)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, QueryRunnerTestHelper.UNIQUES_2)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.qualityUniques)
        )
        .postAggregators(
            Collections.singletonList(new ExpressionPostAggregator(
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                "uniques + 1",
                null,
                TestExprMacroTable.INSTANCE
            ))
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put(QueryRunnerTestHelper.uniqueMetric, QueryRunnerTestHelper.UNIQUES_9)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, QueryRunnerTestHelper.UNIQUES_9 + 1)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put(QueryRunnerTestHelper.uniqueMetric, QueryRunnerTestHelper.UNIQUES_2)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, QueryRunnerTestHelper.UNIQUES_2 + 1)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put(QueryRunnerTestHelper.uniqueMetric, QueryRunnerTestHelper.UNIQUES_2)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, QueryRunnerTestHelper.UNIQUES_2 + 1)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(QueryRunnerTestHelper.qualityUniquesRounded)
        )
        .postAggregators(
            Collections.singletonList(new ExpressionPostAggregator(
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                "uniques + 1",
                null,
                TestExprMacroTable.INSTANCE
            ))
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("market", "spot")
                        .put(QueryRunnerTestHelper.uniqueMetric, 9L)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, 10L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "total_market")
                        .put(QueryRunnerTestHelper.uniqueMetric, 2L)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, 3L)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("market", "upfront")
                        .put(QueryRunnerTestHelper.uniqueMetric, 2L)
                        .put(QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric, 3L)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.monthGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric("last")
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                new LongFirstAggregatorFactory("first", "index"),
                new LongLastAggregatorFactory("last", "index")
            )
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
  public void testTopNOverFirstLastAggregatorChunkPeriod()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.monthGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric("last")
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                new LongFirstAggregatorFactory("first", "index"),
                new LongLastAggregatorFactory("last", "index")
            )
        )
        .context(ImmutableMap.<String, Object>of("chunkPeriod", "P1D"))
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

    final Sequence<Result<TopNResultValue>> retval = runWithPreMergeAndMerge(query);
    TestHelper.assertExpectedResults(expectedResults, retval);
  }

  @Test
  public void testTopNOverFirstLastFloatAggregatorUsingDoubleColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.monthGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric("last")
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                new FloatFirstAggregatorFactory("first", "index"),
                new FloatLastAggregatorFactory("last", "index")
            )
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.monthGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric("last")
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.<AggregatorFactory>asList(
                new FloatFirstAggregatorFactory("first", "indexFloat"),
                new FloatLastAggregatorFactory("last", "indexFloat")
            )
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .context(specialContext)
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 5356.814783D,
                        "index", 5351.814783D,
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "uniques", QueryRunnerTestHelper.UNIQUES_2,
                        "rows", 4L
                    ),
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 4880.669692D,
                        "index", 4875.669692D,
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "uniques", QueryRunnerTestHelper.UNIQUES_2,
                        "rows", 4L
                    ),
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 2250.876812D,
                        "index", 2231.876812D,
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "uniques", QueryRunnerTestHelper.UNIQUES_9,
                        "rows", 18L
                    )
                )
            )
        )
    );
    Sequence<Result<TopNResultValue>> results = runWithMerge(
        query,
        specialContext
    );
    List<Result<BySegmentTopNResultValue>> resultList = Sequences.toList(
        Sequences.map(
            results,
            new Function<Result<TopNResultValue>, Result<BySegmentTopNResultValue>>()
            {
              @Nullable
              @Override
              public Result<BySegmentTopNResultValue> apply(
                  Result<TopNResultValue> input
              )
              {
                // Stupid type erasure
                Object val = input.getValue();
                if (val instanceof BySegmentResultValue) {
                  BySegmentResultValue bySegVal = (BySegmentResultValue) val;
                  List<?> results = bySegVal.getResults();
                  return new Result<BySegmentTopNResultValue>(
                      input.getTimestamp(),
                      new BySegmentTopNResultValue(
                          Lists.transform(
                              results,
                              new Function<Object, Result<TopNResultValue>>()
                              {
                                @Nullable
                                @Override
                                public Result<TopNResultValue> apply(@Nullable Object input)
                                {
                                  if (Preconditions.checkNotNull(input) instanceof Result) {
                                    Result result = (Result) input;
                                    Object resVal = result.getValue();
                                    if (resVal instanceof TopNResultValue) {
                                      return new Result<TopNResultValue>(
                                          result.getTimestamp(),
                                          (TopNResultValue) resVal
                                      );
                                    }
                                  }
                                  throw new IAE("Bad input: [%s]", input);
                                }
                              }
                          ),
                          bySegVal.getSegmentId(),
                          bySegVal.getInterval()
                      )
                  );
                }
                throw new ISE("Bad type");
              }
            }
        ),
        Lists.<Result<BySegmentTopNResultValue>>newArrayList()
    );
    Result<BySegmentTopNResultValue> result = resultList.get(0);
    TestHelper.assertExpectedResults(expectedResults, result.getValue().getResults());
  }

  @Test
  public void testTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new NumericTopNMetricSpec("uniques"))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.marketDimension, "total_market", "upfront", "spot")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.marketDimension, "total_market", "upfront")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.marketDimension, "upfront")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 2L,
                        "index", 2591.68359375D,
                        "addRowsIndexConstant", 2594.68359375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 2L,
                        "index", 2508.39599609375D,
                        "addRowsIndexConstant", 2511.39599609375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(
            new MultipleIntervalSegmentSpec(
                Arrays.asList(Intervals.of("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"))
            )
        )
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 1L,
                        "index", new Float(1447.341160).doubleValue(),
                        "addRowsIndexConstant", new Float(1449.341160).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 1L,
                        "index", new Float(1314.839715).doubleValue(),
                        "addRowsIndexConstant", new Float(1316.839715).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.marketDimension, "total_market", "upfront", "billyblank")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.marketDimension, "billyblank")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    assertExpectedResults(
        Lists.<Result<TopNResultValue>>newArrayList(
            new Result<TopNResultValue>(
                DateTimes.of("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ),
        query
    );
  }

  @Test
  public void testTopNWithNonExistentFilterMultiDim()
  {
    AndDimFilter andDimFilter = new AndDimFilter(
        new SelectorDimFilter(QueryRunnerTestHelper.marketDimension, "billyblank", null),
        new SelectorDimFilter(QueryRunnerTestHelper.qualityDimension, "mezzanine", null)
    );
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(andDimFilter)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    assertExpectedResults(
        Lists.<Result<TopNResultValue>>newArrayList(
            new Result<TopNResultValue>(
                DateTimes.of("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ), query
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "m")
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    assertExpectedResults(
        Sequences.toList(
            runWithMerge(
                new TopNQueryBuilder()
                    .dataSource(QueryRunnerTestHelper.dataSource)
                    .granularity(QueryRunnerTestHelper.allGran)
                    .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
                    .dimension(QueryRunnerTestHelper.marketDimension)
                    .metric(QueryRunnerTestHelper.indexMetric)
                    .threshold(4)
                    .intervals(QueryRunnerTestHelper.firstToThird)
                    .aggregators(commonAggregators)
                    .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                    .build()
            ), Lists.<Result<TopNResultValue>>newArrayList()
        ), query
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "m", "a", "b")
        .dimension(QueryRunnerTestHelper.qualityDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    assertExpectedResults(
        Sequences.toList(
            runWithMerge(
                new TopNQueryBuilder()
                    .dataSource(QueryRunnerTestHelper.dataSource)
                    .granularity(QueryRunnerTestHelper.allGran)
                    .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine", "automotive", "business")
                    .dimension(QueryRunnerTestHelper.qualityDimension)
                    .metric(QueryRunnerTestHelper.indexMetric)
                    .threshold(4)
                    .intervals(QueryRunnerTestHelper.firstToThird)
                    .aggregators(commonAggregators)
                    .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                    .build()
            ), Lists.<Result<TopNResultValue>>newArrayList()
        ), query
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter3()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "a")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "a",
                        "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "a", "b")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 4L,
                        "index", 514.868408203125D,
                        "addRowsIndexConstant", 519.868408203125D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish",
                        "a", "rows", 2L,
                        "index", 283.31103515625D,
                        "addRowsIndexConstant", 286.31103515625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "preferred")
        .dimension(QueryRunnerTestHelper.placementishDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "placementish", "preferred",
                        "rows", 26L,
                        "index", 12459.361190795898D,
                        "addRowsIndexConstant", 12486.361190795898D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "p",
                        "rows", 6L,
                        "index", 5407.213653564453D,
                        "addRowsIndexConstant", 5414.213653564453D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        "placementish", "m",
                        "rows", 6L,
                        "index", 5320.717338562012D,
                        "addRowsIndexConstant", 5327.717338562012D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(1)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.marketDimension, "upfront")
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters("doesn't exist", null)
        .dimension("doesn't exist")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(1)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Collections.<Map<String, Object>>singletonList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec("", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec("", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot"
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market"
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront"
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec("spot", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec("t", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("upfront", StringComparators.LEXICOGRAPHIC)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("u", StringComparators.LEXICOGRAPHIC)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
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
  public void testTopNDimExtractionToOne() throws IOException
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new JavaScriptExtractionFn("function(f) { return \"POTATO\"; }", false, JavaScriptConfig.getEnabledInstance())
            )
        )
        .metric("rows")
        .threshold(10)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 504542.5071372986D,
                        "index", 503332.5071372986D,
                        QueryRunnerTestHelper.marketDimension, "POTATO",
                        "uniques", QueryRunnerTestHelper.UNIQUES_9,
                        "rows", 1209L
                    )
                )
            )
        )
    );
    List<Result<TopNResultValue>> list = Sequences.toList(
        runWithMerge(query),
        new ArrayList<Result<TopNResultValue>>()
    );
    Assert.assertEquals(list.size(), 1);
    Assert.assertEquals("Didn't merge results", list.get(0).getValue().getValue().size(), 1);
    TestHelper.assertExpectedResults(expectedResults, list, "Failed to match");
  }

  @Test
  public void testTopNCollapsingDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.qualityDimension, QueryRunnerTestHelper.qualityDimension,
                new RegexDimExtractionFn(".(.)", false, null)
            )
        )
        .metric("index")
        .threshold(2)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.indexDoubleSum
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.qualityDimension, "e",
                        "rows", 558L,
                        "index", 246645.1204032898,
                        "addRowsIndexConstant", 247204.1204032898
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.qualityDimension, "r",
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
            QueryRunnerTestHelper.rowsCount,
            new DoubleSumAggregatorFactory("index", null, "-index + 100", ExprMacroTable.nil())
        )
    );

    expectedResults = Arrays.asList(
        TopNQueryRunnerTestHelper.createExpectedRows(
            "2011-01-12T00:00:00.000Z",
            new String[]{QueryRunnerTestHelper.qualityDimension, "rows", "index", "addRowsIndexConstant"},
            Arrays.asList(
                new Object[]{"n", 93L, -2786.4727909999997, -2692.4727909999997},
                new Object[]{"u", 186L, -3949.824348000002, -3762.824348000002}
            )
        )
    );

    assertExpectedResults(expectedResults, query);
  }

  @Test
  public void testTopNDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric(new LexicographicTopNMetricSpec(QueryRunnerTestHelper.marketDimension))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s"
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t"
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u"
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot0",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market0",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront0",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("..(.)", false, null)
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "o",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "f",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric(new DimensionTopNMetricSpec("s", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension, QueryRunnerTestHelper.marketDimension,
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("(.)", false, null)
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("u", StringComparators.LEXICOGRAPHIC)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                new RegexDimExtractionFn("..(.)", false, null)
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("p", StringComparators.LEXICOGRAPHIC)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "o",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "f",
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
        return dimValue.equals("total_market") ? null : dimValue;
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                nullStringDimExtraction
            )
        )
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    QueryRunnerTestHelper.orderedMap(
                        QueryRunnerTestHelper.marketDimension, null,
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
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
        return dimValue.equals("total_market") ? "" : dimValue;
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                emptyStringDimExtraction
            )
        )
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    QueryRunnerTestHelper.orderedMap(
                        QueryRunnerTestHelper.marketDimension, "",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
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
            .dataSource(QueryRunnerTestHelper.dataSource)
            .granularity(QueryRunnerTestHelper.allGran)
            .dimension(QueryRunnerTestHelper.marketDimension)
            .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec(QueryRunnerTestHelper.indexMetric)))
            .threshold(3)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(commonAggregators)
            .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669692D,
                        "addRowsIndexConstant", 4880.669692D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
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
    ImmutableList<DimensionSpec> aggregatorDimensionSpecs = ImmutableList.<DimensionSpec>of(new DefaultDimensionSpec(
        QueryRunnerTestHelper.qualityDimension,
        QueryRunnerTestHelper.qualityDimension
    ));
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.dataSource)
            .granularity(QueryRunnerTestHelper.allGran)
            .dimension(QueryRunnerTestHelper.marketDimension)
            .metric(new NumericTopNMetricSpec("numVals"))
            .threshold(10)
            .intervals(QueryRunnerTestHelper.firstToThird)
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
                        ImmutableMap.<String, Object>of(
                            "market", "spot",
                            "numVals", 9.019833517963864d
                        ),
                        ImmutableMap.<String, Object>of(
                            "market", "total_market",
                            "numVals", 2.000977198748901d
                        ),
                        ImmutableMap.<String, Object>of(
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

    DimensionSpec dimSpec = new ExtractionDimensionSpec(QueryRunnerTestHelper.marketDimension,
                                                        QueryRunnerTestHelper.marketDimension,
                                                        helloFn);

    ImmutableList<DimensionSpec> aggregatorDimensionSpecs = ImmutableList.<DimensionSpec>of(new ExtractionDimensionSpec(
        QueryRunnerTestHelper.qualityDimension,
        QueryRunnerTestHelper.qualityDimension,
        helloFn
    ));
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.dataSource)
            .granularity(QueryRunnerTestHelper.allGran)
            .dimension(dimSpec)
            .metric(new NumericTopNMetricSpec("numVals"))
            .threshold(10)
            .intervals(QueryRunnerTestHelper.firstToThird)
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
                        ImmutableMap.<String, Object>of(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
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
            Arrays.<PostAggregator>asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg,
                QueryRunnerTestHelper.hyperUniqueFinalizingPostAgg
            )
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.92175D)
                                .put("minIndex", 792.3260498046875D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.061029D)
                                .put("minIndex", 545.9906005859375D)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
                                    QueryRunnerTestHelper.UNIQUES_2 + 1.0
                                )
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "spot")
                                .put("rows", 837L)
                                .put("index", 95606.57232284546D)
                                .put("addRowsIndexConstant", 96444.57232284546D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                                .put(
                                    QueryRunnerTestHelper.hyperUniqueFinalizingPostAggMetric,
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
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
            Arrays.<PostAggregator>asList(
                QueryRunnerTestHelper.addRowsIndexConstant,
                QueryRunnerTestHelper.dependentPostAgg
            )
        )
        .context(ImmutableMap.<String, Object>of("finalize", true, "bySegment", true))
        .build();
    TopNResultValue topNResult = new TopNResultValue(
        Arrays.<Map<String, Object>>asList(
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "total_market")
                        .put("rows", 186L)
                        .put("index", 215679.82879638672D)
                        .put("addRowsIndexConstant", 215866.82879638672D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1743.92175D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "spot")
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
    List<Result<BySegmentResultValueClass>> expectedResults = Collections.singletonList(
        new Result<BySegmentResultValueClass>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new BySegmentResultValueClass(
                Collections.singletonList(
                    new Result<TopNResultValue>(
                        DateTimes.of("2011-01-12T00:00:00.000Z"),
                        topNResult
                    )
                ),
                QueryRunnerTestHelper.segmentId,
                Intervals.of("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z")
            )
        )
    );
    Sequence<Result<TopNResultValue>> results = runWithMerge(query);
    for (Result<TopNResultValue> result : Sequences.toList(results, new ArrayList<Result<TopNResultValue>>())) {
      Assert.assertEquals(result.getValue(), result.getValue()); // TODO: fix this test
    }
  }

  @Test
  public void testTopNWithTimeColumn()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.jsCountIfTimeGreaterThan,
                QueryRunnerTestHelper.__timeLongSum
            )
        )
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric("ntimestamps")
        .threshold(3)
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows",
                        18L,
                        "ntimestamps",
                        9.0,
                        "sumtime",
                        23429865600000L
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows",
                        4L,
                        "ntimestamps",
                        2.0,
                        "sumtime",
                        5206636800000L
                    ),
                    ImmutableMap.<String, Object>of(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                Column.TIME_COLUMN_NAME,
                "dayOfWeek",
                new TimeFormatExtractionFn("EEEE", null, null, null, false)
            )
        )
        .metric("index")
        .threshold(2)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.asList(
                QueryRunnerTestHelper.rowsCount,
                QueryRunnerTestHelper.indexDoubleSum
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "dayOfWeek", "Wednesday",
                        "rows", 182L,
                        "index", 76010.28100585938,
                        "addRowsIndexConstant", 76193.28100585938
                    ),
                    ImmutableMap.<String, Object>of(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
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
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("null_column", null);
    map.put("rows", 1209L);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.061029D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension("null_column")
        .filters(
            new SelectorDimFilter("null_column", null, null)
        )
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
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
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("null_column", null);
    map.put("rows", 1209L);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.061029D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(Granularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("partial_null_column", null);
    map.put("rows", 22L);
    map.put("index", 7583.691513061523D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(Granularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .filters(new SelectorDimFilter("partial_null_column", null, null))
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("partial_null_column", null);
    map.put("rows", 22L);
    map.put("index", 7583.691513061523D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(Granularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .filters(new SelectorDimFilter("partial_null_column", "value", null))
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec(null, StringComparators.ALPHANUMERIC))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.secondOnly)
        .aggregators(duplicateAggregators(
            QueryRunnerTestHelper.rowsCount,
            new CountAggregatorFactory("rows1")
        ))
        .build();
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                withDuplicateResults(
                    Arrays.asList(
                        ImmutableMap.<String, Object>of(
                            "market", "spot",
                            "rows", 9L
                        ),
                        ImmutableMap.<String, Object>of(
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
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query), new HashMap<String, Object>()));
  }

  @Test
  public void testNumericDimensionTopNWithNullPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(Granularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.secondOnly)
        .aggregators(duplicateAggregators(
            QueryRunnerTestHelper.rowsCount,
            new CountAggregatorFactory("rows1")
        ))
        .build();
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                withDuplicateResults(
                    Arrays.asList(
                        ImmutableMap.<String, Object>of(
                            "market", "spot",
                            "rows", 9L
                        ),
                        ImmutableMap.<String, Object>of(
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
    TestHelper.assertExpectedResults(expectedResults, runner.run(QueryPlus.wrap(query), new HashMap<String, Object>()));
  }


  @Test
  public void testTopNWithExtractionFilter()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("spot", "spot0");
    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);

    TopNQuery query = new TopNQueryBuilder().dataSource(QueryRunnerTestHelper.dataSource)
                                            .granularity(QueryRunnerTestHelper.allGran)
                                            .dimension(QueryRunnerTestHelper.marketDimension)
                                            .metric("rows")
                                            .threshold(3)
                                            .intervals(QueryRunnerTestHelper.firstToThird)
                                            .aggregators(commonAggregators)
                                            .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                            .filters(
                                                new ExtractionDimFilter(
                                                    QueryRunnerTestHelper.marketDimension,
                                                    "spot0",
                                                    lookupExtractionFn,
                                                    null
                                                )
                                            )
                                            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
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
    extractionMap.put("", "NULL");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, false);
    DimFilter extractionFilter = new ExtractionDimFilter("null_column", "NULL", lookupExtractionFn, null);
    TopNQueryBuilder topNQueryBuilder = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.newArrayList(
                Iterables.concat(
                    commonAggregators, Lists.newArrayList(
                        new FilteredAggregatorFactory(
                            new DoubleMaxAggregatorFactory("maxIndex", "index"),
                            extractionFilter
                        ),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant));
    TopNQuery topNQueryWithNULLValueExtraction = topNQueryBuilder
        .filters(extractionFilter)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("null_column", null);
    map.put("rows", 1209L);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.061029D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, topNQueryWithNULLValueExtraction);
  }

  private Sequence<Result<TopNResultValue>> runWithPreMergeAndMerge(TopNQuery query)
  {
    return runWithPreMergeAndMerge(query, ImmutableMap.<String, Object>of());
  }

  private Sequence<Result<TopNResultValue>> runWithPreMergeAndMerge(TopNQuery query, Map<String, Object> context)
  {
    final TopNQueryQueryToolChest chest = new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );
    final QueryRunner<Result<TopNResultValue>> Runner = new FinalizeResultsQueryRunner(
        chest.mergeResults(chest.preMergeQueryDecoration(runner)),
        chest
    );
    return Runner.run(QueryPlus.wrap(query), context);
  }

  @Test
  public void testTopNWithExtractionFilterNoExistingValue()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("", "NULL");

    MapLookupExtractor mapLookupExtractor = new MapLookupExtractor(extractionMap, false);
    LookupExtractionFn lookupExtractionFn = new LookupExtractionFn(mapLookupExtractor, false, null, true, true);
    DimFilter extractionFilter = new ExtractionDimFilter("null_column", "NULL", lookupExtractionFn, null);
    TopNQueryBuilder topNQueryBuilder = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension("null_column")
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(Lists.newArrayList(Iterables.concat(commonAggregators, Lists.newArrayList(
            new FilteredAggregatorFactory(new DoubleMaxAggregatorFactory("maxIndex", "index"),
                                          extractionFilter),
            //new DoubleMaxAggregatorFactory("maxIndex", "index"),
            new DoubleMinAggregatorFactory("minIndex", "index")))))
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant));
    TopNQuery topNQueryWithNULLValueExtraction = topNQueryBuilder
        .filters(extractionFilter)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("null_column", null);
    map.put("rows", 1209L);
    map.put("index", 503332.5071372986D);
    map.put("addRowsIndexConstant", 504542.5071372986D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    map.put("maxIndex", 1870.061029D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec(QueryRunnerTestHelper.indexMetric, "index_alias", ValueType.FLOAT))
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1000.0f)
                        .put(QueryRunnerTestHelper.indexMetric, 2000.0D)
                        .put("rows", 2L)
                        .put("addRowsIndexConstant", 2003.0D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1000.0D)
                        .put("minIndex", 1000.0D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1870.061029f)
                        .put(QueryRunnerTestHelper.indexMetric, 1870.061029D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1872.06103515625D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 1870.061029D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1862.737933f)
                        .put(QueryRunnerTestHelper.indexMetric, 1862.737933D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1864.7379150390625D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 1862.737933D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", 1743.92175f)
                        .put(QueryRunnerTestHelper.indexMetric, 1743.92175D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new ExtractionDimensionSpec(QueryRunnerTestHelper.indexMetric, "index_alias", jsExtractionFn))
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", "super-1000")
                        .put(QueryRunnerTestHelper.indexMetric, 2000.0D)
                        .put("rows", 2L)
                        .put("addRowsIndexConstant", 2003.0D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1000.0D)
                        .put("minIndex", 1000.0D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", "super-1870.061029")
                        .put(QueryRunnerTestHelper.indexMetric, 1870.061029D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1872.06103515625D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 1870.061029D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", "super-1862.737933")
                        .put(QueryRunnerTestHelper.indexMetric, 1862.737933D)
                        .put("rows", 1L)
                        .put("addRowsIndexConstant", 1864.7379150390625D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 1862.737933D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("index_alias", "super-1743.92175")
                        .put(QueryRunnerTestHelper.indexMetric, 1743.92175D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("qualityFloat", "qf_alias"))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("qf_alias", "14000.0")
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qf_alias", "16000.0")
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qf_alias", "10000.0")
                        .put(QueryRunnerTestHelper.indexMetric, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qf_alias", "12000.0")
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("qualityLong", "ql_alias", ValueType.LONG))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1400L)
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1600L)
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1000L)
                        .put(QueryRunnerTestHelper.indexMetric, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1200L)
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("ql_expr", "ql_alias", ValueType.LONG))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .virtualColumns(new ExpressionVirtualColumn("ql_expr", "qualityLong", ValueType.LONG, ExprMacroTable.nil()))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1400L)
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1600L)
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1000L)
                        .put(QueryRunnerTestHelper.indexMetric, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1200L)
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
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
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            DateTimes.of("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "vc", "spot spot",
                        "rows", 18L,
                        "index", 2231.876812D,
                        "addRowsIndexConstant", 2250.876812D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        "vc", "total_market total_market",
                        "rows", 4L,
                        "index", 5351.814783D,
                        "addRowsIndexConstant", 5356.814783D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new ExtractionDimensionSpec("qualityLong", "ql_alias", jsExtractionFn))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "super-1400")
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "super-1600")
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "super-1000")
                        .put(QueryRunnerTestHelper.indexMetric, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "super-1200")
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("qualityLong", "ql_alias"))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "1400")
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "1600")
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "1000")
                        .put(QueryRunnerTestHelper.indexMetric, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", "1200")
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("qualityNumericString", "qns_alias", ValueType.LONG))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 140000L)
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 160000L)
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 100000L)
                        .put(QueryRunnerTestHelper.indexMetric, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 120000L)
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("qualityNumericString", "qns_alias", ValueType.FLOAT))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 140000.0f)
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 160000.0f)
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 100000.0f)
                        .put(QueryRunnerTestHelper.indexMetric, 12270.807093D)
                        .put("rows", 93L)
                        .put("addRowsIndexConstant", 12364.807093D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 71.315931D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 120000.0f)
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec(Column.TIME_COLUMN_NAME, "time_alias", ValueType.LONG))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", 1296345600000L)
                        .put(QueryRunnerTestHelper.indexMetric, 5497.331253051758D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 5511.331253051758D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 97.02391052246094D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", 1298678400000L)
                        .put(QueryRunnerTestHelper.indexMetric, 6541.463027954102D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6555.463027954102D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 83.099365234375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", 1301529600000L)
                        .put(QueryRunnerTestHelper.indexMetric, 6814.467971801758D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6828.467971801758D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1734.27490234375D)
                        .put("minIndex", 93.39083862304688D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", 1294876800000L)
                        .put(QueryRunnerTestHelper.indexMetric, 6077.949111938477D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("index", "index_alias", ValueType.LONG))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("__time", "__time_alias", ValueType.LONG))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("market", "alias", ValueType.DOUBLE))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .build();

    final Map<String, Object> nullAliasMap = new HashMap<>();
    nullAliasMap.put("alias", 0.0d);

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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new DefaultDimensionSpec("index", "index_alias", ValueType.DOUBLE))
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new ExtractionDimensionSpec(Column.TIME_COLUMN_NAME, "time_alias", jsExtractionFn))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", "super-1296345600000")
                        .put(QueryRunnerTestHelper.indexMetric, 5497.331253051758D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 5511.331253051758D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 97.02391052246094D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", "super-1298678400000")
                        .put(QueryRunnerTestHelper.indexMetric, 6541.463027954102D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6555.463027954102D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 83.099365234375D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", "super-1301529600000")
                        .put(QueryRunnerTestHelper.indexMetric, 6814.467971801758D)
                        .put("rows", 13L)
                        .put("addRowsIndexConstant", 6828.467971801758D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 1734.27490234375D)
                        .put("minIndex", 93.39083862304688D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("time_alias", "super-1294876800000")
                        .put(QueryRunnerTestHelper.indexMetric, 6077.949111938477D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new ExtractionDimensionSpec(
            QueryRunnerTestHelper.marketDimension,
            QueryRunnerTestHelper.marketDimension,
            jsExtractionFn
        ))
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    Map<String, Object> expectedMap = new HashMap<>();
    expectedMap.put(QueryRunnerTestHelper.marketDimension, null);
    expectedMap.put("rows", 1209L);
    expectedMap.put("index", 503332.5071372986D);
    expectedMap.put("addRowsIndexConstant", 504542.5071372986D);
    expectedMap.put("uniques", 9.019833517963864);
    expectedMap.put("maxIndex", 1870.061029D);
    expectedMap.put("minIndex", 59.02102279663086D);

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(new ExtractionDimensionSpec(QueryRunnerTestHelper.qualityDimension, "alias", ValueType.LONG, strlenFn))
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("alias", 9L)
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("alias", 7L)
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("alias", 10L)
                        .put(QueryRunnerTestHelper.indexMetric, 20479.497562408447D)
                        .put("rows", 186L)
                        .put("addRowsIndexConstant", 20666.497562408447D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 277.273533D)
                        .put("minIndex", 59.02102279663086D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("alias", 13L)
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(filteredSpec)
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 140000L)
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 160000L)
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("qns_alias", 120000L)
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(filteredSpec)
        .metric("maxIndex")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    commonAggregators,
                    Lists.newArrayList(
                        new DoubleMaxAggregatorFactory("maxIndex", "index"),
                        new DoubleMinAggregatorFactory("minIndex", "index")
                    )
                )
            )
        )
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1400L)
                        .put(QueryRunnerTestHelper.indexMetric, 217725.41940800005D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 218005.41940800005D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1870.061029D)
                        .put("minIndex", 91.270553D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1600L)
                        .put(QueryRunnerTestHelper.indexMetric, 210865.67977600006D)
                        .put("rows", 279L)
                        .put("addRowsIndexConstant", 211145.67977600006D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_1)
                        .put("maxIndex", 1862.737933D)
                        .put("minIndex", 99.284525D)
                        .build(),
                    ImmutableMap.<String, Object>builder()
                        .put("ql_alias", 1200L)
                        .put(QueryRunnerTestHelper.indexMetric, 12086.472791D)
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
        QueryRunnerTestHelper.rowsCount,
        Longs.asList(186L, 186L, 837L)
    ));
    Pair<AggregatorFactory, List<?>> indexAggregation = new Pair<>(
        QueryRunnerTestHelper.indexDoubleSum,
        Doubles.asList(215679.82879638672D, 192046.1060180664D, 95606.57232284546D)
    );
    aggregations.add(indexAggregation);
    aggregations.add(new Pair<>(
        QueryRunnerTestHelper.qualityUniques,
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
          .dataSource(QueryRunnerTestHelper.dataSource)
          .granularity(QueryRunnerTestHelper.allGran)
          .dimension(QueryRunnerTestHelper.marketDimension)
          .threshold(4)
          .intervals(QueryRunnerTestHelper.fullOnInterval)
          .aggregators(aggregationCombination.stream().map(agg -> agg.lhs).collect(Collectors.toList()));
      String metric;
      if (hasIndexAggregator) {
        metric = "index";
      } else {
        metric = aggregationCombination.get(0).lhs.getName();
      }
      queryBuilder.metric(metric);
      if (hasIndexAggregator && hasRowsAggregator) {
        queryBuilder.postAggregators(Collections.singletonList(QueryRunnerTestHelper.addRowsIndexConstant));
      }
      TopNQuery query = queryBuilder.build();

      ImmutableMap.Builder<String, Object> row1 = ImmutableMap.<String, Object>builder()
          .put(QueryRunnerTestHelper.marketDimension, "total_market");
      ImmutableMap.Builder<String, Object> row2 = ImmutableMap.<String, Object>builder()
          .put(QueryRunnerTestHelper.marketDimension, "upfront");
      ImmutableMap.Builder<String, Object> row3 = ImmutableMap.<String, Object>builder()
          .put(QueryRunnerTestHelper.marketDimension, "spot");
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
    // this tests the stack overflow issue from https://github.com/druid-io/druid/issues/4628
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(QueryRunnerTestHelper.marketDimension, "Market")
        .filters(new BoundDimFilter(
            QueryRunnerTestHelper.indexMetric,
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
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Arrays.asList(new LongSumAggregatorFactory("Count", "qualityLong"))
        )
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            DateTimes.of("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(Arrays.asList())
        )
    );
    assertExpectedResults(expectedResults, query);
  }

  /**
   * Regression test for https://github.com/druid-io/druid/issues/5132
   */
  @Test
  public void testTopNWithNonBitmapFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(new BoundDimFilter(
            Column.TIME_COLUMN_NAME,
            "0",
            String.valueOf(Long.MAX_VALUE),
            true,
            true,
            false,
            null,
            StringComparators.NUMERIC
        ))
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric("count")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(
            Collections.singletonList(new DoubleSumAggregatorFactory("count", "qualityDouble"))
        )
        .build();

    // Don't check results, just the fact that the query could complete
    Assert.assertNotNull(Sequences.toList(runWithMerge(query), new ArrayList<>()));
  }
}
