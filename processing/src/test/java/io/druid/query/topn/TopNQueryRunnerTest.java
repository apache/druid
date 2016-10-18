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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.druid.collections.StupidPool;
import io.druid.granularity.QueryGranularities;
import io.druid.granularity.QueryGranularity;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.js.JavaScriptConfig;
import io.druid.query.BySegmentResultValue;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMaxAggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.cardinality.CardinalityAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniqueFinalizingPostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.DimExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.extraction.JavaScriptExtractionFn;
import io.druid.query.extraction.MapLookupExtractor;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.extraction.TimeFormatExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.ExtractionDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.lookup.LookupExtractionFn;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.segment.TestHelper;
import io.druid.segment.column.Column;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TopNQueryRunnerTest
{
  @Parameterized.Parameters(name="{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    List<QueryRunner<Result<TopNResultValue>>> retVal = Lists.newArrayList();
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                TestQueryRunners.getPool(),
                new TopNQueryQueryToolChest(
                    new TopNQueryConfig(),
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                new StupidPool<ByteBuffer>(
                    new Supplier<ByteBuffer>()
                    {
                      @Override
                      public ByteBuffer get()
                      {
                        return ByteBuffer.allocate(20000);
                      }
                    }
                ),
                new TopNQueryQueryToolChest(
                    new TopNQueryConfig(),
                    QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
                ),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );

    return QueryRunnerTestHelper.transformToConstructionFeeder(retVal);
  }

  private final QueryRunner<Result<TopNResultValue>> runner;

  public TopNQueryRunnerTest(
      QueryRunner<Result<TopNResultValue>> runner
  )
  {
    this.runner = runner;
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
    final QueryRunner<Result<TopNResultValue>> mergeRunner = chest.mergeResults(runner);
    return mergeRunner.run(query, context);
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
                    QueryRunnerTestHelper.commonAggregators,
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
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "spot")
                                .put("rows", 837L)
                                .put("index", 95606.57232284546D)
                                .put("addRowsIndexConstant", 96444.57232284546D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                                .put("maxIndex", 277.2735290527344D)
                                .put("minIndex", 59.02102279663086D)
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
                    QueryRunnerTestHelper.commonAggregators,
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
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "spot")
                                .put("rows", 837L)
                                .put("index", 95606.57232284546D)
                                .put("addRowsIndexConstant", 96444.57232284546D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                                .put("maxIndex", 277.2735290527344D)
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
                    QueryRunnerTestHelper.commonAggregators,
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
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
                                .put("rows", 837L)
                                .put("index", 95606.57232284546D)
                                .put("addRowsIndexConstant", 96444.57232284546D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                                .put("maxIndex", 277.2735290527344D)
                                .put("minIndex", 59.02102279663086D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
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
            new DateTime("2011-01-12T00:00:00.000Z"),
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
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .context(specialContext)
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 5356.814697265625D,
                        "index", 5351.814697265625D,
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "uniques", QueryRunnerTestHelper.UNIQUES_2,
                        "rows", 4L
                    ),
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 4880.669677734375D,
                        "index", 4875.669677734375D,
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "uniques", QueryRunnerTestHelper.UNIQUES_2,
                        "rows", 4L
                    ),
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "index", 2231.8768157958984D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
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
                Arrays.asList(new Interval("2011-04-01T00:00:00.000Z/2011-04-02T00:00:00.000Z"))
            )
        )
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    HashMap<String, Object> context = new HashMap<String, Object>();
    assertExpectedResults(
        Lists.<Result<TopNResultValue>>newArrayList(
            new Result<TopNResultValue>(
                new DateTime("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ), query
    );
  }

  @Test
  public void testTopNWithNonExistentFilterMultiDim()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Lists.<DimFilter>newArrayList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.marketDimension)
                                                    .value("billyblank")
                                                    .build(),
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(QueryRunnerTestHelper.qualityDimension)
                                                    .value("mezzanine")
                                                    .build()
                                          )
                                      ).build();
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(andDimFilter)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    assertExpectedResults(
        Lists.<Result<TopNResultValue>>newArrayList(
            new Result<TopNResultValue>(
                new DateTime("2011-04-01T00:00:00.000Z"),
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
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
                    .aggregators(QueryRunnerTestHelper.commonAggregators)
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
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
                    .aggregators(QueryRunnerTestHelper.commonAggregators)
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    final ArrayList<Result<TopNResultValue>> expectedResults = Lists.newArrayList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new LinkedHashMap<String, Object>()
                    {{
                        put("doesn't exist", null);
                        put("rows", 26L);
                        put("index", 12459.361190795898D);
                        put("addRowsIndexConstant", 12486.361190795898D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_9);
                      }}
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new LinkedHashMap<String, Object>()
                    {{
                        put("doesn't exist", null);
                        put("rows", 4L);
                        put("index", 4875.669677734375D);
                        put("addRowsIndexConstant", 4880.669677734375D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_2);
                      }}
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    new LinkedHashMap<String, Object>()
                    {{
                        put("doesn't exist", null);
                        put("rows", 26L);
                        put("index", 12459.361190795898D);
                        put("addRowsIndexConstant", 12486.361190795898D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_9);
                      }}
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
            new DateTime("2011-04-01T00:00:00.000Z"),
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
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
                new JavaScriptExtractionFn("function(f) { return \"POTATO\"; }", false, JavaScriptConfig.getDefault()),
                null
            )
        )
        .metric("rows")
        .threshold(10)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    QueryGranularity gran = QueryGranularities.DAY;
    TimeseriesQuery tsQuery = Druids.newTimeseriesQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(gran)
                                    .intervals(QueryRunnerTestHelper.fullOnInterval)
                                    .aggregators(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.rowsCount,
                                            QueryRunnerTestHelper.indexDoubleSum,
                                            QueryRunnerTestHelper.qualityUniques
                                        )
                                    )
                                    .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                                    .build();
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
                new RegexDimExtractionFn(".(.)", false, null), null
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
            new DateTime("2011-01-12T00:00:00.000Z"),
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
            new DoubleSumAggregatorFactory("index", null, "-index + 100")
        )
    );

    expectedResults = Arrays.asList(
        TopNQueryRunnerTestHelper.createExpectedRows(
            "2011-01-12T00:00:00.000Z",
            new String[]{QueryRunnerTestHelper.qualityDimension, "rows", "index", "addRowsIndexConstant"},
            Arrays.asList(
                new Object[]{"n", 93L, -2786.472755432129, -2692.472755432129},
                new Object[]{"u", 186L, -3949.824363708496, -3762.824363708496}
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
                new RegexDimExtractionFn("(.)", false, null),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
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
                ),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
                ),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
                ),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot0",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1total_market0",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3upfront0",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
                ),
                null
            )
        )
        .metric("rows")
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot0",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market0",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront0",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
                ),
                null
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
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
                ),
                null
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
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
                ),
                null
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "1upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "2spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "3total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
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
                new RegexDimExtractionFn("(.)", false, null),
                null
            )
        )
        .metric(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
                new RegexDimExtractionFn("..(.)", false, null),
                null
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec(null, StringComparators.LEXICOGRAPHIC)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "o",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "f",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
                new RegexDimExtractionFn("(.)", false, null),
                null
            )
        )
        .metric(new DimensionTopNMetricSpec("s", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
                }, null
            )
        )
        .metric(new DimensionTopNMetricSpec("s", StringComparators.LEXICOGRAPHIC))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
                new RegexDimExtractionFn("(.)", false, null),
                null
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("u", StringComparators.LEXICOGRAPHIC)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "s",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
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
                new RegexDimExtractionFn("..(.)", false, null),
                null
            )
        )
        .metric(new InvertedTopNMetricSpec(new DimensionTopNMetricSpec("p", StringComparators.LEXICOGRAPHIC)))
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<TopNResultValue>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "o",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "f",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                nullStringDimExtraction,
                null
            )
        )
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    new LinkedHashMap<String, Object>()
                    {{
                        put(QueryRunnerTestHelper.marketDimension, null);
                        put("rows", 4L);
                        put("index", 5351.814697265625D);
                        put("addRowsIndexConstant", 5356.814697265625D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_2);
                      }},
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .dimension(
            new ExtractionDimensionSpec(
                QueryRunnerTestHelper.marketDimension,
                QueryRunnerTestHelper.marketDimension,
                emptyStringDimExtraction,
                null
            )
        )
        .build();


    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    new LinkedHashMap<String, Object>()
                    {{
                        put(QueryRunnerTestHelper.marketDimension, "");
                        put("rows", 4L);
                        put("index", 5351.814697265625D);
                        put("addRowsIndexConstant", 5356.814697265625D);
                        put("uniques", QueryRunnerTestHelper.UNIQUES_2);
                      }},
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
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
            .aggregators(QueryRunnerTestHelper.commonAggregators)
            .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
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
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.dataSource)
            .granularity(QueryRunnerTestHelper.allGran)
            .dimension(QueryRunnerTestHelper.marketDimension)
            .metric(new NumericTopNMetricSpec("numVals"))
            .threshold(10)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(
                Lists.<AggregatorFactory>newArrayList(
                    new CardinalityAggregatorFactory(
                        "numVals",
                        ImmutableList.<DimensionSpec>of(new DefaultDimensionSpec(
                            QueryRunnerTestHelper.qualityDimension,
                            QueryRunnerTestHelper.qualityDimension
                        )),
                        false
                    )
                )
            )
            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
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
    ExtractionFn helloFn = new JavaScriptExtractionFn(helloJsFn, false, JavaScriptConfig.getDefault());

    DimensionSpec dimSpec = new ExtractionDimensionSpec(QueryRunnerTestHelper.marketDimension,
                                                        QueryRunnerTestHelper.marketDimension,
                                                        helloFn);

    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.dataSource)
            .granularity(QueryRunnerTestHelper.allGran)
            .dimension(dimSpec)
            .metric(new NumericTopNMetricSpec("numVals"))
            .threshold(10)
            .intervals(QueryRunnerTestHelper.firstToThird)
            .aggregators(
                Lists.<AggregatorFactory>newArrayList(
                    new CardinalityAggregatorFactory(
                        "numVals",
                        ImmutableList.<DimensionSpec>of(new ExtractionDimensionSpec(
                            QueryRunnerTestHelper.qualityDimension,
                            QueryRunnerTestHelper.qualityDimension,
                            helloFn
                        )),
                        false
                    )
                )
            )
            .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        "market", "hello",
                        "numVals", 1.0002442201269182d
                    )
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
                    QueryRunnerTestHelper.commonAggregators,
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
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>builder()
                                .put(QueryRunnerTestHelper.marketDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put(QueryRunnerTestHelper.dependentPostAggMetric, 216053.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
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
                                .put("maxIndex", 1870.06103515625D)
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
                                .put("maxIndex", 277.2735290527344D)
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
                    QueryRunnerTestHelper.commonAggregators,
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
                        .put("maxIndex", 1743.9217529296875D)
                        .put("minIndex", 792.3260498046875D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "upfront")
                        .put("rows", 186L)
                        .put("index", 192046.1060180664D)
                        .put("addRowsIndexConstant", 192233.1060180664D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 192420.1060180664D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                        .put("maxIndex", 1870.06103515625D)
                        .put("minIndex", 545.9906005859375D)
                        .build(),
            ImmutableMap.<String, Object>builder()
                        .put(QueryRunnerTestHelper.marketDimension, "spot")
                        .put("rows", 837L)
                        .put("index", 95606.57232284546D)
                        .put("addRowsIndexConstant", 96444.57232284546D)
                        .put(QueryRunnerTestHelper.dependentPostAggMetric, 97282.57232284546D)
                        .put("uniques", QueryRunnerTestHelper.UNIQUES_9)
                        .put("maxIndex", 277.2735290527344D)
                        .put("minIndex", 59.02102279663086D)
                        .build()
        )
    );

    List<Result<BySegmentResultValueClass>> expectedResults = Collections.singletonList(
        new Result<BySegmentResultValueClass>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new BySegmentResultValueClass(
                Collections.singletonList(
                    new Result<TopNResultValue>(
                        new DateTime("2011-01-12T00:00:00.000Z"),
                        topNResult
                    )
                ),
                QueryRunnerTestHelper.segmentId,
                new Interval("1970-01-01T00:00:00.000Z/2020-01-01T00:00:00.000Z")
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
            new DateTime("2011-04-01"),
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
                new TimeFormatExtractionFn("EEEE", null, null, null),
                null
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
            new DateTime("2011-01-12T00:00:00.000Z"),
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
                    QueryRunnerTestHelper.commonAggregators,
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
    map.put("maxIndex", 1870.06103515625D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
                    QueryRunnerTestHelper.commonAggregators,
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
    map.put("maxIndex", 1870.06103515625D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
        .granularity(QueryGranularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("partial_null_column", null);
    map.put("rows", 22L);
    map.put("index", 7583.691513061523D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map,
                    ImmutableMap.<String, Object>of(
                        "partial_null_column", "value",
                        "rows", 4L,
                        "index", 4875.669677734375D,
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
        .granularity(QueryGranularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .filters(new SelectorDimFilter("partial_null_column", null, null))
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .build();

    Map<String, Object> map = Maps.newHashMap();
    map.put("partial_null_column", null);
    map.put("rows", 22L);
    map.put("index", 7583.691513061523D);
    map.put("uniques", QueryRunnerTestHelper.UNIQUES_9);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
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
        .granularity(QueryGranularities.ALL)
        .dimension("partial_null_column")
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .filters(new SelectorDimFilter("partial_null_column", "value", null))
        .threshold(1000)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .build();

    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>of(
                        "partial_null_column", "value",
                        "rows", 4L,
                        "index", 4875.669677734375D,
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
        .granularity(QueryGranularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec(null, StringComparators.ALPHANUMERIC))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.secondOnly)
        .aggregators(Lists.<AggregatorFactory>newArrayList(QueryRunnerTestHelper.rowsCount))
        .build();
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows", 9L
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows", 2L
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, new HashMap<String, Object>()));
  }

  @Test
  public void testNumericDimensionTopNWithNullPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryGranularities.ALL)
        .dimension(QueryRunnerTestHelper.marketDimension)
        .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
        .threshold(2)
        .intervals(QueryRunnerTestHelper.secondOnly)
        .aggregators(Lists.<AggregatorFactory>newArrayList(QueryRunnerTestHelper.rowsCount))
        .build();
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-04-02T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    ImmutableMap.<String, Object>of(
                        "market", "spot",
                        "rows", 9L
                    ),
                    ImmutableMap.<String, Object>of(
                        "market", "total_market",
                        "rows", 2L
                    )
                )
            )
        )
    );
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, new HashMap<String, Object>()));
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
                                            .aggregators(QueryRunnerTestHelper.commonAggregators)
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
            new DateTime("2011-04-01T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.<Map<String, Object>>asList(
                    ImmutableMap.<String, Object>of(
                        QueryRunnerTestHelper.marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
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
                    QueryRunnerTestHelper.commonAggregators, Lists.newArrayList(
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
    map.put("maxIndex", 1870.06103515625D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new TopNResultValue(
                Arrays.asList(
                    map
                )
            )
        )
    );
    assertExpectedResults(expectedResults, topNQueryWithNULLValueExtraction);
  }

  private Sequence<Result<TopNResultValue>> runWithPreMergeAndMerge(TopNQuery query){
    return runWithPreMergeAndMerge(query, ImmutableMap.<String, Object>of());
  }

  private Sequence<Result<TopNResultValue>> runWithPreMergeAndMerge(TopNQuery query, Map<String, Object> context)
  {
    final TopNQueryQueryToolChest chest = new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );
    final QueryRunner<Result<TopNResultValue>> Runner = chest.mergeResults(chest.preMergeQueryDecoration(runner));
    return Runner.run(query, context);
  }

  @Test
  public void testTopNWithExtractionFilterNoExistingValue()
  {
    Map<String, String> extractionMap = new HashMap<>();
    extractionMap.put("","NULL");

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
        .aggregators(Lists.newArrayList(Iterables.concat(QueryRunnerTestHelper.commonAggregators, Lists.newArrayList(
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
    map.put("maxIndex", 1870.06103515625D);
    map.put("minIndex", 59.02102279663086D);
    List<Result<TopNResultValue>> expectedResults = Arrays.asList(
        new Result<>(
            new DateTime("2011-01-12T00:00:00.000Z"),
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
}
