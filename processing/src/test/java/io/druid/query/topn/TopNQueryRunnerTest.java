/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.topn;

import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.collections.StupidPool;
import io.druid.query.BySegmentResultValueClass;
import io.druid.query.Druids;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.TestQueryRunners;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.MaxAggregatorFactory;
import io.druid.query.aggregation.MinAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.RegexDimExtractionFn;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.TestHelper;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 */
@RunWith(Parameterized.class)
public class TopNQueryRunnerTest
{
  @Parameterized.Parameters
  public static Collection<?> constructorFeeder() throws IOException
  {
    List<Object> retVal = Lists.newArrayList();
    retVal.addAll(
        QueryRunnerTestHelper.makeQueryRunners(
            new TopNQueryRunnerFactory(
                TestQueryRunners.getPool(),
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
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
                        return ByteBuffer.allocate(2000);
                      }
                    }
                ),
                new TopNQueryQueryToolChest(new TopNQueryConfig()),
                QueryRunnerTestHelper.NOOP_QUERYWATCHER
            )
        )
    );

    return retVal;
  }

  private final QueryRunner runner;

  public TopNQueryRunnerTest(
      QueryRunner runner
  )
  {
    this.runner = runner;
  }

  private static final String marketDimension = "market";

  @Test
  public void testFullOnTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
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
                                .put(marketDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(marketDimension, "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(marketDimension, "spot")
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testFullOnTopNOverPostAggs()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(QueryRunnerTestHelper.addRowsIndexConstantMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
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
                                .put(marketDimension, "total_market")
                                .put("rows", 186L)
                                .put("index", 215679.82879638672D)
                                .put("addRowsIndexConstant", 215866.82879638672D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1743.9217529296875D)
                                .put("minIndex", 792.3260498046875D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(marketDimension, "upfront")
                                .put("rows", 186L)
                                .put("index", 192046.1060180664D)
                                .put("addRowsIndexConstant", 192233.1060180664D)
                                .put("uniques", QueryRunnerTestHelper.UNIQUES_2)
                                .put("maxIndex", 1870.06103515625D)
                                .put("minIndex", 545.9906005859375D)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put(marketDimension, "spot")
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }


  @Test
  public void testFullOnTopNOverUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(QueryRunnerTestHelper.uniqueMetric)
        .threshold(3)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNOverMissingUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
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
                                .put("market", "total_market")
                                .put("uniques", 0)
                                .build(),
                    ImmutableMap.<String, Object>builder()
                                .put("market", "spot")
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }


  @Test
  public void testTopNBySegment()
  {

    final HashMap<String, Object> specialContext = new HashMap<String, Object>();
    specialContext.put("bySegment", "true");
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
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
                        marketDimension, "total_market",
                        "uniques", QueryRunnerTestHelper.UNIQUES_2,
                        "rows", 4L
                    ),
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 4880.669677734375D,
                        "index", 4875.669677734375D,
                        marketDimension, "upfront",
                        "uniques", QueryRunnerTestHelper.UNIQUES_2,
                        "rows", 4L
                    ),
                    ImmutableMap.<String, Object>of(
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "index", 2231.8768157958984D,
                        marketDimension, "spot",
                        "uniques", QueryRunnerTestHelper.UNIQUES_9,
                        "rows", 18L
                    )
                )
            )
        )
    );
    Sequence<Result<TopNResultValue>> results = new TopNQueryQueryToolChest(new TopNQueryConfig()).postMergeQueryDecoration(
        runner
    ).run(
        query,
        specialContext
    );
    List<Result<BySegmentTopNResultValue>> resultList = Sequences.toList(
        Sequences.map(
            results, new Function<Result<TopNResultValue>, Result<BySegmentTopNResultValue>>()
            {

              @Nullable
              @Override
              public Result<BySegmentTopNResultValue> apply(
                  Result<TopNResultValue> input
              )
              {
                return new Result<BySegmentTopNResultValue>(
                    input.getTimestamp(),
                    (BySegmentTopNResultValue) input.getValue()
                );
              }
            }
        ),
        Lists.<Result<BySegmentTopNResultValue>>newArrayList()
    );
    TestHelper.assertExpectedResults(expectedResults, resultList.get(0).getValue().getResults());
  }

  @Test
  public void testTopN()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
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
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNByUniques()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNWithOrFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(marketDimension, "total_market", "upfront", "spot")
        .dimension(marketDimension)
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
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNWithOrFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(marketDimension, "total_market", "upfront")
        .dimension(marketDimension)
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
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNWithFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(marketDimension, "upfront")
        .dimension(marketDimension)
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
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNWithFilter2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
        .dimension(marketDimension)
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
                        marketDimension, "upfront",
                        "rows", 2L,
                        "index", 2591.68359375D,
                        "addRowsIndexConstant", 2594.68359375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "total_market",
                        "rows", 2L,
                        "index", 2508.39599609375D,
                        "addRowsIndexConstant", 2511.39599609375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "spot",
                        "rows", 2L,
                        "index", 220.63774871826172D,
                        "addRowsIndexConstant", 223.63774871826172D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNWithFilter2OneDay()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
        .dimension(marketDimension)
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
                        marketDimension, "upfront",
                        "rows", 1L,
                        "index", new Float(1447.341160).doubleValue(),
                        "addRowsIndexConstant", new Float(1449.341160).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "total_market",
                        "rows", 1L,
                        "index", new Float(1314.839715).doubleValue(),
                        "addRowsIndexConstant", new Float(1316.839715).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "spot",
                        "rows", 1L,
                        "index", new Float(109.705815).doubleValue(),
                        "addRowsIndexConstant", new Float(111.705815).doubleValue(),
                        "uniques", QueryRunnerTestHelper.UNIQUES_1
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNWithNonExistentFilterInOr()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(marketDimension, "total_market", "upfront", "billyblank")
        .dimension(marketDimension)
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
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNWithNonExistentFilter()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(marketDimension, "billyblank")
        .dimension(marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        Lists.<Result<TopNResultValue>>newArrayList(
            new Result<TopNResultValue>(
                new DateTime("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ),
        runner.run(query, context)
    );
  }

  @Test
  public void testTopNWithNonExistentFilterMultiDim()
  {
    AndDimFilter andDimFilter = Druids.newAndDimFilterBuilder()
                                      .fields(
                                          Lists.<DimFilter>newArrayList(
                                              Druids.newSelectorDimFilterBuilder()
                                                    .dimension(marketDimension)
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
        .dimension(marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        Lists.<Result<TopNResultValue>>newArrayList(
            new Result<TopNResultValue>(
                new DateTime("2011-04-01T00:00:00.000Z"),
                new TopNResultValue(Lists.<Map<String, Object>>newArrayList())
            )
        ),
        runner.run(query, context)
    );
  }

  @Test
  public void testTopNWithMultiValueDimFilter1()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .filters(QueryRunnerTestHelper.placementishDimension, "m")
        .dimension(marketDimension)
        .metric(QueryRunnerTestHelper.indexMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.firstToThird)
        .aggregators(QueryRunnerTestHelper.commonAggregators)
        .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
        .build();
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        Sequences.toList(
            runner.run(
                new TopNQueryBuilder()
                    .dataSource(QueryRunnerTestHelper.dataSource)
                    .granularity(QueryRunnerTestHelper.allGran)
                    .filters(QueryRunnerTestHelper.qualityDimension, "mezzanine")
                    .dimension(marketDimension)
                    .metric(QueryRunnerTestHelper.indexMetric)
                    .threshold(4)
                    .intervals(QueryRunnerTestHelper.firstToThird)
                    .aggregators(QueryRunnerTestHelper.commonAggregators)
                    .postAggregators(Arrays.<PostAggregator>asList(QueryRunnerTestHelper.addRowsIndexConstant))
                    .build(),
                context
            ), Lists.<Result<TopNResultValue>>newArrayList()
        ), runner.run(query, context)
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(
        Sequences.toList(
            runner.run(
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
                    .build(),
                context
            ), Lists.<Result<TopNResultValue>>newArrayList()
        )
        , runner.run(query, context)
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
        new Result<TopNResultValue>(
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
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
        new Result<TopNResultValue>(
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
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
        new Result<TopNResultValue>(
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNLexicographic()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(new LexicographicTopNMetricSpec(""))
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
                        marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNLexicographicWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(new LexicographicTopNMetricSpec("spot"))
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
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNLexicographicWithNonExistingPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(new LexicographicTopNMetricSpec("t"))
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
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNInvertedLexicographicWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec("upfront")))
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
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNInvertedLexicographicWithNonExistingPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec("u")))
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
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                marketDimension, marketDimension, new RegexDimExtractionFn("(.)")
            )
        )
        .metric("rows")
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
                        marketDimension, "s",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNLexicographicDimExtraction()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                marketDimension, marketDimension, new RegexDimExtractionFn("(.)")
            )
        )
        .metric(new LexicographicTopNMetricSpec(null))
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
                        marketDimension, "s",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testInvertedTopNLexicographicDimExtraction2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                marketDimension, marketDimension, new RegexDimExtractionFn("..(.)")
            )
        )
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec(null)))
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
                        marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "o",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "f",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNLexicographicDimExtractionWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                marketDimension, marketDimension, new RegexDimExtractionFn("(.)")
            )
        )
        .metric(new LexicographicTopNMetricSpec("spot"))
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
                        marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "u",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }


  @Test
  public void testInvertedTopNLexicographicDimExtractionWithPreviousStop()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                marketDimension, marketDimension, new RegexDimExtractionFn("(.)")
            )
        )
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec("u")))
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
                        marketDimension, "t",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "s",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testInvertedTopNLexicographicDimExtractionWithPreviousStop2()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(
            new ExtractionDimensionSpec(
                marketDimension, marketDimension, new RegexDimExtractionFn("..(.)")
            )
        )
        .metric(new InvertedTopNMetricSpec(new LexicographicTopNMetricSpec("p")))
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
                        marketDimension, "o",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "f",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testInvertedTopNQuery()
  {
    TopNQuery query =
        new TopNQueryBuilder()
            .dataSource(QueryRunnerTestHelper.dataSource)
            .granularity(QueryRunnerTestHelper.allGran)
            .dimension(marketDimension)
            .metric(new InvertedTopNMetricSpec(new NumericTopNMetricSpec(QueryRunnerTestHelper.indexMetric)))
            .threshold(3)
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
                        marketDimension, "spot",
                        "rows", 18L,
                        "index", 2231.8768157958984D,
                        "addRowsIndexConstant", 2250.8768157958984D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_9
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "upfront",
                        "rows", 4L,
                        "index", 4875.669677734375D,
                        "addRowsIndexConstant", 4880.669677734375D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    ),
                    ImmutableMap.<String, Object>of(
                        marketDimension, "total_market",
                        "rows", 4L,
                        "index", 5351.814697265625D,
                        "addRowsIndexConstant", 5356.814697265625D,
                        "uniques", QueryRunnerTestHelper.UNIQUES_2
                    )
                )
            )
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }

  @Test
  public void testTopNDependentPostAgg()
  {
    TopNQuery query = new TopNQueryBuilder()
        .dataSource(QueryRunnerTestHelper.dataSource)
        .granularity(QueryRunnerTestHelper.allGran)
        .dimension(marketDimension)
        .metric(QueryRunnerTestHelper.dependentPostAggMetric)
        .threshold(4)
        .intervals(QueryRunnerTestHelper.fullOnInterval)
        .aggregators(
            Lists.<AggregatorFactory>newArrayList(
                Iterables.concat(
                    QueryRunnerTestHelper.commonAggregators,
                    Lists.newArrayList(
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
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
                                .put(marketDimension, "total_market")
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
                                .put(marketDimension, "upfront")
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
                                .put(marketDimension, "spot")
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
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
                        new MaxAggregatorFactory("maxIndex", "index"),
                        new MinAggregatorFactory("minIndex", "index")
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

    List<Result<BySegmentResultValueClass>> expectedResults = Arrays.asList(
        new Result<BySegmentResultValueClass>(
            new DateTime("2011-01-12T00:00:00.000Z"),
            new BySegmentResultValueClass(
                Arrays.asList(
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    TestHelper.assertExpectedResults(expectedResults, runner.run(query, context));
  }
}
