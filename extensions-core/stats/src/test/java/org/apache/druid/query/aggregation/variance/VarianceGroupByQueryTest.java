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

package org.apache.druid.query.aggregation.variance;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.teststats.PvaluefromZscorePostAggregator;
import org.apache.druid.query.aggregation.teststats.ZtestPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.having.GreaterThanHavingSpec;
import org.apache.druid.query.groupby.having.OrHavingSpec;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Period;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
@RunWith(Parameterized.class)
public class VarianceGroupByQueryTest extends InitializedNullHandlingTest
{
  private final GroupByQueryConfig config;
  private final QueryRunner<Row> runner;
  private final GroupByQueryRunnerFactory factory;
  private final String testName;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<Object[]> constructorFeeder()
  {
    // Use GroupByQueryRunnerTest's constructorFeeder, but remove vectorized tests, since this aggregator
    // can't vectorize yet.
    return GroupByQueryRunnerTest.constructorFeeder().stream()
                                 .filter(constructor -> !((boolean) constructor[4]) /* !vectorize */)
                                 .map(
                                     constructor ->
                                         new Object[]{
                                             constructor[0],
                                             constructor[1],
                                             constructor[2],
                                             constructor[3]
                                         }
                                 )
                                 .collect(Collectors.toList());
  }

  public VarianceGroupByQueryTest(
      String testName,
      GroupByQueryConfig config,
      GroupByQueryRunnerFactory factory,
      QueryRunner runner
  )
  {
    this.testName = testName;
    this.config = config;
    this.factory = factory;
    this.runner = factory.mergeRunners(Execs.directExecutor(), ImmutableList.of(runner));
  }

  @Test
  public void testGroupByVarianceOnly()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(VarianceTestHelper.INDEX_VARIANCE_AGGR)
        .setPostAggregatorSpecs(Collections.singletonList(VarianceTestHelper.STD_DEV_OF_INDEX_POST_AGGR))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    VarianceTestHelper.RowBuilder builder =
        new VarianceTestHelper.RowBuilder(new String[]{"alias", "index_stddev", "index_var"});

    List<ResultRow> expectedResults = builder
        .add("2011-04-01", "automotive", 0d, 0d)
        .add("2011-04-01", "business", 0d, 0d)
        .add("2011-04-01", "entertainment", 0d, 0d)
        .add("2011-04-01", "health", 0d, 0d)
        .add("2011-04-01", "mezzanine", 737.0179286322613d, 543195.4271253889d)
        .add("2011-04-01", "news", 0d, 0d)
        .add("2011-04-01", "premium", 726.6322593583996d, 527994.4403402924d)
        .add("2011-04-01", "technology", 0d, 0d)
        .add("2011-04-01", "travel", 0d, 0d)

        .add("2011-04-02", "automotive", 0d, 0d)
        .add("2011-04-02", "business", 0d, 0d)
        .add("2011-04-02", "entertainment", 0d, 0d)
        .add("2011-04-02", "health", 0d, 0d)
        .add("2011-04-02", "mezzanine", 611.3420766546617d, 373739.13468843425d)
        .add("2011-04-02", "news", 0d, 0d)
        .add("2011-04-02", "premium", 621.3898134843073d, 386125.30030206224d)
        .add("2011-04-02", "technology", 0d, 0d)
        .add("2011-04-02", "travel", 0d, 0d)
        .build(query);

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "variance");
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            VarianceTestHelper.INDEX_VARIANCE_AGGR,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setPostAggregatorSpecs(Collections.singletonList(VarianceTestHelper.STD_DEV_OF_INDEX_POST_AGGR))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    VarianceTestHelper.RowBuilder builder =
        new VarianceTestHelper.RowBuilder(new String[]{"alias", "rows", "idx", "index_stddev", "index_var"});

    List<ResultRow> expectedResults = builder
        .add("2011-04-01", "automotive", 1L, 135L, 0d, 0d)
        .add("2011-04-01", "business", 1L, 118L, 0d, 0d)
        .add("2011-04-01", "entertainment", 1L, 158L, 0d, 0d)
        .add("2011-04-01", "health", 1L, 120L, 0d, 0d)
        .add("2011-04-01", "mezzanine", 3L, 2870L, 737.0179286322613d, 543195.4271253889d)
        .add("2011-04-01", "news", 1L, 121L, 0d, 0d)
        .add("2011-04-01", "premium", 3L, 2900L, 726.6322593583996d, 527994.4403402924d)
        .add("2011-04-01", "technology", 1L, 78L, 0d, 0d)
        .add("2011-04-01", "travel", 1L, 119L, 0d, 0d)

        .add("2011-04-02", "automotive", 1L, 147L, 0d, 0d)
        .add("2011-04-02", "business", 1L, 112L, 0d, 0d)
        .add("2011-04-02", "entertainment", 1L, 166L, 0d, 0d)
        .add("2011-04-02", "health", 1L, 113L, 0d, 0d)
        .add("2011-04-02", "mezzanine", 3L, 2447L, 611.3420766546617d, 373739.13468843425d)
        .add("2011-04-02", "news", 1L, 114L, 0d, 0d)
        .add("2011-04-02", "premium", 3L, 2505L, 621.3898134843073d, 386125.30030206224d)
        .add("2011-04-02", "technology", 1L, 97L, 0d, 0d)
        .add("2011-04-02", "travel", 1L, 126L, 0d, 0d)
        .build(query);

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testPostAggHavingSpec()
  {
    VarianceTestHelper.RowBuilder expect = new VarianceTestHelper.RowBuilder(
        new String[]{"alias", "rows", "index", "index_var", "index_stddev"}
    );

    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setInterval("2011-04-02/2011-04-04")
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            QueryRunnerTestHelper.INDEX_LONG_SUM,
            VarianceTestHelper.INDEX_VARIANCE_AGGR
        )
        .setPostAggregatorSpecs(ImmutableList.of(VarianceTestHelper.STD_DEV_OF_INDEX_POST_AGGR))
        .setGranularity(new PeriodGranularity(new Period("P1M"), null, null))
        .setHavingSpec(
            new OrHavingSpec(
                ImmutableList.of(
                    new GreaterThanHavingSpec(VarianceTestHelper.STD_DEV_OF_INDEX_METRIC, 15L) // 3 rows
                )
            )
        )
        .build();

    List<ResultRow> expectedResults = expect
        .add("2011-04-01", "automotive", 2L, 269L, 299.0009819048282, 17.29164485827847)
        .add("2011-04-01", "mezzanine", 6L, 4420L, 254083.76447001836, 504.06722217380724)
        .add("2011-04-01", "premium", 6L, 4416L, 252279.2020389339, 502.27403082275106)
        .build(query);

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "havingSpec");

    query = query.withLimitSpec(
        new DefaultLimitSpec(
            Collections.singletonList(
                OrderByColumnSpec.asc(
                    VarianceTestHelper.STD_DEV_OF_INDEX_METRIC
                )
            ), 2
        )
    );

    expectedResults = expect
        .add("2011-04-01", "automotive", 2L, 269L, 299.0009819048282, 17.29164485827847)
        .add("2011-04-01", "premium", 6L, 4416L, 252279.2020389339, 502.27403082275106)
        .build(query);

    results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "limitSpec");
  }

  @Test
  public void testGroupByZtestPostAgg()
  {
    // test postaggs from 'teststats' package in here since we've already gone to the trouble of setting up the test
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            VarianceTestHelper.INDEX_VARIANCE_AGGR,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setPostAggregatorSpecs(
            ImmutableList.of(
                VarianceTestHelper.STD_DEV_OF_INDEX_POST_AGGR,
                // these inputs are totally nonsensical, i just want the code path to be executed
                new ZtestPostAggregator(
                    "ztest",
                    new FieldAccessPostAggregator("f1", "idx"),
                    new ConstantPostAggregator("f2", 100000L),
                    new FieldAccessPostAggregator("f3", "index_stddev"),
                    new ConstantPostAggregator("f2", 100000L)
                )
            )
        )
        .setLimitSpec(new DefaultLimitSpec(OrderByColumnSpec.descending("ztest"), 1))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    VarianceTestHelper.RowBuilder builder =
        new VarianceTestHelper.RowBuilder(new String[]{"alias", "rows", "idx", "index_stddev", "index_var", "ztest"});

    List<ResultRow> expectedResults = builder
        .add("2011-04-01", "premium", 3L, 2900.0, 726.632270328514, 527994.4562827706, 36.54266309285626)
        .build(query);

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }

  @Test
  public void testGroupByTestPvalueZscorePostAgg()
  {
    // test postaggs from 'teststats' package in here since we've already gone to the trouble of setting up the test
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setQuerySegmentSpec(QueryRunnerTestHelper.FIRST_TO_THIRD)
        .setDimensions(new DefaultDimensionSpec("quality", "alias"))
        .setAggregatorSpecs(
            QueryRunnerTestHelper.ROWS_COUNT,
            VarianceTestHelper.INDEX_VARIANCE_AGGR,
            new LongSumAggregatorFactory("idx", "index")
        )
        .setPostAggregatorSpecs(
            ImmutableList.of(
                VarianceTestHelper.STD_DEV_OF_INDEX_POST_AGGR,
                // nonsensical inputs
                new PvaluefromZscorePostAggregator("pvalueZscore", new FieldAccessPostAggregator("f1", "index_stddev"))
            )
        )
        .setLimitSpec(new DefaultLimitSpec(OrderByColumnSpec.descending("pvalueZscore"), 1))
        .setGranularity(QueryRunnerTestHelper.DAY_GRAN)
        .build();

    VarianceTestHelper.RowBuilder builder =
        new VarianceTestHelper.RowBuilder(new String[]{"alias", "rows", "idx", "index_stddev", "index_var", "pvalueZscore"});

    List<ResultRow> expectedResults = builder
        .add("2011-04-01", "automotive", 1L, 135.0, 0.0, 0.0, 1.0)
        .build(query);

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "groupBy");
  }
}
