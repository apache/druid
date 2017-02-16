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

package io.druid.query.aggregation.avg;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.data.input.Row;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.segment.TestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class AvgGroupByQueryTest
{
  private final GroupByQueryConfig config;
  private final QueryRunner<Row> runner;
  private final GroupByQueryRunnerFactory factory;
  private final String testName;

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder() throws IOException
  {
    return GroupByQueryRunnerTest.constructorFeeder();
  }

  public AvgGroupByQueryTest(
      String testName,
      GroupByQueryConfig config,
      GroupByQueryRunnerFactory factory,
      QueryRunner runner
  )
  {
    this.testName = testName;
    this.config = config;
    this.factory = factory;
    this.runner = factory.mergeRunners(MoreExecutors.sameThreadExecutor(), ImmutableList.<QueryRunner<Row>>of(runner));
  }

  @Test
  public void testGroupByAvgOnly()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(AvgTestHelper.indexAvgAggr))
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    AvgTestHelper.RowBuilder builder =
        new AvgTestHelper.RowBuilder(new String[]{"alias", "index_var"});

    List<Row> expectedResults = builder
        .add("2011-04-01", "automotive", 135.885094d)
        .add("2011-04-01", "business", 118.570340d)
        .add("2011-04-01", "entertainment", 158.747224d)
        .add("2011-04-01", "health", 120.134704d)
        .add("2011-04-01", "mezzanine", 957.295563d)
        .add("2011-04-01", "news", 121.583581d)
        .add("2011-04-01", "premium", 966.932882d)
        .add("2011-04-01", "technology", 78.622547d)
        .add("2011-04-01", "travel", 119.922742d)

        .add("2011-04-02", "automotive", 147.425935d)
        .add("2011-04-02", "business", 112.987027d)
        .add("2011-04-02", "entertainment", 166.016049d)
        .add("2011-04-02", "health", 113.446008d)
        .add("2011-04-02", "mezzanine", 816.276871d)
        .add("2011-04-02", "news", 114.290141d)
        .add("2011-04-02", "premium", 835.471716d)
        .add("2011-04-02", "technology", 97.387433d)
        .add("2011-04-02", "travel", 126.411364d)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }

  @Test
  public void testGroupBy()
  {
    GroupByQuery query = GroupByQuery
        .builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setQuerySegmentSpec(QueryRunnerTestHelper.firstToThird)
        .setDimensions(Lists.<DimensionSpec>newArrayList(new DefaultDimensionSpec("quality", "alias")))
        .setAggregatorSpecs(
            Arrays.asList(
                AvgTestHelper.rowsCount,
                AvgTestHelper.indexAvgAggr,
                new LongSumAggregatorFactory("idx", "index")
            )
        )
        .setGranularity(QueryRunnerTestHelper.dayGran)
        .build();

    AvgTestHelper.RowBuilder builder =
        new AvgTestHelper.RowBuilder(new String[]{"alias", "rows", "idx", "index_var"});

    List<Row> expectedResults = builder
        .add("2011-04-01", "automotive", 1L, 135L, 135.885094d)
        .add("2011-04-01", "business", 1L, 118L, 118.570340d)
        .add("2011-04-01", "entertainment", 1L, 158L, 158.747224d)
        .add("2011-04-01", "health", 1L, 120L, 120.134704d)
        .add("2011-04-01", "mezzanine", 3L, 2870L, 957.295563d)
        .add("2011-04-01", "news", 1L, 121L, 121.583581d)
        .add("2011-04-01", "premium", 3L, 2900L, 966.932882d)
        .add("2011-04-01", "technology", 1L, 78L, 78.622547d)
        .add("2011-04-01", "travel", 1L, 119L, 119.922742d)

        .add("2011-04-02", "automotive", 1L, 147L, 147.425935d)
        .add("2011-04-02", "business", 1L, 112L, 112.987027d)
        .add("2011-04-02", "entertainment", 1L, 166L, 166.016049d)
        .add("2011-04-02", "health", 1L, 113L, 113.446008d)
        .add("2011-04-02", "mezzanine", 3L, 2447L, 816.276871d)
        .add("2011-04-02", "news", 1L, 114L, 114.290141d)
        .add("2011-04-02", "premium", 3L, 2505L, 835.471716d)
        .add("2011-04-02", "technology", 1L, 97L, 97.387433d)
        .add("2011-04-02", "travel", 1L, 126L, 126.411364d)
        .build();

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "");
  }
}
