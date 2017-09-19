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

package io.druid.query.aggregation.histogram;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.data.input.Row;
import io.druid.java.util.common.StringUtils;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.GroupByQueryRunnerTest;
import io.druid.query.groupby.GroupByQueryRunnerTestHelper;
import io.druid.query.groupby.orderby.DefaultLimitSpec;
import io.druid.query.groupby.orderby.OrderByColumnSpec;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.segment.TestHelper;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class ApproximateHistogramGroupByQueryTest
{
  private final QueryRunner<Row> runner;
  private GroupByQueryRunnerFactory factory;
  private String testName;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    final GroupByQueryConfig v1Config = new GroupByQueryConfig()
    {
      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V1;
      }

      @Override
      public String toString()
      {
        return "v1";
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

    v1Config.setMaxIntermediateRows(10000);
    v1SingleThreadedConfig.setMaxIntermediateRows(10000);

    final List<Object[]> constructors = Lists.newArrayList();
    final List<GroupByQueryConfig> configs = ImmutableList.of(
        v1Config,
        v1SingleThreadedConfig,
        v2Config
    );

    for (GroupByQueryConfig config : configs) {
      final GroupByQueryRunnerFactory factory = GroupByQueryRunnerTest.makeQueryRunnerFactory(config);
      for (QueryRunner<Row> runner : QueryRunnerTestHelper.makeQueryRunners(factory)) {
        final String testName = StringUtils.format(
            "config=%s, runner=%s",
            config.toString(),
            runner.toString()
        );
        constructors.add(new Object[]{testName, factory, runner});
      }
    }

    return constructors;
  }

  public ApproximateHistogramGroupByQueryTest(String testName, GroupByQueryRunnerFactory factory, QueryRunner runner)
  {
    this.testName = testName;
    this.factory = factory;
    this.runner = runner;

    //Note: this is needed in order to properly register the serde for Histogram.
    new ApproximateHistogramDruidModule().configure(null);
  }

  @Test
  public void testGroupByWithApproximateHistogramAgg()
  {
    ApproximateHistogramAggregatorFactory aggFactory = new ApproximateHistogramAggregatorFactory(
        "apphisto",
        "index",
        10,
        5,
        Float.NEGATIVE_INFINITY,
        Float.POSITIVE_INFINITY
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    "marketalias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "marketalias",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 1
            )
        )
        .setAggregatorSpecs(
            Lists.newArrayList(
                QueryRunnerTestHelper.rowsCount,
                aggFactory
            )
        )
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new QuantilePostAggregator("quantile", "apphisto", 0.5f)
            )
        )
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias", "upfront",
            "rows", 186L,
            "quantile", 880.9881f,
            "apphisto",
            new Histogram(
                new float[]{
                    214.97299194335938f,
                    545.9906005859375f,
                    877.0081787109375f,
                    1208.0257568359375f,
                    1539.0433349609375f,
                    1870.06103515625f
                },
                new double[]{
                    0.0, 67.53287506103516, 72.22068786621094, 31.984678268432617, 14.261756896972656
                }
            )
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "approx-histo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithSameNameComplexPostAgg()
  {
    ApproximateHistogramAggregatorFactory aggFactory = new ApproximateHistogramAggregatorFactory(
        "quantile",
        "index",
        10,
        5,
        Float.NEGATIVE_INFINITY,
        Float.POSITIVE_INFINITY
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.dataSource)
        .setGranularity(QueryRunnerTestHelper.allGran)
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec(
                    QueryRunnerTestHelper.marketDimension,
                    "marketalias"
                )
            )
        )
        .setInterval(QueryRunnerTestHelper.fullOnInterval)
        .setLimitSpec(
            new DefaultLimitSpec(
                Lists.newArrayList(
                    new OrderByColumnSpec(
                        "marketalias",
                        OrderByColumnSpec.Direction.DESCENDING
                    )
                ), 1
            )
        )
        .setAggregatorSpecs(
            Lists.newArrayList(
                QueryRunnerTestHelper.rowsCount,
                aggFactory
            )
        )
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new QuantilePostAggregator("quantile", "quantile", 0.5f)
            )
        )
        .build();

    List<Row> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            "1970-01-01T00:00:00.000Z",
            "marketalias", "upfront",
            "rows", 186L,
            "quantile", 880.9881f
        )
    );

    Iterable<Row> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "approx-histo");
  }
}
