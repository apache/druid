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

package org.apache.druid.query.aggregation.histogram;

import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.GroupByQueryRunnerTestHelper;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 */
@RunWith(Parameterized.class)
public class FixedBucketsHistogramGroupByQueryTest extends InitializedNullHandlingTest
{
  private static final Closer RESOURCE_CLOSER = Closer.create();

  private final QueryRunner<Row> runner;
  private final GroupByQueryRunnerFactory factory;

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
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

    final List<Object[]> constructors = new ArrayList<>();
    final List<GroupByQueryConfig> configs = ImmutableList.of(
        v1Config,
        v1SingleThreadedConfig,
        v2Config
    );

    for (GroupByQueryConfig config : configs) {
      final Pair<GroupByQueryRunnerFactory, Closer> factoryAndCloser = GroupByQueryRunnerTest.makeQueryRunnerFactory(
          config
      );
      final GroupByQueryRunnerFactory factory = factoryAndCloser.lhs;
      RESOURCE_CLOSER.register(factoryAndCloser.rhs);
      for (QueryRunner<ResultRow> runner : QueryRunnerTestHelper.makeQueryRunners(factory)) {
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

  public FixedBucketsHistogramGroupByQueryTest(
      String testName,
      GroupByQueryRunnerFactory factory,
      QueryRunner runner
  )
  {
    this.factory = factory;
    this.runner = runner;

    ApproximateHistogramDruidModule.registerSerde();
  }

  @After
  public void teardown() throws IOException
  {
    RESOURCE_CLOSER.close();
  }

  @Test
  public void testGroupByWithFixedHistogramAgg()
  {
    FixedBucketsHistogramAggregatorFactory aggFactory = new FixedBucketsHistogramAggregatorFactory(
        "histo",
        "index",
        10,
        0,
        2000,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        false
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("marketalias", OrderByColumnSpec.Direction.DESCENDING)),
                1
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, aggFactory)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new QuantilePostAggregator("quantile", "histo", 0.5f)
            )
        )
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias", "upfront",
            "rows", 186L,
            "quantile", 969.6969604492188f,
            "histo",
            new FixedBucketsHistogram(
                0,
                2000,
                10,
                FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
                new long[]{0, 0, 4, 33, 66, 35, 25, 11, 10, 2},
                186,
                1870.061029,
                545.990623,
                0,
                0,
                0
            ).toString()
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "fixed-histo");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testGroupByWithSameNameComplexPostAgg()
  {
    FixedBucketsHistogramAggregatorFactory aggFactory = new FixedBucketsHistogramAggregatorFactory(
        "histo",
        "index",
        10,
        0,
        2000,
        FixedBucketsHistogram.OutlierHandlingMode.OVERFLOW,
        false
    );

    GroupByQuery query = new GroupByQuery.Builder()
        .setDataSource(QueryRunnerTestHelper.DATA_SOURCE)
        .setGranularity(QueryRunnerTestHelper.ALL_GRAN).setDimensions(new DefaultDimensionSpec(
            QueryRunnerTestHelper.MARKET_DIMENSION,
            "marketalias"
        ))
        .setInterval(QueryRunnerTestHelper.FULL_ON_INTERVAL)
        .setLimitSpec(
            new DefaultLimitSpec(
                Collections.singletonList(new OrderByColumnSpec("marketalias", OrderByColumnSpec.Direction.DESCENDING)),
                1
            )
        ).setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT, aggFactory)
        .setPostAggregatorSpecs(
            Collections.singletonList(
                new QuantilePostAggregator("quantile", "quantile", 0.5f)
            )
        )
        .build();

    List<ResultRow> expectedResults = Collections.singletonList(
        GroupByQueryRunnerTestHelper.createExpectedRow(
            query,
            "1970-01-01T00:00:00.000Z",
            "marketalias", "upfront",
            "rows", 186L,
            "quantile", 969.6969604492188f
        )
    );

    Iterable<ResultRow> results = GroupByQueryRunnerTestHelper.runQuery(factory, runner, query);
    TestHelper.assertExpectedObjects(expectedResults, results, "fixed-histo");
  }
}
