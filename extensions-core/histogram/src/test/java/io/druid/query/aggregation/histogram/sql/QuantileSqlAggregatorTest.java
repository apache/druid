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

package io.druid.query.aggregation.histogram.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import io.druid.query.aggregation.histogram.QuantilePostAggregator;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.sql.calcite.aggregation.SqlAggregator;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Planner;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

import static io.druid.sql.calcite.CalciteQueryTest.TIMESERIES_CONTEXT;

public class QuantileSqlAggregatorTest
{
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker;
  private PlannerFactory plannerFactory;

  @Before
  public void setUp() throws Exception
  {
    Calcites.setSystemProperties();
    walker = CalciteTests.createMockWalker(temporaryFolder.newFolder());
    final PlannerConfig plannerConfig = new PlannerConfig();
    final SchemaPlus rootSchema = Calcites.createRootSchema(
        CalciteTests.createMockSchema(
            walker,
            plannerConfig
        )
    );
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.<SqlAggregator>of(
            new QuantileSqlAggregator()
        )
    );
    plannerFactory = new PlannerFactory(rootSchema, operatorTable, plannerConfig);
  }

  @After
  public void tearDown() throws Exception
  {
    walker.close();
    walker = null;
  }

  @Test
  public void testQuantileOnFloatAndLongs() throws Exception
  {
    try (final Planner planner = plannerFactory.createPlanner()) {
      final String sql = "SELECT QUANTILE(m1, 0.01), QUANTILE(m1, 0.5), QUANTILE(m1, 0.99), QUANTILE(cnt, 0.5) FROM foo";
      final PlannerResult plannerResult = Calcites.plan(planner, sql);

      // Verify results
      final List<Object[]> results = Sequences.toList(plannerResult.run(), new ArrayList<Object[]>());
      final List<Object[]> expectedResults = ImmutableList.of(
          new Object[]{1.0, 3.0, 5.940000057220459, 1.0}
      );
      Assert.assertEquals(expectedResults.size(), results.size());
      for (int i = 0; i < expectedResults.size(); i++) {
        Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
      }

      // Verify query
      Assert.assertEquals(
          Druids.newTimeseriesQueryBuilder()
                .dataSource(CalciteTests.DATASOURCE1)
                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                .granularity(QueryGranularities.ALL)
                .aggregators(ImmutableList.<AggregatorFactory>of(
                    new ApproximateHistogramAggregatorFactory("a0:agg", "m1", null, null, null, null),
                    new ApproximateHistogramAggregatorFactory("a3:agg", "cnt", null, null, null, null)
                ))
                .postAggregators(ImmutableList.<PostAggregator>of(
                    new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                    new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                    new QuantilePostAggregator("a2", "a0:agg", 0.99f),
                    new QuantilePostAggregator("a3", "a3:agg", 0.50f)
                ))
                .context(TIMESERIES_CONTEXT)
                .build(),
          Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
      );
    }
  }
}
