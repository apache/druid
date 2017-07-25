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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import io.druid.query.aggregation.histogram.ApproximateHistogramDruidModule;
import io.druid.query.aggregation.histogram.ApproximateHistogramFoldingAggregatorFactory;
import io.druid.query.aggregation.histogram.QuantilePostAggregator;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.IndexBuilder;
import io.druid.segment.QueryableIndex;
import io.druid.segment.TestHelper;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.virtual.ExpressionVirtualColumn;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.Calcites;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.QueryLogHook;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.List;

public class QuantileSqlAggregatorTest
{
  private static final String DATA_SOURCE = "foo";

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

    // Note: this is needed in order to properly register the serde for Histogram.
    new ApproximateHistogramDruidModule().configure(null);

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .indexMerger(TestHelper.getTestIndexMergerV9())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                                         new ApproximateHistogramAggregatorFactory(
                                                             "hist_m1",
                                                             "m1",
                                                             null,
                                                             null,
                                                             null,
                                                             null
                                                         )
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(CalciteTests.ROWS1)
                                             .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(CalciteTests.queryRunnerFactoryConglomerate()).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .build(),
        index
    );

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(walker, plannerConfig);
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(new QuantileSqlAggregator()),
        ImmutableSet.of()
    );
    plannerFactory = new PlannerFactory(
        druidSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker),
        operatorTable,
        CalciteTests.createExprMacroTable(),
        plannerConfig
    );
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
    try (final DruidPlanner planner = plannerFactory.createPlanner(null)) {
      final String sql = "SELECT\n"
                         + "APPROX_QUANTILE(m1, 0.01),\n"
                         + "APPROX_QUANTILE(m1, 0.5, 50),\n"
                         + "APPROX_QUANTILE(m1, 0.98, 200),\n"
                         + "APPROX_QUANTILE(m1, 0.99),\n"
                         + "APPROX_QUANTILE(m1 * 2, 0.97),\n"
                         + "APPROX_QUANTILE(m1, 0.99) FILTER(WHERE dim1 = 'abc'),\n"
                         + "APPROX_QUANTILE(m1, 0.999) FILTER(WHERE dim1 <> 'abc'),\n"
                         + "APPROX_QUANTILE(m1, 0.999) FILTER(WHERE dim1 = 'abc'),\n"
                         + "APPROX_QUANTILE(cnt, 0.5)\n"
                         + "FROM foo";

      final PlannerResult plannerResult = planner.plan(sql);

      // Verify results
      final List<Object[]> results = Sequences.toList(plannerResult.run(), new ArrayList<Object[]>());
      final List<Object[]> expectedResults = ImmutableList.of(
          new Object[]{
              1.0,
              3.0,
              5.880000114440918,
              5.940000057220459,
              11.640000343322754,
              6.0,
              4.994999885559082,
              6.0,
              1.0
          }
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
                .granularity(Granularities.ALL)
                .virtualColumns(
                    new ExpressionVirtualColumn(
                        "a4:v",
                        "(\"m1\" * 2)",
                        ValueType.FLOAT,
                        TestExprMacroTable.INSTANCE
                    )
                )
                .aggregators(ImmutableList.of(
                    new ApproximateHistogramAggregatorFactory("a0:agg", "m1", null, null, null, null),
                    new ApproximateHistogramAggregatorFactory("a2:agg", "m1", 200, null, null, null),
                    new ApproximateHistogramAggregatorFactory("a4:agg", "a4:v", null, null, null, null),
                    new FilteredAggregatorFactory(
                        new ApproximateHistogramAggregatorFactory("a5:agg", "m1", null, null, null, null),
                        new SelectorDimFilter("dim1", "abc", null)
                    ),
                    new FilteredAggregatorFactory(
                        new ApproximateHistogramAggregatorFactory("a6:agg", "m1", null, null, null, null),
                        new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                    ),
                    new ApproximateHistogramAggregatorFactory("a8:agg", "cnt", null, null, null, null)
                ))
                .postAggregators(ImmutableList.<PostAggregator>of(
                    new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                    new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                    new QuantilePostAggregator("a2", "a2:agg", 0.98f),
                    new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                    new QuantilePostAggregator("a4", "a4:agg", 0.97f),
                    new QuantilePostAggregator("a5", "a5:agg", 0.99f),
                    new QuantilePostAggregator("a6", "a6:agg", 0.999f),
                    new QuantilePostAggregator("a7", "a5:agg", 0.999f),
                    new QuantilePostAggregator("a8", "a8:agg", 0.50f)
                ))
                .context(ImmutableMap.<String, Object>of("skipEmptyBuckets", true))
                .build(),
          Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
      );
    }
  }

  @Test
  public void testQuantileOnComplexColumn() throws Exception
  {
    try (final DruidPlanner planner = plannerFactory.createPlanner(null)) {
      final String sql = "SELECT\n"
                         + "APPROX_QUANTILE(hist_m1, 0.01),\n"
                         + "APPROX_QUANTILE(hist_m1, 0.5, 50),\n"
                         + "APPROX_QUANTILE(hist_m1, 0.98, 200),\n"
                         + "APPROX_QUANTILE(hist_m1, 0.99),\n"
                         + "APPROX_QUANTILE(hist_m1, 0.99) FILTER(WHERE dim1 = 'abc'),\n"
                         + "APPROX_QUANTILE(hist_m1, 0.999) FILTER(WHERE dim1 <> 'abc'),\n"
                         + "APPROX_QUANTILE(hist_m1, 0.999) FILTER(WHERE dim1 = 'abc')\n"
                         + "FROM foo";

      final PlannerResult plannerResult = planner.plan(sql);

      // Verify results
      final List<Object[]> results = Sequences.toList(plannerResult.run(), new ArrayList<Object[]>());
      final List<Object[]> expectedResults = ImmutableList.of(
          new Object[]{1.0, 3.0, 5.880000114440918, 5.940000057220459, 6.0, 4.994999885559082, 6.0}
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
                .granularity(Granularities.ALL)
                .aggregators(ImmutableList.of(
                    new ApproximateHistogramFoldingAggregatorFactory("a0:agg", "hist_m1", null, null, null, null),
                    new ApproximateHistogramFoldingAggregatorFactory("a2:agg", "hist_m1", 200, null, null, null),
                    new FilteredAggregatorFactory(
                        new ApproximateHistogramFoldingAggregatorFactory("a4:agg", "hist_m1", null, null, null, null),
                        new SelectorDimFilter("dim1", "abc", null)
                    ),
                    new FilteredAggregatorFactory(
                        new ApproximateHistogramFoldingAggregatorFactory("a5:agg", "hist_m1", null, null, null, null),
                        new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                    )
                ))
                .postAggregators(ImmutableList.<PostAggregator>of(
                    new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                    new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                    new QuantilePostAggregator("a2", "a2:agg", 0.98f),
                    new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                    new QuantilePostAggregator("a4", "a4:agg", 0.99f),
                    new QuantilePostAggregator("a5", "a5:agg", 0.999f),
                    new QuantilePostAggregator("a6", "a4:agg", 0.999f)
                ))
                .context(ImmutableMap.<String, Object>of("skipEmptyBuckets", true))
                .build(),
          Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
      );
    }
  }
}
