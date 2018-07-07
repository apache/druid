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

package io.druid.query.aggregation.histogram.sql;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.query.Druids;
import io.druid.query.QueryDataSource;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.histogram.ApproximateHistogramAggregatorFactory;
import io.druid.query.aggregation.histogram.ApproximateHistogramDruidModule;
import io.druid.query.aggregation.histogram.ApproximateHistogramFoldingAggregatorFactory;
import io.druid.query.aggregation.histogram.QuantilePostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.expression.TestExprMacroTable;
import io.druid.query.filter.NotDimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.segment.IndexBuilder;
import io.druid.segment.QueryableIndex;
import io.druid.segment.column.ValueType;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.virtual.ExpressionVirtualColumn;
import io.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import io.druid.server.security.AuthTestUtils;
import io.druid.server.security.NoopEscalator;
import io.druid.sql.calcite.filtration.Filtration;
import io.druid.sql.calcite.planner.DruidOperatorTable;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.schema.DruidSchema;
import io.druid.sql.calcite.util.CalciteTestBase;
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

import java.util.List;

public class QuantileSqlAggregatorTest extends CalciteTestBase
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
    // Note: this is needed in order to properly register the serde for Histogram.
    new ApproximateHistogramDruidModule().configure(null);

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
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
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper()
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

      final PlannerResult plannerResult = planner.plan(
          sql,
          NoopEscalator.getInstance().createEscalatedAuthenticationResult()
      );

      // Verify results
      final List<Object[]> results = plannerResult.run().toList();
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
                .postAggregators(
                    new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                    new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                    new QuantilePostAggregator("a2", "a2:agg", 0.98f),
                    new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                    new QuantilePostAggregator("a4", "a4:agg", 0.97f),
                    new QuantilePostAggregator("a5", "a5:agg", 0.99f),
                    new QuantilePostAggregator("a6", "a6:agg", 0.999f),
                    new QuantilePostAggregator("a7", "a5:agg", 0.999f),
                    new QuantilePostAggregator("a8", "a8:agg", 0.50f)
                )
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

      final PlannerResult plannerResult = planner.plan(
          sql,
          NoopEscalator.getInstance().createEscalatedAuthenticationResult()
      );

      // Verify results
      final List<Object[]> results = plannerResult.run().toList();
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
                .postAggregators(
                    new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                    new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                    new QuantilePostAggregator("a2", "a2:agg", 0.98f),
                    new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                    new QuantilePostAggregator("a4", "a4:agg", 0.99f),
                    new QuantilePostAggregator("a5", "a5:agg", 0.999f),
                    new QuantilePostAggregator("a6", "a4:agg", 0.999f)
                )
                .context(ImmutableMap.<String, Object>of("skipEmptyBuckets", true))
                .build(),
          Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
      );
    }
  }

  @Test
  public void testQuantileOnInnerQuery() throws Exception
  {
    try (final DruidPlanner planner = plannerFactory.createPlanner(null)) {
      final String sql = "SELECT AVG(x), APPROX_QUANTILE(x, 0.98)\n"
                         + "FROM (SELECT dim2, SUM(m1) AS x FROM foo GROUP BY dim2)";

      final PlannerResult plannerResult = planner.plan(
          sql,
          NoopEscalator.getInstance().createEscalatedAuthenticationResult()
      );

      // Verify results
      final List<Object[]> results = plannerResult.run().toList();
      final List<Object[]> expectedResults = ImmutableList.of(
          new Object[]{7.0, 8.26386833190918}
      );
      Assert.assertEquals(expectedResults.size(), results.size());
      for (int i = 0; i < expectedResults.size(); i++) {
        Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
      }

      // Verify query
      Assert.assertEquals(
          GroupByQuery.builder()
                      .setDataSource(
                          new QueryDataSource(
                              GroupByQuery.builder()
                                          .setDataSource(CalciteTests.DATASOURCE1)
                                          .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                          .setGranularity(Granularities.ALL)
                                          .setDimensions(ImmutableList.of(new DefaultDimensionSpec("dim2", "d0")))
                                          .setAggregatorSpecs(
                                              ImmutableList.of(
                                                  new DoubleSumAggregatorFactory("a0", "m1")
                                              )
                                          )
                                          .setContext(ImmutableMap.of())
                                          .build()
                          )
                      )
                      .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                      .setGranularity(Granularities.ALL)
                      .setAggregatorSpecs(ImmutableList.of(
                          new DoubleSumAggregatorFactory("_a0:sum", "a0"),
                          new CountAggregatorFactory("_a0:count"),
                          new ApproximateHistogramAggregatorFactory("_a1:agg", "a0", null, null, null, null)
                      ))
                      .setPostAggregatorSpecs(
                          ImmutableList.of(
                              new ArithmeticPostAggregator(
                                  "_a0",
                                  "quotient",
                                  ImmutableList.of(
                                      new FieldAccessPostAggregator(null, "_a0:sum"),
                                      new FieldAccessPostAggregator(null, "_a0:count")
                                  )
                              ),
                              new QuantilePostAggregator("_a1", "_a1:agg", 0.98f)
                          )
                      )
                      .setContext(ImmutableMap.of())
                      .build(),
          Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
      );
    }
  }
}
