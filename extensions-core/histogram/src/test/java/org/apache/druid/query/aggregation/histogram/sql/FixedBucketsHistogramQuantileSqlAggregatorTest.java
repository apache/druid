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

package org.apache.druid.query.aggregation.histogram.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.ApproximateHistogramDruidModule;
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogram;
import org.apache.druid.query.aggregation.histogram.FixedBucketsHistogramAggregatorFactory;
import org.apache.druid.query.aggregation.histogram.QuantilePostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.filtration.Filtration;
import org.apache.druid.sql.calcite.planner.DruidOperatorTable;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.util.CalciteTestBase;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.QueryLogHook;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class FixedBucketsHistogramQuantileSqlAggregatorTest extends CalciteTestBase
{
  private static final String DATA_SOURCE = "foo";

  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  private static AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;
  private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, "dummy"
  );

  @BeforeClass
  public static void setUpClass()
  {
    resourceCloser = Closer.create();
    conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(resourceCloser);
  }

  @AfterClass
  public static void tearDownClass() throws IOException
  {
    resourceCloser.close();
  }

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create();

  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlLifecycleFactory sqlLifecycleFactory;

  @Before
  public void setUp() throws Exception
  {
    ApproximateHistogramDruidModule.registerSerde();
    for (Module mod : new ApproximateHistogramDruidModule().getJacksonModules()) {
      CalciteTests.getJsonMapper().registerModule(mod);
    }

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                                         new FixedBucketsHistogramAggregatorFactory(
                                                             "fbhist_m1",
                                                             "m1",
                                                             20,
                                                             0,
                                                             10,
                                                             FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                                             false
                                                         )
                                                     )
                                                     .withRollup(false)
                                                     .build()
                                             )
                                             .rows(CalciteTests.ROWS1)
                                             .buildMMappedIndex();

    walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        DataSegment.builder()
                   .dataSource(DATA_SOURCE)
                   .interval(index.getDataInterval())
                   .version("1")
                   .shardSpec(new LinearShardSpec(0))
                   .size(0)
                   .build(),
        index
    );

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(new QuantileSqlAggregator(), new FixedBucketsHistogramQuantileSqlAggregator()),
        ImmutableSet.of()
    );
    SchemaPlus rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);

    sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(
        new PlannerFactory(
            rootSchema,
            CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
            operatorTable,
            CalciteTests.createExprMacroTable(),
            plannerConfig,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            CalciteTests.getJsonMapper(),
            CalciteTests.DRUID_SCHEMA_NAME
        )
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
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.01, 20, 0.0, 10.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.5, 20, 0.0, 10.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.98, 20, 0.0, 10.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.99, 20, 0.0, 10.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(m1 * 2, 0.97, 40, 0.0, 20.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.99, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc'),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 <> 'abc'),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc'),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(cnt, 0.5, 20, 0.0, 10.0)\n"
                       + "FROM foo";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1.0299999713897705,
            3.5,
            6.440000057220459,
            6.470000267028809,
            12.40999984741211,
            6.494999885559082,
            5.497499942779541,
            6.499499797821045,
            1.25
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Query actual = Iterables.getOnlyElement(queryLogHook.getRecordedQueries());
    Query expected = Druids.newTimeseriesQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                           .granularity(Granularities.ALL)
                           .virtualColumns(
                               new ExpressionVirtualColumn(
                                   "v0",
                                   "(\"m1\" * 2)",
                                   ValueType.FLOAT,
                                   TestExprMacroTable.INSTANCE
                               )
                           )
                           .aggregators(ImmutableList.of(
                               new FixedBucketsHistogramAggregatorFactory(
                                   "a0:agg",
                                   "m1",
                                   20,
                                   0.0d,
                                   10.0d,
                                   FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                   false
                               ),
                               new FixedBucketsHistogramAggregatorFactory(
                                   "a4:agg",
                                   "v0",
                                   40,
                                   0.0d,
                                   20.0d,
                                   FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                   false
                               ),
                               new FilteredAggregatorFactory(
                                   new FixedBucketsHistogramAggregatorFactory(
                                       "a5:agg",
                                       "m1",
                                       20,
                                       0.0d,
                                       10.0d,
                                       FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                       false
                                   ),
                                   new SelectorDimFilter("dim1", "abc", null)
                               ),
                               new FilteredAggregatorFactory(
                                   new FixedBucketsHistogramAggregatorFactory(
                                       "a6:agg",
                                       "m1",
                                       20,
                                       0.0d,
                                       10.0d,
                                       FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                       false
                                   ),
                                   new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                               ),
                               new FixedBucketsHistogramAggregatorFactory(
                                   "a8:agg",
                                   "cnt",
                                   20,
                                   0.0d,
                                   10.0d,
                                   FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                   false
                               )
                           ))
                           .postAggregators(
                               new QuantilePostAggregator("a0", "a0:agg", 0.01f),
                               new QuantilePostAggregator("a1", "a0:agg", 0.50f),
                               new QuantilePostAggregator("a2", "a0:agg", 0.98f),
                               new QuantilePostAggregator("a3", "a0:agg", 0.99f),
                               new QuantilePostAggregator("a4", "a4:agg", 0.97f),
                               new QuantilePostAggregator("a5", "a5:agg", 0.99f),
                               new QuantilePostAggregator("a6", "a6:agg", 0.999f),
                               new QuantilePostAggregator("a7", "a5:agg", 0.999f),
                               new QuantilePostAggregator("a8", "a8:agg", 0.50f)
                           )
                           .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                           .build();

    // Verify query
    Assert.assertEquals(
        expected,
        actual
    );
  }

  @Test
  public void testQuantileOnComplexColumn() throws Exception
  {
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.01, 20, 0.0, 10.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.5, 20, 0.0, 10.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.98, 30, 0.0, 10.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.99, 20, 0.0, 10.0),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.99, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc'),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 <> 'abc'),\n"
                       + "APPROX_QUANTILE_FIXED_BUCKETS(fbhist_m1, 0.999, 20, 0.0, 10.0) FILTER(WHERE dim1 = 'abc')\n"
                       + "FROM foo";

    // Verify results
    final List<Object[]> results = lifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1.0299999713897705,
            3.5,
            6.293333530426025,
            6.470000267028809,
            6.494999885559082,
            5.497499942779541,
            6.499499797821045
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Query actual = Iterables.getOnlyElement(queryLogHook.getRecordedQueries());
    Query expected = Druids.newTimeseriesQueryBuilder()
                           .dataSource(CalciteTests.DATASOURCE1)
                           .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                           .granularity(Granularities.ALL)
                           .aggregators(ImmutableList.of(
                               new FixedBucketsHistogramAggregatorFactory(
                                   "a0:agg",
                                   "fbhist_m1",
                                   20,
                                   0.0,
                                   10.0,
                                   FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                   false
                               ),
                               new FixedBucketsHistogramAggregatorFactory(
                                   "a2:agg",
                                   "fbhist_m1",
                                   30,
                                   0.0,
                                   10.0,
                                   FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                   false
                               ),
                               new FilteredAggregatorFactory(
                                   new FixedBucketsHistogramAggregatorFactory(
                                       "a4:agg",
                                       "fbhist_m1",
                                       20,
                                       0.0,
                                       10.0,
                                       FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                       false
                                   ),
                                   new SelectorDimFilter("dim1", "abc", null)
                               ),
                               new FilteredAggregatorFactory(
                                   new FixedBucketsHistogramAggregatorFactory(
                                       "a5:agg",
                                       "fbhist_m1",
                                       20,
                                       0.0,
                                       10.0,
                                       FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                       false
                                   ),
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
                           .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                           .build();

    // Verify query
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testQuantileOnInnerQuery() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT AVG(x), APPROX_QUANTILE_FIXED_BUCKETS(x, 0.98, 100, 0.0, 100.0)\n"
                       + "FROM (SELECT dim2, SUM(m1) AS x FROM foo GROUP BY dim2)";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults;
    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(new Object[]{7.0, 11.940000534057617});
    } else {
      expectedResults = ImmutableList.of(new Object[]{5.25, 8.920000076293945});
    }
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Query actual = Iterables.getOnlyElement(queryLogHook.getRecordedQueries());
    Query expected = GroupByQuery.builder()
                                 .setDataSource(
                                     new QueryDataSource(
                                         GroupByQuery.builder()
                                                     .setDataSource(CalciteTests.DATASOURCE1)
                                                     .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(
                                                         Filtration.eternity())))
                                                     .setGranularity(Granularities.ALL)
                                                     .setDimensions(new DefaultDimensionSpec("dim2", "d0"))
                                                     .setAggregatorSpecs(
                                                         ImmutableList.of(
                                                             new DoubleSumAggregatorFactory("a0", "m1")
                                                         )
                                                     )
                                                     .setContext(ImmutableMap.of(
                                                         PlannerContext.CTX_SQL_QUERY_ID,
                                                         "dummy"
                                                     ))
                                                     .build()
                                     )
                                 )
                                 .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                 .setGranularity(Granularities.ALL)
                                 .setAggregatorSpecs(
                                     new DoubleSumAggregatorFactory("_a0:sum", "a0"),
                                     new CountAggregatorFactory("_a0:count"),
                                     new FixedBucketsHistogramAggregatorFactory(
                                         "_a1:agg",
                                         "a0",
                                         100,
                                         0,
                                         100.0d,
                                         FixedBucketsHistogram.OutlierHandlingMode.IGNORE,
                                         false
                                     )
                                 )
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
                                 .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                                 .build();

    // Verify query
    Assert.assertEquals(expected, actual);
  }
}
