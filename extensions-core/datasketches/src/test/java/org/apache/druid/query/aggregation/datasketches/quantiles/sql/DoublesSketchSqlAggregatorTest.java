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

package org.apache.druid.query.aggregation.datasketches.quantiles.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchModule;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToCDFPostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToHistogramPostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToQuantilePostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToQuantilesPostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToRankPostAggregator;
import org.apache.druid.query.aggregation.datasketches.quantiles.DoublesSketchToStringPostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.NotDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
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

public class DoublesSketchSqlAggregatorTest extends CalciteTestBase
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
    DoublesSketchModule.registerSerde();
    for (Module mod : new DoublesSketchModule().getJacksonModules()) {
      CalciteTests.getJsonMapper().registerModule(mod);
      TestHelper.JSON_MAPPER.registerModule(mod);
    }

    final QueryableIndex index =
        IndexBuilder.create()
                    .tmpDir(temporaryFolder.newFolder())
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        new IncrementalIndexSchema.Builder()
                            .withMetrics(
                                new CountAggregatorFactory("cnt"),
                                new DoubleSumAggregatorFactory("m1", "m1"),
                                new DoublesSketchAggregatorFactory(
                                    "qsketch_m1",
                                    "m1",
                                    128
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
        ImmutableSet.of(
            new DoublesSketchApproxQuantileSqlAggregator(),
            new DoublesSketchObjectSqlAggregator()
        ),
        ImmutableSet.of(
            new DoublesSketchQuantileOperatorConversion(),
            new DoublesSketchQuantilesOperatorConversion(),
            new DoublesSketchToHistogramOperatorConversion(),
            new DoublesSketchRankOperatorConversion(),
            new DoublesSketchCDFOperatorConversion(),
            new DoublesSketchSummaryOperatorConversion()
        )
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
                       + "APPROX_QUANTILE_DS(m1, 0.01),\n"
                       + "APPROX_QUANTILE_DS(m1, 0.5, 64),\n"
                       + "APPROX_QUANTILE_DS(m1, 0.98, 256),\n"
                       + "APPROX_QUANTILE_DS(m1, 0.99),\n"
                       + "APPROX_QUANTILE_DS(m1 * 2, 0.97),\n"
                       + "APPROX_QUANTILE_DS(m1, 0.99) FILTER(WHERE dim1 = 'abc'),\n"
                       + "APPROX_QUANTILE_DS(m1, 0.999) FILTER(WHERE dim1 <> 'abc'),\n"
                       + "APPROX_QUANTILE_DS(m1, 0.999) FILTER(WHERE dim1 = 'abc'),\n"
                       + "APPROX_QUANTILE_DS(cnt, 0.5)\n"
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
            1.0,
            4.0,
            6.0,
            6.0,
            12.0,
            6.0,
            5.0,
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
                      "v0",
                      "(\"m1\" * 2)",
                      ValueType.FLOAT,
                      TestExprMacroTable.INSTANCE
                  )
              )
              .aggregators(ImmutableList.of(
                  new DoublesSketchAggregatorFactory("a0:agg", "m1", null),
                  new DoublesSketchAggregatorFactory("a1:agg", "m1", 64),
                  new DoublesSketchAggregatorFactory("a2:agg", "m1", 256),
                  new DoublesSketchAggregatorFactory("a4:agg", "v0", null),
                  new FilteredAggregatorFactory(
                      new DoublesSketchAggregatorFactory("a5:agg", "m1", null),
                      new SelectorDimFilter("dim1", "abc", null)
                  ),
                  new FilteredAggregatorFactory(
                      new DoublesSketchAggregatorFactory("a6:agg", "m1", null),
                      new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                  ),
                  new DoublesSketchAggregatorFactory("a8:agg", "cnt", null)
              ))
              .postAggregators(
                  new DoublesSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.01f),
                  new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.50f),
                  new DoublesSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a2:agg"), 0.98f),
                  new DoublesSketchToQuantilePostAggregator("a3", makeFieldAccessPostAgg("a0:agg"), 0.99f),
                  new DoublesSketchToQuantilePostAggregator("a4", makeFieldAccessPostAgg("a4:agg"), 0.97f),
                  new DoublesSketchToQuantilePostAggregator("a5", makeFieldAccessPostAgg("a5:agg"), 0.99f),
                  new DoublesSketchToQuantilePostAggregator("a6", makeFieldAccessPostAgg("a6:agg"), 0.999f),
                  new DoublesSketchToQuantilePostAggregator("a7", makeFieldAccessPostAgg("a5:agg"), 0.999f),
                  new DoublesSketchToQuantilePostAggregator("a8", makeFieldAccessPostAgg("a8:agg"), 0.50f)
              )
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testQuantileOnComplexColumn() throws Exception
  {
    SqlLifecycle lifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "APPROX_QUANTILE_DS(qsketch_m1, 0.01),\n"
                       + "APPROX_QUANTILE_DS(qsketch_m1, 0.5, 64),\n"
                       + "APPROX_QUANTILE_DS(qsketch_m1, 0.98, 256),\n"
                       + "APPROX_QUANTILE_DS(qsketch_m1, 0.99),\n"
                       + "APPROX_QUANTILE_DS(qsketch_m1, 0.99) FILTER(WHERE dim1 = 'abc'),\n"
                       + "APPROX_QUANTILE_DS(qsketch_m1, 0.999) FILTER(WHERE dim1 <> 'abc'),\n"
                       + "APPROX_QUANTILE_DS(qsketch_m1, 0.999) FILTER(WHERE dim1 = 'abc')\n"
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
            1.0,
            4.0,
            6.0,
            6.0,
            6.0,
            5.0,
            6.0
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
              .aggregators(ImmutableList.of(
                  new DoublesSketchAggregatorFactory("a0:agg", "qsketch_m1", null),
                  new DoublesSketchAggregatorFactory("a1:agg", "qsketch_m1", 64),
                  new DoublesSketchAggregatorFactory("a2:agg", "qsketch_m1", 256),
                  new FilteredAggregatorFactory(
                      new DoublesSketchAggregatorFactory("a4:agg", "qsketch_m1", null),
                      new SelectorDimFilter("dim1", "abc", null)
                  ),
                  new FilteredAggregatorFactory(
                      new DoublesSketchAggregatorFactory("a5:agg", "qsketch_m1", null),
                      new NotDimFilter(new SelectorDimFilter("dim1", "abc", null))
                  )
              ))
              .postAggregators(
                  new DoublesSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.01f),
                  new DoublesSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.50f),
                  new DoublesSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a2:agg"), 0.98f),
                  new DoublesSketchToQuantilePostAggregator("a3", makeFieldAccessPostAgg("a0:agg"), 0.99f),
                  new DoublesSketchToQuantilePostAggregator("a4", makeFieldAccessPostAgg("a4:agg"), 0.99f),
                  new DoublesSketchToQuantilePostAggregator("a5", makeFieldAccessPostAgg("a5:agg"), 0.999f),
                  new DoublesSketchToQuantilePostAggregator("a6", makeFieldAccessPostAgg("a4:agg"), 0.999f)
              )
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testQuantileOnInnerQuery() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT AVG(x), APPROX_QUANTILE_DS(x, 0.98)\n"
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
      expectedResults = ImmutableList.of(new Object[]{7.0, 11.0});
    } else {
      expectedResults = ImmutableList.of(new Object[]{5.25, 8.0});
    }
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
                                        .setDimensions(new DefaultDimensionSpec("dim2", "d0"))
                                        .setAggregatorSpecs(
                                            ImmutableList.of(
                                                new DoubleSumAggregatorFactory("a0", "m1")
                                            )
                                        )
                                        .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                                        .build()
                        )
                    )
                    .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        new DoubleSumAggregatorFactory("_a0:sum", "a0"),
                        new CountAggregatorFactory("_a0:count"),
                        new DoublesSketchAggregatorFactory(
                            "_a1:agg",
                            "a0",
                            null
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
                            new DoublesSketchToQuantilePostAggregator("_a1", makeFieldAccessPostAgg("_a1:agg"), 0.98f)
                        )
                    )
                    .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                    .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testQuantileOnInnerQuantileQuery() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT dim1, APPROX_QUANTILE_DS(x, 0.5)\n"
                       + "FROM (SELECT dim1, dim2, APPROX_QUANTILE_DS(m1, 0.5) AS x FROM foo GROUP BY dim1, dim2) GROUP BY dim1";


    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();

    ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
    builder.add(new Object[]{"", 1.0});
    builder.add(new Object[]{"1", 4.0});
    builder.add(new Object[]{"10.1", 2.0});
    builder.add(new Object[]{"2", 3.0});
    builder.add(new Object[]{"abc", 6.0});
    builder.add(new Object[]{"def", 5.0});
    final List<Object[]> expectedResults = builder.build();
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
                                        .setDimensions(
                                            new DefaultDimensionSpec("dim1", "d0"),
                                            new DefaultDimensionSpec("dim2", "d1")
                                        )
                                        .setAggregatorSpecs(
                                            ImmutableList.of(
                                                new DoublesSketchAggregatorFactory("a0:agg", "m1", 128)
                                            )
                                        )
                                        .setPostAggregatorSpecs(
                                            ImmutableList.of(
                                                new DoublesSketchToQuantilePostAggregator(
                                                    "a0",
                                                    makeFieldAccessPostAgg("a0:agg"),
                                                    0.5f
                                                )
                                            )
                                        )
                                        .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                                        .build()
                        )
                    )
                    .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                    .setGranularity(Granularities.ALL)
                    .setDimensions(new DefaultDimensionSpec("d0", "_d0", ValueType.STRING))
                    .setAggregatorSpecs(
                        new DoublesSketchAggregatorFactory("_a0:agg", "a0", 128)
                    )
                    .setPostAggregatorSpecs(
                        ImmutableList.of(
                            new DoublesSketchToQuantilePostAggregator("_a0", makeFieldAccessPostAgg("_a0:agg"), 0.5f)
                        )
                    )
                    .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                    .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testDoublesSketchPostAggs() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "  SUM(cnt),\n"
                       + "  APPROX_QUANTILE_DS(cnt, 0.5) + 1,\n"
                       + "  DS_GET_QUANTILE(DS_QUANTILES_SKETCH(cnt), 0.5) + 1000,\n"
                       + "  DS_GET_QUANTILE(DS_QUANTILES_SKETCH(cnt + 123), 0.5) + 1000,\n"
                       + "  ABS(DS_GET_QUANTILE(DS_QUANTILES_SKETCH(cnt), 0.5)),\n"
                       + "  DS_GET_QUANTILES(DS_QUANTILES_SKETCH(cnt), 0.5, 0.8),\n"
                       + "  DS_HISTOGRAM(DS_QUANTILES_SKETCH(cnt), 0.2, 0.6),\n"
                       + "  DS_RANK(DS_QUANTILES_SKETCH(cnt), 3),\n"
                       + "  DS_CDF(DS_QUANTILES_SKETCH(cnt), 0.2, 0.6),\n"
                       + "  DS_QUANTILE_SUMMARY(DS_QUANTILES_SKETCH(cnt))\n"
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
            6L,
            2.0d,
            1001.0d,
            1124.0d,
            1.0d,
            "[1.0,1.0]",
            "[0.0,0.0,6.0]",
            1.0d,
            "[0.0,0.0,1.0]",
            "\n"
              + "### Quantiles HeapUpdateDoublesSketch SUMMARY: \n"
              + "   Empty                        : false\n"
              + "   Direct, Capacity bytes       : false, \n"
              + "   Estimation Mode              : false\n"
              + "   K                            : 128\n"
              + "   N                            : 6\n"
              + "   Levels (Needed, Total, Valid): 0, 0, 0\n"
              + "   Level Bit Pattern            : 0\n"
              + "   BaseBufferCount              : 6\n"
              + "   Combined Buffer Capacity     : 8\n"
              + "   Retained Items               : 6\n"
              + "   Compact Storage Bytes        : 80\n"
              + "   Updatable Storage Bytes      : 96\n"
              + "   Normalized Rank Error        : 1.406%\n"
              + "   Normalized Rank Error (PMF)  : 1.711%\n"
              + "   Min Value                    : 1.000000e+00\n"
              + "   Max Value                    : 1.000000e+00\n"
              + "### END SKETCH SUMMARY\n"
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Query actualQuery = Iterables.getOnlyElement(queryLogHook.getRecordedQueries());
    Query expectedQuery = Druids.newTimeseriesQueryBuilder()
                                .dataSource(CalciteTests.DATASOURCE1)
                                .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                .granularity(Granularities.ALL)
                                .virtualColumns(
                                    new ExpressionVirtualColumn(
                                        "v0",
                                        "(\"cnt\" + 123)",
                                        ValueType.FLOAT,
                                        TestExprMacroTable.INSTANCE
                                    )
                                )
                                .aggregators(ImmutableList.of(
                                    new LongSumAggregatorFactory("a0", "cnt"),
                                    new DoublesSketchAggregatorFactory("a1:agg", "cnt", 128),
                                    new DoublesSketchAggregatorFactory("a2:agg", "cnt", 128),
                                    new DoublesSketchAggregatorFactory("a3:agg", "v0", 128)
                                ))
                                .postAggregators(
                                    new DoublesSketchToQuantilePostAggregator(
                                        "a1",
                                        makeFieldAccessPostAgg("a1:agg"),
                                        0.5f
                                    ),
                                    new ExpressionPostAggregator(
                                        "p0",
                                        "(\"a1\" + 1)",
                                        null,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    new DoublesSketchToQuantilePostAggregator(
                                        "p2",
                                        new FieldAccessPostAggregator(
                                            "p1",
                                            "a2:agg"
                                        ),
                                        0.5f
                                    ),
                                    new ExpressionPostAggregator(
                                        "p3",
                                        "(p2 + 1000)",
                                        null,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    new DoublesSketchToQuantilePostAggregator(
                                        "p5",
                                        new FieldAccessPostAggregator(
                                            "p4",
                                            "a3:agg"
                                        ),
                                        0.5f
                                    ),
                                    new ExpressionPostAggregator(
                                        "p6",
                                        "(p5 + 1000)",
                                        null,
                                        TestExprMacroTable.INSTANCE
                                    ),
                                    new DoublesSketchToQuantilePostAggregator(
                                        "p8",
                                        new FieldAccessPostAggregator(
                                            "p7",
                                            "a2:agg"
                                        ),
                                        0.5f
                                    ),
                                    new ExpressionPostAggregator("p9", "abs(p8)", null, TestExprMacroTable.INSTANCE),
                                    new DoublesSketchToQuantilesPostAggregator(
                                        "p11",
                                        new FieldAccessPostAggregator(
                                            "p10",
                                            "a2:agg"
                                        ),
                                        new double[]{0.5d, 0.8d}
                                    ),
                                    new DoublesSketchToHistogramPostAggregator(
                                        "p13",
                                        new FieldAccessPostAggregator(
                                            "p12",
                                            "a2:agg"
                                        ),
                                        new double[]{0.2d, 0.6d},
                                        null
                                    ),
                                    new DoublesSketchToRankPostAggregator(
                                        "p15",
                                        new FieldAccessPostAggregator(
                                            "p14",
                                            "a2:agg"
                                        ),
                                        3.0d
                                    ),
                                    new DoublesSketchToCDFPostAggregator(
                                        "p17",
                                        new FieldAccessPostAggregator(
                                            "p16",
                                            "a2:agg"
                                        ),
                                        new double[]{0.2d, 0.6d}
                                    ),
                                    new DoublesSketchToStringPostAggregator(
                                        "p19",
                                        new FieldAccessPostAggregator(
                                            "p18",
                                            "a2:agg"
                                        )
                                    )
                                )
                                .context(ImmutableMap.of(
                                    "skipEmptyBuckets",
                                    true,
                                    PlannerContext.CTX_SQL_QUERY_ID,
                                    "dummy"
                                ))
                                .build();

    // Verify query
    Assert.assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testDoublesSketchPostAggsPostSort() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT DS_QUANTILES_SKETCH(m1) as y FROM druid.foo ORDER BY  DS_GET_QUANTILE(DS_QUANTILES_SKETCH(m1), 0.5) DESC LIMIT 10";
    final String sql2 = StringUtils.format("SELECT DS_GET_QUANTILE(y, 0.5), DS_GET_QUANTILE(y, 0.98) from (%s)", sql);

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql2,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            4.0d,
            6.0d
        }
    );

    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Query actualQuery = Iterables.getOnlyElement(queryLogHook.getRecordedQueries());

    Query expectedQuery =
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(
                  ImmutableList.of(
                      new DoublesSketchAggregatorFactory("a0:agg", "m1", 128)
                  )
              )
              .postAggregators(
                  ImmutableList.of(
                      new FieldAccessPostAggregator("p0", "a0:agg"),
                      new DoublesSketchToQuantilePostAggregator(
                          "p2",
                          new FieldAccessPostAggregator("p1", "a0:agg"),
                          0.5
                      ),
                      new DoublesSketchToQuantilePostAggregator("s1", new FieldAccessPostAggregator("s0", "p0"), 0.5),
                      new DoublesSketchToQuantilePostAggregator(
                          "s3",
                          new FieldAccessPostAggregator("s2", "p0"),
                          0.9800000190734863
                      )
                  )
              )
              .context(ImmutableMap.of(
                  "skipEmptyBuckets", true,
                  PlannerContext.CTX_SQL_QUERY_ID, "dummy"
              ))
              .build();

    // Verify query
    Assert.assertEquals(expectedQuery, actualQuery);
  }

  private static PostAggregator makeFieldAccessPostAgg(String name)
  {
    return new FieldAccessPostAggregator(name, name);
  }
}
