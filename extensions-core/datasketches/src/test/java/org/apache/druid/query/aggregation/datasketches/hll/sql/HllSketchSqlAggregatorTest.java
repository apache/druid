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

package org.apache.druid.query.aggregation.datasketches.hll.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchBuildAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchModule;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToEstimatePostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToEstimateWithBoundsPostAggregator;
import org.apache.druid.query.aggregation.datasketches.hll.HllSketchToStringPostAggregator;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.ExpressionPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.BaseCalciteQueryTest;
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
import org.joda.time.DateTimeZone;
import org.joda.time.Period;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class HllSketchSqlAggregatorTest extends CalciteTestBase
{
  private static final String DATA_SOURCE = "foo";
  private static final boolean ROUND = true;
  private static QueryRunnerFactoryConglomerate conglomerate;
  private static Closer resourceCloser;
  private static AuthenticationResult authenticationResult = CalciteTests.REGULAR_USER_AUTH_RESULT;

  // For testHllSketchPostAggsGroupBy, testHllSketchPostAggsTimeseries
  private static final Object[] EXPECTED_PA_RESULT =
      new Object[]{
          "\"AgEHDAMIAgDhUv8P63iABQ==\"",
          "\"AgEHDAMIBgALpZ0PjpTfBY5ElQo+C7UE4jA+DKfcYQQ=\"",
          "\"AgEHDAMIAQAr8vsG\"",
          2.000000004967054d,
          3.000000004967054d,
          3.000000014901161d,
          2.000000004967054d,
          "[2.000000004967054,2.0,2.0001997319422404]",
          "[2.000000004967054,2.0,2.000099863468538]",
          "\"AgEHDAMIBgC1EYgH1mlHBwsKPwu5SK8MIiUxB7iZVwU=\"",
          2L,
          "### HLL SKETCH SUMMARY: \n"
            + "  Log Config K   : 12\n"
            + "  Hll Target     : HLL_4\n"
            + "  Current Mode   : LIST\n"
            + "  Memory         : false\n"
            + "  LB             : 2.0\n"
            + "  Estimate       : 2.000000004967054\n"
            + "  UB             : 2.000099863468538\n"
            + "  OutOfOrder Flag: false\n"
            + "  Coupon Count   : 2\n",
          "### HLL SKETCH SUMMARY: \n"
            + "  LOG CONFIG K   : 12\n"
            + "  HLL TARGET     : HLL_4\n"
            + "  CURRENT MODE   : LIST\n"
            + "  MEMORY         : FALSE\n"
            + "  LB             : 2.0\n"
            + "  ESTIMATE       : 2.000000004967054\n"
            + "  UB             : 2.000099863468538\n"
            + "  OUTOFORDER FLAG: FALSE\n"
            + "  COUPON COUNT   : 2\n",
          2.0
      };

  private static final List<VirtualColumn> EXPECTED_PA_VIRTUAL_COLUMNS =
      ImmutableList.of(
          new ExpressionVirtualColumn(
              "v0",
              "concat(\"dim2\",'hello')",
              ValueType.STRING,
              TestExprMacroTable.INSTANCE
          ),
          new ExpressionVirtualColumn(
              "v1",
              "pow(abs((\"m1\" + 100)),2)",
              ValueType.DOUBLE,
              TestExprMacroTable.INSTANCE
          )
      );

  private static final List<AggregatorFactory> EXPECTED_PA_AGGREGATORS =
      ImmutableList.of(
          new HllSketchBuildAggregatorFactory("a0", "dim2", null, null, null, true),
          new HllSketchBuildAggregatorFactory("a1", "m1", null, null, null, true),
          new HllSketchBuildAggregatorFactory("a2", "cnt", null, null, null, true),
          new HllSketchBuildAggregatorFactory("a3", "v0", null, null, null, true),
          new HllSketchBuildAggregatorFactory("a4", "v1", null, null, null, true),
          new HllSketchBuildAggregatorFactory("a5", "dim2", null, null, null, true)
      );

  private static final List<PostAggregator> EXPECTED_PA_POST_AGGREGATORS =
      ImmutableList.of(
          new FieldAccessPostAggregator("p0", "a0"),
          new FieldAccessPostAggregator("p1", "a1"),
          new FieldAccessPostAggregator("p2", "a2"),
          new HllSketchToEstimatePostAggregator("p4", new FieldAccessPostAggregator("p3", "a0"), false),
          new HllSketchToEstimatePostAggregator("p6", new FieldAccessPostAggregator("p5", "a0"), false),
          new ExpressionPostAggregator("p7", "(p6 + 1)", null, TestExprMacroTable.INSTANCE),
          new HllSketchToEstimatePostAggregator("p9", new FieldAccessPostAggregator("p8", "a3"), false),
          new HllSketchToEstimatePostAggregator("p11", new FieldAccessPostAggregator("p10", "a0"), false),
          new ExpressionPostAggregator("p12", "abs(p11)", null, TestExprMacroTable.INSTANCE),
          new HllSketchToEstimateWithBoundsPostAggregator(
              "p14",
              new FieldAccessPostAggregator("p13", "a0"),
              2
          ),
          new HllSketchToEstimateWithBoundsPostAggregator(
              "p16",
              new FieldAccessPostAggregator("p15", "a0"),
              1
          ),
          new FieldAccessPostAggregator("p17", "a4"),
          new HllSketchToStringPostAggregator("p19", new FieldAccessPostAggregator("p18", "a0")),
          new HllSketchToStringPostAggregator("p21", new FieldAccessPostAggregator("p20", "a0")),
          new ExpressionPostAggregator("p22", "upper(p21)", null, TestExprMacroTable.INSTANCE),
          new HllSketchToEstimatePostAggregator("p24", new FieldAccessPostAggregator("p23", "a0"), true)
      );

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public QueryLogHook queryLogHook = QueryLogHook.create(TestHelper.JSON_MAPPER);

  private final Map<String, Object> queryContext;
  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlLifecycleFactory sqlLifecycleFactory;

  public HllSketchSqlAggregatorTest(final String vectorize)
  {
    this.queryContext = ImmutableMap.of(
        PlannerContext.CTX_SQL_QUERY_ID, "dummy",
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
  }

  @Parameterized.Parameters(name = "vectorize = {0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (String vectorize : new String[]{"false", "true", "force"}) {
      constructors.add(new Object[]{vectorize});
    }
    return constructors;
  }

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

  @Before
  public void setUp() throws Exception
  {
    HllSketchModule.registerSerde();
    for (Module mod : new HllSketchModule().getJacksonModules()) {
      CalciteTests.getJsonMapper().registerModule(mod);
      TestHelper.JSON_MAPPER.registerModule(mod);
    }

    final QueryableIndex index = IndexBuilder.create()
                                             .tmpDir(temporaryFolder.newFolder())
                                             .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                                             .schema(
                                                 new IncrementalIndexSchema.Builder()
                                                     .withMetrics(
                                                         new CountAggregatorFactory("cnt"),
                                                         new DoubleSumAggregatorFactory("m1", "m1"),
                                                         new HllSketchBuildAggregatorFactory(
                                                             "hllsketch_dim1",
                                                             "dim1",
                                                             null,
                                                             null,
                                                             null,
                                                             ROUND
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
            new HllSketchApproxCountDistinctSqlAggregator(),
            new HllSketchObjectSqlAggregator()
        ),
        ImmutableSet.of(
            new HllSketchEstimateOperatorConversion(),
            new HllSketchEstimateWithErrorBoundsOperatorConversion(),
            new HllSketchSetUnionOperatorConversion(),
            new HllSketchToStringOperatorConversion()
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
  public void testApproxCountDistinctHllSketch() throws Exception
  {
    // Can't vectorize due to CONCAT expression.
    cannotVectorize();

    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT\n"
                       + "  SUM(cnt),\n"
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2),\n" // uppercase
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(SUBSTRING(dim2, 1, 1)),\n" // on extractionFn
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(SUBSTRING(dim2, 1, 1) || 'x'),\n" // on expression
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(hllsketch_dim1, 21, 'HLL_8'),\n" // on native HllSketch column
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(hllsketch_dim1)\n" // on native HllSketch column
                       + "FROM druid.foo";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        queryContext,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults;

    if (NullHandling.replaceWithDefault()) {
      expectedResults = ImmutableList.of(
          new Object[]{
              6L,
              2L,
              2L,
              1L,
              2L,
              5L,
              5L
          }
      );
    } else {
      expectedResults = ImmutableList.of(
          new Object[]{
              6L,
              2L,
              2L,
              1L,
              1L,
              5L,
              5L
          }
      );
    }

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
                      "substring(\"dim2\", 0, 1)",
                      ValueType.STRING,
                      TestExprMacroTable.INSTANCE
                  ),
                  new ExpressionVirtualColumn(
                      "v1",
                      "concat(substring(\"dim2\", 0, 1),'x')",
                      ValueType.STRING,
                      TestExprMacroTable.INSTANCE
                  )
              )
              .aggregators(
                  ImmutableList.of(
                      new LongSumAggregatorFactory("a0", "cnt"),
                      new HllSketchBuildAggregatorFactory(
                          "a1",
                          "dim2",
                          null,
                          null,
                          null,
                          ROUND
                      ),
                      new FilteredAggregatorFactory(
                          new HllSketchBuildAggregatorFactory(
                              "a2",
                              "dim2",
                              null,
                              null,
                              null,
                              ROUND
                          ),
                          BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("dim2", "", null))
                      ),
                      new HllSketchBuildAggregatorFactory(
                          "a3",
                          "v0",
                          null,
                          null,
                          null,
                          ROUND
                      ),
                      new HllSketchBuildAggregatorFactory(
                          "a4",
                          "v1",
                          null,
                          null,
                          null,
                          ROUND
                      ),
                      new HllSketchMergeAggregatorFactory("a5", "hllsketch_dim1", 21, "HLL_8", null, ROUND),
                      new HllSketchMergeAggregatorFactory("a6", "hllsketch_dim1", null, null, null, ROUND)
                  )
              )
              .context(queryContext)
              .build()
              .withOverriddenContext(ImmutableMap.of("skipEmptyBuckets", true)),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }


  @Test
  public void testAvgDailyCountDistinctHllSketch() throws Exception
  {
    // Can't vectorize due to outer query, which runs on an inline datasource.
    cannotVectorize();

    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT\n"
                       + "  AVG(u)\n"
                       + "FROM ("
                       + "  SELECT FLOOR(__time TO DAY), APPROX_COUNT_DISTINCT_DS_HLL(cnt) AS u\n"
                       + "  FROM druid.foo\n"
                       + "  GROUP BY 1\n"
                       + ")";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        queryContext,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            1L
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Query expected = GroupByQuery.builder()
                                 .setDataSource(
                                     new QueryDataSource(
                                         Druids.newTimeseriesQueryBuilder()
                                               .dataSource(CalciteTests.DATASOURCE1)
                                               .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                               .granularity(new PeriodGranularity(Period.days(1), null, DateTimeZone.UTC))
                                               .aggregators(
                                                   Collections.singletonList(
                                                       new HllSketchBuildAggregatorFactory(
                                                           "a0:a",
                                                           "cnt",
                                                           null,
                                                           null,
                                                           null,
                                                           ROUND
                                                       )
                                                   )
                                               )
                                               .postAggregators(
                                                   ImmutableList.of(
                                                       new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                                   )
                                               )
                                               .context(queryContext)
                                               .build()
                                               .withOverriddenContext(
                                                   BaseCalciteQueryTest.getTimeseriesContextWithFloorTime(
                                                       ImmutableMap.of("skipEmptyBuckets", true, "sqlQueryId", "dummy"),
                                                       "d0"
                                                   )
                                               )
                                     )
                                 )
                                 .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                 .setGranularity(Granularities.ALL)
                                 .setAggregatorSpecs(
                                     NullHandling.replaceWithDefault()
                                     ? Arrays.asList(
                                         new LongSumAggregatorFactory("_a0:sum", "a0"),
                                         new CountAggregatorFactory("_a0:count")
                                     )
                                     : Arrays.asList(
                                         new LongSumAggregatorFactory("_a0:sum", "a0"),
                                         new FilteredAggregatorFactory(
                                             new CountAggregatorFactory("_a0:count"),
                                             BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("a0", null, null))
                                         )
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
                                         )
                                     )
                                 )
                                 .setContext(queryContext)
                                 .build();

    Query actual = Iterables.getOnlyElement(queryLogHook.getRecordedQueries());

    // Verify query
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testApproxCountDistinctHllSketchIsRounded() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT"
                       + "   dim2,"
                       + "   APPROX_COUNT_DISTINCT_DS_HLL(m1)"
                       + " FROM druid.foo"
                       + " GROUP BY dim2"
                       + " HAVING APPROX_COUNT_DISTINCT_DS_HLL(m1) = 2";

    // Verify results
    final List<Object[]> results =
        sqlLifecycle.runSimple(sql, queryContext, DEFAULT_PARAMETERS, authenticationResult).toList();
    final int expected = NullHandling.replaceWithDefault() ? 1 : 2;
    Assert.assertEquals(expected, results.size());
  }

  @Test
  public void testHllSketchPostAggsTimeseries() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT\n"
                       + "  DS_HLL(dim2),\n"
                       + "  DS_HLL(m1),\n"
                       + "  DS_HLL(cnt),\n"
                       + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)),\n"
                       + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)) + 1,\n"
                       + "  HLL_SKETCH_ESTIMATE(DS_HLL(CONCAT(dim2, 'hello'))),\n"
                       + "  ABS(HLL_SKETCH_ESTIMATE(DS_HLL(dim2))),\n"
                       + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2), 2),\n"
                       + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2)),\n"
                       + "  DS_HLL(POWER(ABS(m1 + 100), 2)),\n"
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2),\n"
                       + "  HLL_SKETCH_TO_STRING(DS_HLL(dim2)),\n"
                       + "  UPPER(HLL_SKETCH_TO_STRING(DS_HLL(dim2))),\n"
                       + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2), true)\n"
                       + "FROM druid.foo";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        queryContext,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults = ImmutableList.of(EXPECTED_PA_RESULT);

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
              .virtualColumns(VirtualColumns.create(EXPECTED_PA_VIRTUAL_COLUMNS))
              .aggregators(EXPECTED_PA_AGGREGATORS)
              .postAggregators(EXPECTED_PA_POST_AGGREGATORS)
              .context(queryContext)
              .build()
              .withOverriddenContext(ImmutableMap.of("skipEmptyBuckets", true));

    // Verify query
    Assert.assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testHllSketchPostAggsGroupBy() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT\n"
                       + "  DS_HLL(dim2),\n"
                       + "  DS_HLL(m1),\n"
                       + "  DS_HLL(cnt),\n"
                       + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)),\n"
                       + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2)) + 1,\n"
                       + "  HLL_SKETCH_ESTIMATE(DS_HLL(CONCAT(dim2, 'hello'))),\n"
                       + "  ABS(HLL_SKETCH_ESTIMATE(DS_HLL(dim2))),\n"
                       + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2), 2),\n"
                       + "  HLL_SKETCH_ESTIMATE_WITH_ERROR_BOUNDS(DS_HLL(dim2)),\n"
                       + "  DS_HLL(POWER(ABS(m1 + 100), 2)),\n"
                       + "  APPROX_COUNT_DISTINCT_DS_HLL(dim2),\n"
                       + "  HLL_SKETCH_TO_STRING(DS_HLL(dim2)),\n"
                       + "  UPPER(HLL_SKETCH_TO_STRING(DS_HLL(dim2))),\n"
                       + "  HLL_SKETCH_ESTIMATE(DS_HLL(dim2), true)\n"
                       + "FROM druid.foo\n"
                       + "GROUP BY cnt";


    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        queryContext,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults = ImmutableList.of(EXPECTED_PA_RESULT);

    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Assert.assertArrayEquals(expectedResults.get(i), results.get(i));
    }

    Query actualQuery = Iterables.getOnlyElement(queryLogHook.getRecordedQueries());

    Query expectedQuery =
        GroupByQuery.builder()
                    .setDataSource(CalciteTests.DATASOURCE1)
                    .setInterval(new MultipleIntervalSegmentSpec(Collections.singletonList(Filtration.eternity())))
                    .setGranularity(Granularities.ALL)
                    .setVirtualColumns(VirtualColumns.create(EXPECTED_PA_VIRTUAL_COLUMNS))
                    .setDimensions(new DefaultDimensionSpec("cnt", "d0", ValueType.LONG))
                    .setAggregatorSpecs(EXPECTED_PA_AGGREGATORS)
                    .setPostAggregatorSpecs(EXPECTED_PA_POST_AGGREGATORS)
                    .setContext(queryContext)
                    .build();

    // Verify query
    Assert.assertEquals(expectedQuery, actualQuery);
  }

  @Test
  public void testtHllSketchPostAggsPostSort() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT DS_HLL(dim2) as y FROM druid.foo ORDER BY HLL_SKETCH_ESTIMATE(DS_HLL(dim2)) DESC LIMIT 10";
    final String sql2 = StringUtils.format("SELECT HLL_SKETCH_ESTIMATE(y), HLL_SKETCH_TO_STRING(y) from (%s)", sql);

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql2,
        queryContext,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<Object[]> expectedResults = ImmutableList.of(
        new Object[]{
            2.000000004967054d,
            "### HLL SKETCH SUMMARY: \n"
              + "  Log Config K   : 12\n"
              + "  Hll Target     : HLL_4\n"
              + "  Current Mode   : LIST\n"
              + "  Memory         : false\n"
              + "  LB             : 2.0\n"
              + "  Estimate       : 2.000000004967054\n"
              + "  UB             : 2.000099863468538\n"
              + "  OutOfOrder Flag: false\n"
              + "  Coupon Count   : 2\n"
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
                      new HllSketchBuildAggregatorFactory(
                          "a0",
                          "dim2",
                          null,
                          null,
                          null,
                          true
                      )
                  )
              )
              .postAggregators(
                  ImmutableList.of(
                      new FieldAccessPostAggregator("p0", "a0"),
                      new HllSketchToEstimatePostAggregator("p2", new FieldAccessPostAggregator("p1", "a0"), false),
                      new HllSketchToEstimatePostAggregator("s1", new FieldAccessPostAggregator("s0", "p0"), false),
                      new HllSketchToStringPostAggregator("s3", new FieldAccessPostAggregator("s2", "p0"))
                  )
              )
              .context(queryContext)
              .build()
              .withOverriddenContext(ImmutableMap.of("skipEmptyBuckets", true));

    // Verify query
    Assert.assertEquals(expectedQuery, actualQuery);
  }

  private void cannotVectorize()
  {
    if (QueryContexts.Vectorize.fromString((String) queryContext.get(QueryContexts.VECTORIZE_KEY))
        == QueryContexts.Vectorize.FORCE) {
      expectedException.expectMessage("Cannot vectorize");
    }
  }
}
