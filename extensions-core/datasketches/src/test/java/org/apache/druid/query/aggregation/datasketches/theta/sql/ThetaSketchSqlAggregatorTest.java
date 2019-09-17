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

package org.apache.druid.query.aggregation.datasketches.theta.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.Pair;
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
import org.apache.druid.query.aggregation.datasketches.theta.SketchMergeAggregatorFactory;
import org.apache.druid.query.aggregation.datasketches.theta.SketchModule;
import org.apache.druid.query.aggregation.post.ArithmeticPostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
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
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ThetaSketchSqlAggregatorTest extends CalciteTestBase
{
  private static final String DATA_SOURCE = "foo";

  @SuppressWarnings("SSBasedInspection")
  private static QueryRunnerFactoryConglomerate conglomerate;
  @SuppressWarnings("SSBasedInspection")
  private static Closer resourceCloser;
  private static final AuthenticationResult AUTHENTICATION_RESULT = CalciteTests.REGULAR_USER_AUTH_RESULT;
  private static final Map<String, Object> QUERY_CONTEXT_DEFAULT = ImmutableMap.of(
      PlannerContext.CTX_SQL_QUERY_ID, "dummy"
  );

  @BeforeClass
  public static void setUpClass()
  {
    final Pair<QueryRunnerFactoryConglomerate, Closer> conglomerateCloserPair = CalciteTests
        .createQueryRunnerFactoryConglomerate();
    conglomerate = conglomerateCloserPair.lhs;
    resourceCloser = conglomerateCloserPair.rhs;
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
    SketchModule.registerSerde();
    for (Module mod : new SketchModule().getJacksonModules()) {
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
                                                         new SketchMergeAggregatorFactory(
                                                             "thetasketch_dim1",
                                                             "dim1",
                                                             null,
                                                             false,
                                                             false,
                                                             null
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
                   .build(),
        index
    );

    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(conglomerate, walker, plannerConfig);
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig);
    final DruidOperatorTable operatorTable = new DruidOperatorTable(
        ImmutableSet.of(new ThetaSketchSqlAggregator()),
        ImmutableSet.of()
    );

    sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(
        new PlannerFactory(
            druidSchema,
            systemSchema,
            CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
            operatorTable,
            CalciteTests.createExprMacroTable(),
            plannerConfig,
            AuthTestUtils.TEST_AUTHORIZER_MAPPER,
            CalciteTests.getJsonMapper()
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
  public void testApproxCountDistinctThetaSketch() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "  SUM(cnt),\n"
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(dim2),\n" // uppercase
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(dim2) FILTER(WHERE dim2 <> ''),\n" // lowercase; also, filtered
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(SUBSTRING(dim2, 1, 1)),\n" // on extractionFn
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(SUBSTRING(dim2, 1, 1) || 'x'),\n" // on expression
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(thetasketch_dim1, 32768),\n" // on native theta sketch column
                       + "  APPROX_COUNT_DISTINCT_DS_THETA(thetasketch_dim1)\n" // on native theta sketch column
                       + "FROM druid.foo";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, AUTHENTICATION_RESULT).toList();
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
                      new SketchMergeAggregatorFactory(
                          "a1",
                          "dim2",
                          null,
                          null,
                          null,
                          null
                      ),
                      new FilteredAggregatorFactory(
                          new SketchMergeAggregatorFactory(
                              "a2",
                              "dim2",
                              null,
                              null,
                              null,
                              null
                          ),
                          BaseCalciteQueryTest.not(BaseCalciteQueryTest.selector("dim2", "", null))
                      ),
                      new SketchMergeAggregatorFactory(
                          "a3",
                          "v0",
                          null,
                          null,
                          null,
                          null
                      ),
                      new SketchMergeAggregatorFactory(
                          "a4",
                          "v1",
                          null,
                          null,
                          null,
                          null
                      ),
                      new SketchMergeAggregatorFactory("a5", "thetasketch_dim1", 32768, null, null, null),
                      new SketchMergeAggregatorFactory("a6", "thetasketch_dim1", null, null, null, null)
                  )
              )
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testAvgDailyCountDistinctThetaSketch() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();

    final String sql = "SELECT\n"
                       + "  AVG(u)\n"
                       + "FROM (SELECT FLOOR(__time TO DAY), APPROX_COUNT_DISTINCT_DS_THETA(cnt) AS u FROM druid.foo GROUP BY 1)";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(sql, QUERY_CONTEXT_DEFAULT, AUTHENTICATION_RESULT).toList();
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
                                         GroupByQuery.builder()
                                                     .setDataSource(CalciteTests.DATASOURCE1)
                                                     .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(
                                                         Filtration.eternity())))
                                                     .setGranularity(Granularities.ALL)
                                                     .setVirtualColumns(
                                                         new ExpressionVirtualColumn(
                                                             "v0",
                                                             "timestamp_floor(\"__time\",'P1D',null,'UTC')",
                                                             ValueType.LONG,
                                                             TestExprMacroTable.INSTANCE
                                                         )
                                                     )
                                                     .setDimensions(
                                                         Collections.singletonList(
                                                             new DefaultDimensionSpec(
                                                                 "v0",
                                                                 "v0",
                                                                 ValueType.LONG
                                                             )
                                                         )
                                                     )
                                                     .setAggregatorSpecs(
                                                         Collections.singletonList(
                                                             new SketchMergeAggregatorFactory(
                                                                 "a0:a",
                                                                 "cnt",
                                                                 null,
                                                                 null,
                                                                 null,
                                                                 null
                                                             )
                                                         )
                                                     )
                                                     .setPostAggregatorSpecs(
                                                         ImmutableList.of(
                                                             new FinalizingFieldAccessPostAggregator("a0", "a0:a")
                                                         )
                                                     )
                                                     .setContext(QUERY_CONTEXT_DEFAULT)
                                                     .build()
                                     )
                                 )
                                 .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                                 .setGranularity(Granularities.ALL)
                                 .setAggregatorSpecs(Arrays.asList(
                                     new LongSumAggregatorFactory("_a0:sum", "a0"),
                                     new CountAggregatorFactory("_a0:count")
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
                                         )
                                     )
                                 )
                                 .setContext(QUERY_CONTEXT_DEFAULT)
                                 .build();

    Query actual = Iterables.getOnlyElement(queryLogHook.getRecordedQueries());

    // Verify query
    Assert.assertEquals(expected, actual);
  }
}
