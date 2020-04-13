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

package org.apache.druid.query.aggregation.tdigestsketch.sql;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryDataSource;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchAggregatorFactory;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchModule;
import org.apache.druid.query.aggregation.tdigestsketch.TDigestSketchToQuantilePostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
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
import java.util.stream.Stream;

public class TDigestSketchSqlAggregatorTest extends CalciteTestBase
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
    TDigestSketchModule.registerSerde();
    for (Module mod : new TDigestSketchModule().getJacksonModules()) {
      CalciteTests.getJsonMapper().registerModule(mod);
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
                                new TDigestSketchAggregatorFactory(
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
        ImmutableSet.of(new TDigestSketchQuantileSqlAggregator(), new TDigestGenerateSketchSqlAggregator()),
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
  public void testComputingSketchOnNumericValues() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "TDIGEST_GENERATE_SKETCH(m1, 200)"
                       + "FROM foo";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<String[]> expectedResults = ImmutableList.of(
        new String[]{
            "\"AAAAAT/wAAAAAAAAQBgAAAAAAABAaQAAAAAAAAAAAAY/8AAAAAAAAD/wAAAAAAAAP/AAAAAAAABAAAAAAAAAAD/wAAAAAAAAQAgAAAAAAAA/8AAAAAAAAEAQAAAAAAAAP/AAAAAAAABAFAAAAAAAAD/wAAAAAAAAQBgAAAAAAAA=\""
        }
    );

    Assert.assertEquals(expectedResults.size(), results.size());

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(ImmutableList.of(
                  new TDigestSketchAggregatorFactory("a0:agg", "m1", 200)
              ))
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testDefaultCompressionForTDigestGenerateSketchAgg() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "TDIGEST_GENERATE_SKETCH(m1)"
                       + "FROM foo";

    // Log query
    sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(ImmutableList.of(
                  new TDigestSketchAggregatorFactory("a0:agg", "m1", TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION)
              ))
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testComputingQuantileOnPreAggregatedSketch() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "TDIGEST_QUANTILE(qsketch_m1, 0.1),\n"
                       + "TDIGEST_QUANTILE(qsketch_m1, 0.4),\n"
                       + "TDIGEST_QUANTILE(qsketch_m1, 0.8),\n"
                       + "TDIGEST_QUANTILE(qsketch_m1, 1.0)\n"
                       + "FROM foo";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<double[]> expectedResults = ImmutableList.of(
        new double[]{
            1.1,
            2.9,
            5.3,
            6.0
        }
    );

    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Object[] objects = results.get(i);
      Assert.assertArrayEquals(
          expectedResults.get(i),
          Stream.of(objects).mapToDouble(value -> ((Double) value).doubleValue()).toArray(),
          0.000001
      );
    }

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(ImmutableList.of(
                  new TDigestSketchAggregatorFactory("a0:agg", "qsketch_m1", 100)
              ))
              .postAggregators(
                  new TDigestSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.1f),
                  new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a0:agg"), 0.4f),
                  new TDigestSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a0:agg"), 0.8f),
                  new TDigestSketchToQuantilePostAggregator("a3", makeFieldAccessPostAgg("a0:agg"), 1.0f)
              )
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testGeneratingSketchAndComputingQuantileOnFly() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT TDIGEST_QUANTILE(x, 0.0), TDIGEST_QUANTILE(x, 0.5), TDIGEST_QUANTILE(x, 1.0)\n"
                       + "FROM (SELECT dim1, TDIGEST_GENERATE_SKETCH(m1, 200) AS x FROM foo group by dim1)";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<double[]> expectedResults = ImmutableList.of(
        new double[]{
            1.0,
            3.5,
            6.0
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Object[] objects = results.get(i);
      Assert.assertArrayEquals(
          expectedResults.get(i),
          Stream.of(objects).mapToDouble(value -> ((Double) value).doubleValue()).toArray(),
          0.000001
      );
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
                                        .setDimensions(new DefaultDimensionSpec("dim1", "d0"))
                                        .setAggregatorSpecs(
                                            ImmutableList.of(
                                                new TDigestSketchAggregatorFactory("a0:agg", "m1", 200)
                                            )
                                        )
                                        .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                                        .build()
                        )
                    )
                    .setInterval(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
                    .setGranularity(Granularities.ALL)
                    .setAggregatorSpecs(
                        ImmutableList.of(
                            new TDigestSketchAggregatorFactory("_a0:agg", "a0:agg", 100)
                        )
                    )
                    .setPostAggregatorSpecs(
                        ImmutableList.of(
                            new TDigestSketchToQuantilePostAggregator("_a0", makeFieldAccessPostAgg("_a0:agg"), 0.0f),
                            new TDigestSketchToQuantilePostAggregator("_a1", makeFieldAccessPostAgg("_a0:agg"), 0.5f),
                            new TDigestSketchToQuantilePostAggregator("_a2", makeFieldAccessPostAgg("_a0:agg"), 1.0f)
                        )
                    )
                    .setContext(ImmutableMap.of(PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
                    .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testQuantileOnNumericValues() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "TDIGEST_QUANTILE(m1, 0.0), TDIGEST_QUANTILE(m1, 0.5), TDIGEST_QUANTILE(m1, 1.0)\n"
                       + "FROM foo";

    // Verify results
    final List<Object[]> results = sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();
    final List<double[]> expectedResults = ImmutableList.of(
        new double[]{
            1.0,
            3.5,
            6.0
        }
    );
    Assert.assertEquals(expectedResults.size(), results.size());
    for (int i = 0; i < expectedResults.size(); i++) {
      Object[] objects = results.get(i);
      Assert.assertArrayEquals(
          expectedResults.get(i),
          Stream.of(objects).mapToDouble(value -> ((Double) value).doubleValue()).toArray(),
          0.000001
      );
    }

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(ImmutableList.of(
                  new TDigestSketchAggregatorFactory("a0:agg", "m1", null)
              ))
              .postAggregators(
                  new TDigestSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.0f),
                  new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a0:agg"), 0.5f),
                  new TDigestSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a0:agg"), 1.0f)
              )
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }

  @Test
  public void testCompressionParamForTDigestQuantileAgg() throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final String sql = "SELECT\n"
                       + "TDIGEST_QUANTILE(m1, 0.0), TDIGEST_QUANTILE(m1, 0.5, 200), TDIGEST_QUANTILE(m1, 1.0, 300)\n"
                       + "FROM foo";

    // Log query
    sqlLifecycle.runSimple(
        sql,
        QUERY_CONTEXT_DEFAULT,
        DEFAULT_PARAMETERS,
        authenticationResult
    ).toList();

    // Verify query
    Assert.assertEquals(
        Druids.newTimeseriesQueryBuilder()
              .dataSource(CalciteTests.DATASOURCE1)
              .intervals(new MultipleIntervalSegmentSpec(ImmutableList.of(Filtration.eternity())))
              .granularity(Granularities.ALL)
              .aggregators(ImmutableList.of(
                  new TDigestSketchAggregatorFactory("a0:agg", "m1",
                                                     TDigestSketchAggregatorFactory.DEFAULT_COMPRESSION
                  ),
                  new TDigestSketchAggregatorFactory("a1:agg", "m1",
                                                     200
                  ),
                  new TDigestSketchAggregatorFactory("a2:agg", "m1",
                                                     300
                  )
              ))
              .postAggregators(
                  new TDigestSketchToQuantilePostAggregator("a0", makeFieldAccessPostAgg("a0:agg"), 0.0f),
                  new TDigestSketchToQuantilePostAggregator("a1", makeFieldAccessPostAgg("a1:agg"), 0.5f),
                  new TDigestSketchToQuantilePostAggregator("a2", makeFieldAccessPostAgg("a2:agg"), 1.0f)
              )
              .context(ImmutableMap.of("skipEmptyBuckets", true, PlannerContext.CTX_SQL_QUERY_ID, "dummy"))
              .build(),
        Iterables.getOnlyElement(queryLogHook.getRecordedQueries())
    );
  }


  private static PostAggregator makeFieldAccessPostAgg(String name)
  {
    return new FieldAccessPostAggregator(name, name);
  }
}
