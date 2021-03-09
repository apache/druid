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

package org.apache.druid.benchmark.query;

import org.apache.druid.com.google.common.collect.ImmutableList;
import org.apache.druid.com.google.common.collect.ImmutableMap;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.SqlVectorizedExpressionSanityTest;
import org.apache.druid.sql.calcite.planner.Calcites;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.util.CalciteTests;
import org.apache.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlExpressionBenchmark
{
  private static final Logger log = new Logger(SqlExpressionBenchmark.class);

  static {
    NullHandling.initializeForTests();
    Calcites.setSystemProperties();
  }

  private static final DruidProcessingConfig PROCESSING_CONFIG = new DruidProcessingConfig()
  {
    @Override
    public int intermediateComputeSizeBytes()
    {
      return 512 * 1024 * 1024;
    }

    @Override
    public int getNumMergeBuffers()
    {
      return 3;
    }

    @Override
    public int getNumThreads()
    {
      return 1;
    }

    @Override
    public boolean useParallelMergePoolConfigured()
    {
      return true;
    }

    @Override
    public String getFormatString()
    {
      return "benchmarks-processing-%s";
    }
  };


  private static final List<String> QUERIES = ImmutableList.of(
      // ===========================
      // non-expression reference queries
      // ===========================
      // 0: non-expression timeseries reference, 1 columns
      "SELECT SUM(long1) FROM foo",
      // 1: non-expression timeseries reference, 2 columns
      "SELECT SUM(long1), SUM(long2) FROM foo",
      // 2: non-expression timeseries reference, 3 columns
      "SELECT SUM(long1), SUM(long4), SUM(double1) FROM foo",
      // 3: non-expression timeseries reference, 4 columns
      "SELECT SUM(long1), SUM(long4), SUM(double1), SUM(float3) FROM foo",
      // 4: non-expression timeseries reference, 5 columns
      "SELECT SUM(long1), SUM(long4), SUM(double1), SUM(float3), SUM(long5) FROM foo",
      // 5: group by non-expr with 1 agg
      "SELECT string2, SUM(long1) FROM foo GROUP BY 1 ORDER BY 2",
      // 6: group by non-expr with 2 agg
      "SELECT string2, SUM(long1), SUM(double3) FROM foo GROUP BY 1 ORDER BY 2",
      // ===========================
      // expressions
      // ===========================
      // 7: math op - 2 longs
      "SELECT SUM(long1 * long2) FROM foo",
      // 8: mixed math - 2 longs, 1 double
      "SELECT SUM((long1 * long2) / double1) FROM foo",
      // 9: mixed math - 2 longs, 1 double, 1 float
      "SELECT SUM(float3 + ((long1 * long4)/double1)) FROM foo",
      // 10: mixed math - 3 longs, 1 double, 1 float
      "SELECT SUM(long5 - (float3 + ((long1 * long4)/double1))) FROM foo",
      // 11: all same math op - 3 longs, 1 double, 1 float
      "SELECT SUM(long5 * float3 * long1 * long4 * double1) FROM foo",
      // 12: cos
      "SELECT cos(double2) FROM foo",
      // 13: unary negate
      "SELECT SUM(-long4) FROM foo",
      // 14: string long
      "SELECT SUM(PARSE_LONG(string1)) FROM foo",
      // 15: string longer
      "SELECT SUM(PARSE_LONG(string3)) FROM foo",
      // 16: time floor, non-expr col + reg agg
      "SELECT TIME_FLOOR(__time, 'PT1H'), string2, SUM(double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 17: time floor, non-expr col + expr agg
      "SELECT TIME_FLOOR(__time, 'PT1H'), string2, SUM(long1 * double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 18: time floor + non-expr agg (timeseries) (non-expression reference)
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1) FROM foo GROUP BY 1 ORDER BY 1",
      // 19: time floor + expr agg (timeseries)
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 1",
      // 20: time floor + non-expr agg (group by)
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1) FROM foo GROUP BY 1 ORDER BY 2",
      // 21: time floor + expr agg (group by)
      "SELECT TIME_FLOOR(__time, 'PT1H'), SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 2",
      // 22: time floor offset by 1 day + non-expr agg (group by)
      "SELECT TIME_FLOOR(TIMESTAMPADD(DAY, -1, __time), 'PT1H'), SUM(long1) FROM foo GROUP BY 1 ORDER BY 1",
      // 23: time floor offset by 1 day + expr agg (group by)
      "SELECT TIME_FLOOR(TIMESTAMPADD(DAY, -1, __time), 'PT1H'), SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 1",
      // 24: group by long expr with non-expr agg
      "SELECT (long1 * long2), SUM(double1) FROM foo GROUP BY 1 ORDER BY 2",
      // 25: group by non-expr with expr agg
      "SELECT string2, SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 2"
  );

  @Param({"5000000"})
  private int rowsPerSegment;

  @Param({"false", "force"})
  private String vectorize;

  @Param({
      // non-expression reference
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      // expressions
      "7",
      "8",
      "9",
      "10",
      "11",
      "12",
      "13",
      "14",
      "15",
      "16",
      "17",
      "18",
      "19",
      "20",
      "21",
      "22",
      "23",
      "24",
      "25"
  })
  private String query;

  @Nullable
  private PlannerFactory plannerFactory;
  private Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup()
  {
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("expression-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final PlannerConfig plannerConfig = new PlannerConfig();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    log.info("Starting benchmark setup using cacheDir[%s], rows[%,d].", segmentGenerator.getCacheDir(), rowsPerSegment);
    final QueryableIndex index = segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment);

    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
        closer,
        PROCESSING_CONFIG
    );

    final SpecificSegmentsQuerySegmentWalker walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(
        dataSegment,
        index
    );
    closer.register(walker);

    final SchemaPlus rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper(),
        CalciteTests.DRUID_SCHEMA_NAME
    );

    try {
      SqlVectorizedExpressionSanityTest.sanityTestVectorizedSqlQueries(
          plannerFactory,
          QUERIES.get(Integer.parseInt(query))
      );
    }
    catch (Throwable ignored) {
      // the show must go on
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void querySql(Blackhole blackhole) throws Exception
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
    final String sql = QUERIES.get(Integer.parseInt(query));
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(context, sql)) {
      final PlannerResult plannerResult = planner.plan(sql);
      final Sequence<Object[]> resultSequence = plannerResult.run();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }
}
