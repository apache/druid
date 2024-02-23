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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.transform.TransformSpec;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.sql.calcite.SqlVectorizedExpressionSanityTest;
import org.apache.druid.sql.calcite.planner.CalciteRulesManager;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.planner.DruidPlanner;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;
import org.apache.druid.sql.calcite.util.CalciteTests;
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
import java.util.stream.Collectors;

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
    ExpressionProcessing.initializeForTests();
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
      "SELECT string2, SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 2",
      // 26: group by string expr with non-expr agg
      "SELECT CONCAT(string2, '-', long2), SUM(double1) FROM foo GROUP BY 1 ORDER BY 2",
      // 27: group by string expr with expr agg
      "SELECT CONCAT(string2, '-', long2), SUM(long1 * double4) FROM foo GROUP BY 1 ORDER BY 2",
      // 28: group by single input string low cardinality expr with expr agg
      "SELECT CONCAT(string2, '-', 'foo'), SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 2",
      // 29: group by single input string high cardinality expr with expr agg
      "SELECT CONCAT(string3, '-', 'foo'), SUM(long1 * long4) FROM foo GROUP BY 1 ORDER BY 2",
      // 30: logical and operator
      "SELECT CAST(long1 as BOOLEAN) AND CAST (long2 as BOOLEAN), COUNT(*) FROM foo GROUP BY 1 ORDER BY 2",
      // 31: isnull, notnull
      "SELECT long5 IS NULL, long3 IS NOT NULL, count(*) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 32: time shift, non-expr col + reg agg, regular
      "SELECT TIME_SHIFT(__time, 'PT1H', 3), string2, SUM(double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 33: time shift, non-expr col + expr agg, sequential low cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long1), 'PT1H', 1), string2, SUM(long1 * double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 34: time shift + non-expr agg (timeseries) (non-expression reference), zipf distribution low cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long2), 'PT1H', 1), string2, SUM(long1 * double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 35: time shift + expr agg (timeseries), zipf distribution high cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long3), 'PT1H', 1), string2, SUM(long1 * double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 36: time shift + non-expr agg (group by), uniform distribution low cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long4), 'PT1H', 1), string2, SUM(long1 * double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 37: time shift + expr agg (group by), uniform distribution high cardinality
      "SELECT TIME_SHIFT(MILLIS_TO_TIMESTAMP(long5), 'PT1H', 1), string2, SUM(long1 * double4) FROM foo GROUP BY 1,2 ORDER BY 3",
      // 38,39: array element filtering
      "SELECT string1, long1 FROM foo WHERE ARRAY_CONTAINS(\"multi-string3\", 100) GROUP BY 1,2",
      "SELECT string1, long1 FROM foo WHERE ARRAY_OVERLAP(\"multi-string3\", ARRAY[100, 200]) GROUP BY 1,2",
      // 40: regex filtering
      "SELECT string4, COUNT(*) FROM foo WHERE REGEXP_EXTRACT(string1, '^1') IS NOT NULL OR REGEXP_EXTRACT('Z' || string2, '^Z2') IS NOT NULL GROUP BY 1"
  );

  @Param({"5000000"})
  private int rowsPerSegment;

  @Param({
      "false",
      "force"
  })
  private String vectorize;

  @Param({
      "explicit",
      "auto"
  })
  private String schema;

  @Param({
      // non-expression reference
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      // expressions, etc
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
      "25",
      "26",
      "27",
      "28",
      "29",
      "30",
      "31",
      "32",
      "33",
      "34",
      "35",
      "36",
      "37",
      "38",
      "39",
      "40"
  })
  private String query;

  private SqlEngine engine;
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
    log.info("Starting benchmark setup using cacheDir[%s], rows[%,d], schema[%s].", segmentGenerator.getCacheDir(), rowsPerSegment, schema);
    final QueryableIndex index;
    if ("auto".equals(schema)) {
      List<DimensionSchema> columnSchemas = schemaInfo.getDimensionsSpec()
                                                      .getDimensions()
                                                      .stream()
                                                      .map(x -> new AutoTypeColumnSchema(x.getName(), null))
                                                      .collect(Collectors.toList());
      index = segmentGenerator.generate(
          dataSegment,
          schemaInfo,
          DimensionsSpec.builder().setDimensions(columnSchemas).build(),
          TransformSpec.NONE,
          IndexSpec.DEFAULT,
          Granularities.NONE,
          rowsPerSegment
      );
    } else {
      index = segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment);
    }

    final QueryRunnerFactoryConglomerate conglomerate = QueryStackTests.createQueryRunnerFactoryConglomerate(
        closer,
        PROCESSING_CONFIG
    );

    final SpecificSegmentsQuerySegmentWalker walker = SpecificSegmentsQuerySegmentWalker.createWalker(conglomerate).add(
        dataSegment,
        index
    );
    closer.register(walker);
    final ObjectMapper jsonMapper = CalciteTests.getJsonMapper();
    final DruidSchemaCatalog rootSchema =
        CalciteTests.createMockRootSchema(conglomerate, walker, plannerConfig, AuthTestUtils.TEST_AUTHORIZER_MAPPER);
    engine = CalciteTests.createMockSqlEngine(walker, conglomerate);
    plannerFactory = new PlannerFactory(
        rootSchema,
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        jsonMapper,
        CalciteTests.DRUID_SCHEMA_NAME,
        new CalciteRulesManager(ImmutableSet.of()),
        CalciteTests.createJoinableFactoryWrapper(),
        CatalogResolver.NULL_RESOLVER,
        new AuthConfig()
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
    final String sql = QUERIES.get(Integer.parseInt(query));

    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, "EXPLAIN PLAN FOR " + sql, ImmutableMap.of("useNativeQueryExplain", true))) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Object[] planResult = resultSequence.toList().get(0);
      log.info("Native query plan:\n" +
               jsonMapper.writerWithDefaultPrettyPrinter()
                         .writeValueAsString(jsonMapper.readValue((String) planResult[0], List.class))
      );
    }
    catch (JsonMappingException e) {
      throw new RuntimeException(e);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }

    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, ImmutableMap.of())) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Yielder<Object[]> yielder = Yielders.each(resultSequence);
      int rowCounter = 0;
      while (!yielder.isDone()) {
        rowCounter++;
        yielder.next(yielder.get());
      }
      log.info("Total result row count:" + rowCounter);
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
  public void querySql(Blackhole blackhole)
  {
    final Map<String, Object> context = ImmutableMap.of(
        QueryContexts.VECTORIZE_KEY, vectorize,
        QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
    );
    final String sql = QUERIES.get(Integer.parseInt(query));
    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, context)) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
      blackhole.consume(lastRow);
    }
  }
}
