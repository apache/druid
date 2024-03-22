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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.DruidProcessingConfig;
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
import java.util.stream.IntStream;

/**
 * Benchmark that tests various SQL queries.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class InPlanningBenchmark
{
  private static final Logger log = new Logger(InPlanningBenchmark.class);

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

  @Param({"500000"})
  private int rowsPerSegment;

  @Param({"0", "2147483647"})
  private int inSubQueryThreshold;

  @Param({
      "1", "10", "100", "1000", "10000", "100000", "1000000"
  })
  private Integer inClauseLiteralsCount;
  private SqlEngine engine;
  @Nullable
  private PlannerFactory plannerFactory;
  private final Closer closer = Closer.create();

  @Setup(Level.Trial)
  public void setup() throws JsonProcessingException
  {
    final GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("in-testbench");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .size(0)
                                               .build();

    final PlannerConfig plannerConfig = new PlannerConfig();

    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    log.info(
        "Starting benchmark setup using cacheDir[%s], rows[%,d], schema[auto].",
        segmentGenerator.getCacheDir(),
        rowsPerSegment
    );
    final QueryableIndex index;
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

    String prefix = ("explain plan for select long1 from foo where long1 in ");
    final String sql = createQuery(prefix, inClauseLiteralsCount);

    final Sequence<Object[]> resultSequence = getPlan(sql, null);
    final Object[] planResult = resultSequence.toList().get(0);
    if (inClauseLiteralsCount <= 100) {
      log.debug("Plan for query [ " + sql + " ]: \n" +
                jsonMapper.writerWithDefaultPrettyPrinter()
                          .writeValueAsString(jsonMapper.readValue((String) planResult[0], List.class))
      );
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryInSql(Blackhole blackhole)
  {
    String prefix = "explain plan for select long1 from foo where long1 in ";
    final String sql = createQuery(prefix, inClauseLiteralsCount);
    getPlan(sql, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryEqualOrInSql(Blackhole blackhole)
  {
    String prefix =
        "explain plan for select long1 from foo where string1 = '7' or long1 in ";
    final String sql = createQuery(prefix, inClauseLiteralsCount);
    getPlan(sql, blackhole);
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryMultiEqualOrInSql(Blackhole blackhole)
  {
    String prefix =
        "explain plan for select long1 from foo where string1 = '7' or string1 = '8' or long1 in ";
    final String sql = createQuery(prefix, inClauseLiteralsCount);
    getPlan(sql, blackhole);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryJoinEqualOrInSql(Blackhole blackhole)
  {

    String prefix =
        "explain plan for select foo.long1, fooright.string1 from foo inner join foo as fooright on foo.string1 = fooright.string1 where fooright.string1 = '7' or foo.long1 in ";
    final String sql = createQuery(prefix, inClauseLiteralsCount);
    getPlan(sql, blackhole);
  }

  private String createQuery(String prefix, int inClauseLiteralsCount)
  {
    StringBuilder sqlBuilder = new StringBuilder();
    sqlBuilder.append(prefix).append('(');
    IntStream.range(1, inClauseLiteralsCount - 1).forEach(i -> sqlBuilder.append(i).append(","));
    sqlBuilder.append(inClauseLiteralsCount).append(")");
    return sqlBuilder.toString();
  }

  private Sequence<Object[]> getPlan(String sql, @Nullable Blackhole blackhole)
  {
    final Map<String, Object> context = ImmutableMap.of(
        "inSubQueryThreshold", inSubQueryThreshold, "useCache", false);

    try (final DruidPlanner planner = plannerFactory.createPlannerForTesting(engine, sql, context)) {
      final PlannerResult plannerResult = planner.plan();
      final Sequence<Object[]> resultSequence = plannerResult.run().getResults();
      if (blackhole != null) {
        final Object[] lastRow = resultSequence.accumulate(null, (accumulated, in) -> in);
        blackhole.consume(lastRow);
      }
      return resultSequence;
    }
  }
}
