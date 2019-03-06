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

import com.google.common.io.Files;
import org.apache.commons.io.FileUtils;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.benchmark.datagen.SegmentGenerator;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.NoopEscalator;
import org.apache.druid.sql.SqlLifecycle;
import org.apache.druid.sql.SqlLifecycleFactory;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerFactory;
import org.apache.druid.sql.calcite.schema.DruidSchema;
import org.apache.druid.sql.calcite.schema.SystemSchema;
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

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark that compares the same groupBy query through the native query layer and through the SQL layer.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
public class SqlBenchmark
{
  @Param({"200000", "1000000"})
  private int rowsPerSegment;

  private static final Logger log = new Logger(SqlBenchmark.class);

  private File tmpDir;
  private SegmentGenerator segmentGenerator;
  private SpecificSegmentsQuerySegmentWalker walker;
  private SqlLifecycleFactory sqlLifecycleFactory;
  private GroupByQuery groupByQuery;
  private String sqlQuery;
  private Closer resourceCloser;

  @Setup(Level.Trial)
  public void setup()
  {
    tmpDir = Files.createTempDir();
    log.info("Starting benchmark setup using tmpDir[%s], rows[%,d].", tmpDir, rowsPerSegment);

    final BenchmarkSchemaInfo schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource("foo")
                                               .interval(schemaInfo.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(0))
                                               .build();

    this.segmentGenerator = new SegmentGenerator();

    final QueryableIndex index = segmentGenerator.generate(dataSegment, schemaInfo, Granularities.NONE, rowsPerSegment);
    final Pair<QueryRunnerFactoryConglomerate, Closer> conglomerateCloserPair = CalciteTests
        .createQueryRunnerFactoryConglomerate();
    final QueryRunnerFactoryConglomerate conglomerate = conglomerateCloserPair.lhs;
    final PlannerConfig plannerConfig = new PlannerConfig();
    final DruidSchema druidSchema = CalciteTests.createMockSchema(conglomerate, walker, plannerConfig);
    final SystemSchema systemSchema = CalciteTests.createMockSystemSchema(druidSchema, walker, plannerConfig);
    this.walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(dataSegment, index);
    final PlannerFactory plannerFactory = new PlannerFactory(
        druidSchema,
        systemSchema,
        CalciteTests.createMockQueryLifecycleFactory(walker, conglomerate),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        CalciteTests.getJsonMapper()
    );
    this.sqlLifecycleFactory = CalciteTests.createSqlLifecycleFactory(plannerFactory);
    groupByQuery = GroupByQuery
        .builder()
        .setDataSource("foo")
        .setInterval(Intervals.ETERNITY)
        .setDimensions(new DefaultDimensionSpec("dimZipf", "d0"), new DefaultDimensionSpec("dimSequential", "d1"))
        .setAggregatorSpecs(new CountAggregatorFactory("c"))
        .setGranularity(Granularities.ALL)
        .build();

    sqlQuery = "SELECT\n"
               + "  dimZipf AS d0,"
               + "  dimSequential AS d1,\n"
               + "  COUNT(*) AS c\n"
               + "FROM druid.foo\n"
               + "GROUP BY dimZipf, dimSequential";
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    if (walker != null) {
      walker.close();
      walker = null;
    }

    if (segmentGenerator != null) {
      segmentGenerator.close();
      segmentGenerator = null;
    }

    if (resourceCloser != null) {
      resourceCloser.close();
    }

    if (tmpDir != null) {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryNative(Blackhole blackhole)
  {
    final Sequence<Row> resultSequence = QueryPlus.wrap(groupByQuery).run(walker, new HashMap<>());
    final List<Row> resultList = resultSequence.toList();
    blackhole.consume(resultList);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryPlanner(Blackhole blackhole) throws Exception
  {
    SqlLifecycle sqlLifecycle = sqlLifecycleFactory.factorize();
    final List<Object[]> results = sqlLifecycle.runSimple(
        sqlQuery,
        null,
        NoopEscalator.getInstance().createEscalatedAuthenticationResult()
    ).toList();
    blackhole.consume(results);
  }
}
