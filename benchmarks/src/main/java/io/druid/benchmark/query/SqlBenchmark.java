/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.benchmark.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.benchmark.datagen.SegmentGenerator;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.Row;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.segment.QueryableIndex;
import io.druid.sql.calcite.planner.DruidPlanner;
import io.druid.sql.calcite.planner.PlannerConfig;
import io.druid.sql.calcite.planner.PlannerFactory;
import io.druid.sql.calcite.planner.PlannerResult;
import io.druid.sql.calcite.util.CalciteTests;
import io.druid.sql.calcite.util.SpecificSegmentsQuerySegmentWalker;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;
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
import java.util.ArrayList;
import java.util.Arrays;
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
  private static final int RNG_SEED = 9999;

  private File tmpDir;
  private SegmentGenerator segmentGenerator;
  private SpecificSegmentsQuerySegmentWalker walker;
  private PlannerFactory plannerFactory;
  private GroupByQuery groupByQuery;
  private String sqlQuery;

  @Setup(Level.Trial)
  public void setup() throws Exception
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

    final QueryableIndex index = segmentGenerator.generate(dataSegment, schemaInfo, rowsPerSegment);
    final QueryRunnerFactoryConglomerate conglomerate = CalciteTests.queryRunnerFactoryConglomerate();
    final PlannerConfig plannerConfig = new PlannerConfig();

    this.walker = new SpecificSegmentsQuerySegmentWalker(conglomerate).add(dataSegment, index);

    plannerFactory = new PlannerFactory(
        CalciteTests.createMockSchema(walker, plannerConfig),
        CalciteTests.createMockQueryLifecycleFactory(walker),
        CalciteTests.createOperatorTable(),
        CalciteTests.createExprMacroTable(),
        plannerConfig
    );
    groupByQuery = GroupByQuery
        .builder()
        .setDataSource("foo")
        .setInterval(new Interval(JodaUtils.MIN_INSTANT, JodaUtils.MAX_INSTANT))
        .setDimensions(
            Arrays.<DimensionSpec>asList(
                new DefaultDimensionSpec("dimZipf", "d0"),
                new DefaultDimensionSpec("dimSequential", "d1")
            )
        )
        .setAggregatorSpecs(Arrays.<AggregatorFactory>asList(new CountAggregatorFactory("c")))
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

    if (tmpDir != null) {
      FileUtils.deleteDirectory(tmpDir);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryNative(Blackhole blackhole) throws Exception
  {
    final Sequence<Row> resultSequence = groupByQuery.run(walker, Maps.<String, Object>newHashMap());
    final ArrayList<Row> resultList = Sequences.toList(resultSequence, Lists.<Row>newArrayList());

    for (Row row : resultList) {
      blackhole.consume(row);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void queryPlanner(Blackhole blackhole) throws Exception
  {
    try (final DruidPlanner planner = plannerFactory.createPlanner(null)) {
      final PlannerResult plannerResult = planner.plan(sqlQuery);
      final ArrayList<Object[]> results = Sequences.toList(plannerResult.run(), Lists.<Object[]>newArrayList());
      blackhole.consume(results);
    }
  }
}
