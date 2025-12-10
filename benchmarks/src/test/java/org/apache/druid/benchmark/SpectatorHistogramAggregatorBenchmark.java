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

package org.apache.druid.benchmark;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.benchmark.query.QueryBenchmarkUtil;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.spectator.histogram.SpectatorHistogramAggregatorFactory;
import org.apache.druid.spectator.histogram.SpectatorHistogramModule;
import org.apache.druid.timeline.SegmentId;
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
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark to compare performance of SpectatorHistogram aggregator with and without vectorization.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgs = {
    "--add-exports=java.base/jdk.internal.misc=ALL-UNNAMED",
    "--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/java.nio=ALL-UNNAMED",
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
    "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED",
    "--add-opens=java.base/java.io=ALL-UNNAMED",
    "--add-opens=java.base/java.lang=ALL-UNNAMED",
    "--add-opens=jdk.management/com.sun.management.internal=ALL-UNNAMED"
})
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class SpectatorHistogramAggregatorBenchmark
{
  private static final Logger log = new Logger(SpectatorHistogramAggregatorBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    // Register the SpectatorHistogram Jackson modules and serde
    SpectatorHistogramModule module = new SpectatorHistogramModule();
    for (Module jacksonModule : module.getJacksonModules()) {
      JSON_MAPPER.registerModule(jacksonModule);
    }
    SpectatorHistogramModule.registerSerde();

    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  @Param({"1000000"})
  private int rowsPerSegment;

  @Param({"false", "true"})
  private String vectorize;

  @Param({"long1"})
  private String metricName;

  private AppendableIndexSpec appendableIndexSpec;
  private AggregatorFactory spectatorHistogramFactory;
  private DataGenerator generator;
  private QueryRunnerFactory factory;
  private GeneratorSchemaInfo schemaInfo;
  private TimeseriesQuery query;

  /**
   * Setup everything common for benchmarking both the incremental-index and the queryable-index.
   */
  @Setup
  public void setup()
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");

    spectatorHistogramFactory = new SpectatorHistogramAggregatorFactory("spectatorHistogram", metricName);

    QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
        Collections.singletonList(schemaInfo.getDataInterval())
    );

    generator = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    query = Druids.newTimeseriesQueryBuilder()
                  .dataSource("blah")
                  .granularity(Granularities.ALL)
                  .intervals(intervalSpec)
                  .aggregators(Collections.singletonList(spectatorHistogramFactory))
                  .descending(false)
                  .build();

    factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(),
        new TimeseriesQueryEngine(),
        QueryBenchmarkUtil.NOOP_QUERYWATCHER
    );
  }

  /**
   * Setup/teardown everything specific for benchmarking the incremental-index.
   */
  @State(Scope.Benchmark)
  public static class IncrementalIndexState
  {
    IncrementalIndex incIndex;

    @Setup(Level.Invocation)
    public void setup(SpectatorHistogramAggregatorBenchmark global)
    {
      global.appendableIndexSpec = new OnheapIncrementalIndex.Spec();
      incIndex = global.makeIncIndex(global.spectatorHistogramFactory);
      global.generator.addToIndex(incIndex, global.rowsPerSegment);
    }

    @TearDown(Level.Invocation)
    public void tearDown()
    {
      if (incIndex != null) {
        incIndex.close();
      }
    }
  }

  /**
   * Setup/teardown everything specific for benchmarking the queryable-index.
   */
  @State(Scope.Benchmark)
  public static class QueryableIndexState
  {
    private File qIndexesDir;
    private QueryableIndex qIndex;

    @Setup
    public void setup(SpectatorHistogramAggregatorBenchmark global) throws IOException
    {
      global.appendableIndexSpec = new OnheapIncrementalIndex.Spec();

      IncrementalIndex incIndex = global.makeIncIndex(global.spectatorHistogramFactory);
      global.generator.addToIndex(incIndex, global.rowsPerSegment);

      qIndexesDir = FileUtils.createTempDir();
      log.info("Using temp dir: " + qIndexesDir.getAbsolutePath());

      File indexFile = INDEX_MERGER_V9.persist(
          incIndex,
          qIndexesDir,
          IndexSpec.getDefault(),
          null
      );
      incIndex.close();

      qIndex = INDEX_IO.loadIndex(indexFile);
    }

    @TearDown
    public void tearDown()
    {
      if (qIndex != null) {
        qIndex.close();
      }
      if (qIndexesDir != null) {
        qIndexesDir.delete();
      }
    }
  }

  private IncrementalIndex makeIncIndex(AggregatorFactory metric)
  {
    return appendableIndexSpec.builder()
                              .setSimpleTestingIndexSchema(metric)
                              .setMaxRowCount(rowsPerSegment)
                              .build();
  }

  private static <T> List<T> runQuery(
      QueryRunnerFactory factory,
      QueryRunner runner,
      Query<T> query,
      String vectorize
  )
  {
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    final QueryPlus<T> queryToRun = QueryPlus.wrap(
        query.withOverriddenContext(
            ImmutableMap.of(
                QueryContexts.VECTORIZE_KEY, vectorize,
                QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, vectorize
            )
        )
    );
    Sequence<T> queryResult = theRunner.run(queryToRun, ResponseContext.createEmpty());
    return queryResult.toList();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryIncrementalIndex(Blackhole blackhole, IncrementalIndexState state)
  {
    QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("incIndex"),
        new IncrementalIndexSegment(state.incIndex, SegmentId.dummy("incIndex"))
    );

    List<Result<TimeseriesResultValue>> results = runQuery(factory, runner, query, vectorize);
    blackhole.consume(results);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryQueryableIndex(Blackhole blackhole, QueryableIndexState state)
  {
    QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("qIndex"),
        new QueryableIndexSegment(state.qIndex, SegmentId.dummy("qIndex"))
    );

    List<Result<TimeseriesResultValue>> results = runQuery(factory, runner, query, vectorize);
    blackhole.consume(results);
  }

  public static void main(String[] args) throws RunnerException
  {
    Options opt = new OptionsBuilder()
        .include(SpectatorHistogramAggregatorBenchmark.class.getSimpleName())
        .forks(1)
        .warmupIterations(1)
        .measurementIterations(10)
        .build();
    new Runner(opt).run();
  }
}

