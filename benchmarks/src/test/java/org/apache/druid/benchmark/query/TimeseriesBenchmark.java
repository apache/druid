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

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExpressionProcessing;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.FilteredAggregatorFactory;
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(
    value = 1,
    jvmArgsAppend = {
        "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.nio=ALL-UNNAMED",
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-opens=java.base/jdk.internal.ref=ALL-UNNAMED"
    }
)
@Warmup(iterations = 5)
@Measurement(iterations = 15)
public class TimeseriesBenchmark
{
  @Param({"750000"})
  private int rowsPerSegment;

  @Param({
      "basic.A",
      "basic.timeFilterNumeric",
      "basic.timeFilterAlphanumeric",
      "basic.timeFilterByInterval",
      "basic.orFilterPartialIndex"
  })
  private String schemaAndQuery;

  @Param({"true", "false"})
  private boolean descending;

  @Param({"false", "true"})
  private String vectorize;

  @Param({"all", "hour"})
  private String queryGranularity;

  private static final Logger log = new Logger(TimeseriesBenchmark.class);

  private QueryRunnerFactory factory;
  private GeneratorSchemaInfo schemaInfo;
  private TimeseriesQuery query;

  private static final Map<String, Map<String, TimeseriesQuery>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    Map<String, TimeseriesQuery> basicQueries = new LinkedHashMap<>();
    GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");

    { // basic.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));
      queryAggs.add(new LongMaxAggregatorFactory("maxLongUniform", "maxLongUniform"));
      queryAggs.add(new DoubleSumAggregatorFactory("sumFloatNormal", "sumFloatNormal"));
      queryAggs.add(new DoubleMinAggregatorFactory("minFloatZipf", "minFloatZipf"));
      queryAggs.add(new HyperUniquesAggregatorFactory("hyperUniquesMet", "hyper"));

      TimeseriesQuery queryA =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(Granularity.fromString(queryGranularity))
                .intervals(intervalSpec)
                .aggregators(queryAggs)
                .descending(descending)
                .context(Map.of("vectorize", vectorize))
                .build();

      basicQueries.put("A", queryA);
    }
    {
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      LongSumAggregatorFactory lsaf = new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential");
      BoundDimFilter timeFilter = new BoundDimFilter(ColumnHolder.TIME_COLUMN_NAME, "200000", "300000", false, false, null, null,
                                                     StringComparators.NUMERIC);
      queryAggs.add(new FilteredAggregatorFactory(lsaf, timeFilter));

      TimeseriesQuery timeFilterQuery =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(Granularity.fromString(queryGranularity))
                .intervals(intervalSpec)
                .aggregators(queryAggs)
                .descending(descending)
                .context(Map.of("vectorize", vectorize))
                .build();

      basicQueries.put("timeFilterNumeric", timeFilterQuery);
    }
    {
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      LongSumAggregatorFactory lsaf = new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential");
      BoundDimFilter timeFilter = new BoundDimFilter(ColumnHolder.TIME_COLUMN_NAME, "200000", "300000", false, false, null, null,
                                                     StringComparators.ALPHANUMERIC);
      queryAggs.add(new FilteredAggregatorFactory(lsaf, timeFilter));

      TimeseriesQuery timeFilterQuery =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(Granularity.fromString(queryGranularity))
                .intervals(intervalSpec)
                .aggregators(queryAggs)
                .descending(descending)
                .context(Map.of("vectorize", vectorize))
                .build();

      basicQueries.put("timeFilterAlphanumeric", timeFilterQuery);
    }
    {
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(Intervals.utc(200000, 300000)));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      LongSumAggregatorFactory lsaf = new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential");
      queryAggs.add(lsaf);

      TimeseriesQuery timeFilterQuery =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(Granularity.fromString(queryGranularity))
                .intervals(intervalSpec)
                .aggregators(queryAggs)
                .descending(descending)
                .context(Map.of("vectorize", vectorize))
                .build();

      basicQueries.put("timeFilterByInterval", timeFilterQuery);
    }
    {
      // One clause has a bitmap index, the other is a numeric column and has none. OrFilter therefore unions the
      // indexable clauses into a partial index and converts that into a value matcher, which walks a bitmap iterator
      // alongside the cursor offset. See OrFilter#convertIndexToVectorValueMatcher.
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));

      DimFilter orFilter = new OrDimFilter(
          new SelectorDimFilter("dimSequential", "311", null),
          new BoundDimFilter("maxLongUniform", "100", null, false, false, null, null, StringComparators.NUMERIC)
      );

      TimeseriesQuery orFilterQuery =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(Granularity.fromString(queryGranularity))
                .intervals(intervalSpec)
                .filters(orFilter)
                .aggregators(queryAggs)
                .descending(descending)
                .context(Map.of("vectorize", vectorize))
                .build();

      basicQueries.put("orFilterPartialIndex", orFilterQuery);
    }


    SCHEMA_QUERY_MAP.put("basic", basicQueries);
  }

  /**
   * Setup everything common for benchmarking both the incremental-index and the queriable-index.
   */
  @Setup
  public void setup()
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    ExpressionProcessing.initializeForTests();
    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    setupQueries();

    String[] schemaQuery = schemaAndQuery.split("\\.");
    String schemaName = schemaQuery[0];
    String queryName = schemaQuery[1];

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schemaName);
    query = SCHEMA_QUERY_MAP.get(schemaName).get(queryName);

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
    private SegmentGenerator segmentGenerator;

    IncrementalIndex incIndex;

    @Setup
    public void setup(TimeseriesBenchmark global)
    {
      segmentGenerator = new SegmentGenerator();
      incIndex = segmentGenerator.generateIncrementalIndex(
          global.makeDataSegment(0),
          global.schemaInfo,
          Granularities.NONE,
          global.rowsPerSegment
      );
    }

    @TearDown
    public void tearDown() throws IOException
    {
      if (incIndex != null) {
        incIndex.close();
      }
      if (segmentGenerator != null) {
        segmentGenerator.close();
      }
    }
  }

  /**
   * Setup/teardown everything specific for benchmarking the queriable-index.
   */
  @State(Scope.Benchmark)
  public static class QueryableIndexState
  {
    @Param({"1"})
    private int numSegments;

    private ExecutorService executorService;
    private SegmentGenerator segmentGenerator;
    private List<QueryableIndex> qIndexes;

    @Setup
    public void setup(TimeseriesBenchmark global)
    {
      executorService = Execs.multiThreaded(numSegments, "TimeseriesThreadPool");

      segmentGenerator = new SegmentGenerator();
      qIndexes = new ArrayList<>();

      for (int i = 0; i < numSegments; i++) {
        log.info("Generating rows for segment " + i);

        qIndexes.add(
            segmentGenerator.generate(
                global.makeDataSegment(i),
                global.schemaInfo,
                Granularities.NONE,
                global.rowsPerSegment
            )
        );
      }
    }

    @TearDown
    public void tearDown() throws IOException
    {
      for (QueryableIndex index : qIndexes) {
        if (index != null) {
          index.close();
        }
      }
      if (segmentGenerator != null) {
        segmentGenerator.close();
      }
    }
  }

  private DataSegment makeDataSegment(final int segmentNumber)
  {
    return DataSegment.builder()
                      .dataSource("blah")
                      .interval(schemaInfo.getDataInterval())
                      .version("1")
                      .shardSpec(new LinearShardSpec(segmentNumber))
                      .size(0)
                      .build();
  }

  private static <T> List<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query)
  {
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    Sequence<T> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    return queryResult.toList();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleIncrementalIndex(Blackhole blackhole, IncrementalIndexState state)
  {
    QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("incIndex"),
        new IncrementalIndexSegment(state.incIndex, SegmentId.dummy("incIndex"))
    );

    List<Result<TimeseriesResultValue>> results = TimeseriesBenchmark.runQuery(factory, runner, query);
    blackhole.consume(results);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndex(Blackhole blackhole, QueryableIndexState state)
  {
    final QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("qIndex"),
        new QueryableIndexSegment(state.qIndexes.get(0), SegmentId.dummy("qIndex"))
    );

    List<Result<TimeseriesResultValue>> results = TimeseriesBenchmark.runQuery(factory, runner, query);
    blackhole.consume(results);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryFilteredSingleQueryableIndex(Blackhole blackhole, QueryableIndexState state)
  {
    final QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("qIndex"),
        new QueryableIndexSegment(state.qIndexes.get(0), SegmentId.dummy("qIndex"))
    );

    DimFilter filter = new SelectorDimFilter("dimSequential", "399", null);
    Query filteredQuery = query.withDimFilter(filter);

    List<Result<TimeseriesResultValue>> results = TimeseriesBenchmark.runQuery(factory, runner, filteredQuery);
    blackhole.consume(results);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndex(Blackhole blackhole, QueryableIndexState state)
  {
    List<QueryRunner<Result<TimeseriesResultValue>>> singleSegmentRunners = new ArrayList<>();
    QueryToolChest toolChest = factory.getToolchest();
    for (int i = 0; i < state.numSegments; i++) {
      SegmentId segmentId = SegmentId.dummy("qIndex " + i);
      QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
          factory,
          segmentId,
          new QueryableIndexSegment(state.qIndexes.get(i), segmentId)
      );
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }

    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(state.executorService, singleSegmentRunners)),
            toolChest
        )
    );

    Sequence<Result<TimeseriesResultValue>> queryResult = theRunner.run(
        QueryPlus.wrap(query),
        ResponseContext.createEmpty()
    );
    List<Result<TimeseriesResultValue>> results = queryResult.toList();

    blackhole.consume(results);
  }
}
