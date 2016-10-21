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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.Files;

import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.Druids;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.FilteredAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.filter.BoundDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryEngine;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.segment.IncrementalIndexSegment;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import org.joda.time.Interval;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(jvmArgsPrepend = "-server", value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class TimeseriesBenchmark
{
  @Param({"1"})
  private int numSegments;

  @Param({"750000"})
  private int rowsPerSegment;

  @Param({"basic.A", "basic.timeFilterNumeric", "basic.timeFilterAlphanumeric", "basic.timeFilterByInterval"})
  private String schemaAndQuery;

  private static final Logger log = new Logger(TimeseriesBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private List<IncrementalIndex> incIndexes;
  private List<QueryableIndex> qIndexes;

  private QueryRunnerFactory factory;
  private BenchmarkSchemaInfo schemaInfo;
  private TimeseriesQuery query;

  private ExecutorService executorService;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
          @Override
          public int columnCacheSizeBytes()
          {
            return 0;
          }
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO);
  }

  private static final Map<String, Map<String, TimeseriesQuery>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    Map<String, TimeseriesQuery> basicQueries = new LinkedHashMap<>();
    BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    { // basic.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Arrays.asList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));
      queryAggs.add(new LongMaxAggregatorFactory("maxLongUniform", "maxLongUniform"));
      queryAggs.add(new DoubleSumAggregatorFactory("sumFloatNormal", "sumFloatNormal"));
      queryAggs.add(new DoubleMinAggregatorFactory("minFloatZipf", "minFloatZipf"));
      queryAggs.add(new HyperUniquesAggregatorFactory("hyperUniquesMet", "hyper"));

      TimeseriesQuery queryA =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(QueryGranularities.ALL)
                .intervals(intervalSpec)
                .aggregators(queryAggs)
                .descending(false)
                .build();

      basicQueries.put("A", queryA);
    }
    {
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Arrays.asList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      LongSumAggregatorFactory lsaf = new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential");
      BoundDimFilter timeFilter = new BoundDimFilter(Column.TIME_COLUMN_NAME, "200000", "300000", false, false, null, null,
                                                     StringComparators.NUMERIC);
      queryAggs.add(new FilteredAggregatorFactory(lsaf, timeFilter));

      TimeseriesQuery timeFilterQuery =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(QueryGranularities.ALL)
                .intervals(intervalSpec)
                .aggregators(queryAggs)
                .descending(false)
                .build();

      basicQueries.put("timeFilterNumeric", timeFilterQuery);
    }
    {
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Arrays.asList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      LongSumAggregatorFactory lsaf = new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential");
      BoundDimFilter timeFilter = new BoundDimFilter(Column.TIME_COLUMN_NAME, "200000", "300000", false, false, null, null,
                                                     StringComparators.ALPHANUMERIC);
      queryAggs.add(new FilteredAggregatorFactory(lsaf, timeFilter));

      TimeseriesQuery timeFilterQuery =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(QueryGranularities.ALL)
                .intervals(intervalSpec)
                .aggregators(queryAggs)
                .descending(false)
                .build();

      basicQueries.put("timeFilterAlphanumeric", timeFilterQuery);
    }
    {
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Arrays.asList(new Interval(200000, 300000)));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      LongSumAggregatorFactory lsaf = new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential");
      queryAggs.add(lsaf);

      TimeseriesQuery timeFilterQuery =
          Druids.newTimeseriesQueryBuilder()
                .dataSource("blah")
                .granularity(QueryGranularities.ALL)
                .intervals(intervalSpec)
                .aggregators(queryAggs)
                .descending(false)
                .build();

      basicQueries.put("timeFilterByInterval", timeFilterQuery);
    }


    SCHEMA_QUERY_MAP.put("basic", basicQueries);
  }

  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(Hashing.murmur3_128()));
    }

    executorService = Execs.multiThreaded(numSegments, "TimeseriesThreadPool");

    setupQueries();

    String[] schemaQuery = schemaAndQuery.split("\\.");
    String schemaName = schemaQuery[0];
    String queryName = schemaQuery[1];

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schemaName);
    query = SCHEMA_QUERY_MAP.get(schemaName).get(queryName);

    incIndexes = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      log.info("Generating rows for segment " + i);
      BenchmarkDataGenerator gen = new BenchmarkDataGenerator(
          schemaInfo.getColumnSchemas(),
          RNG_SEED + i,
          schemaInfo.getDataInterval(),
          rowsPerSegment
      );

      IncrementalIndex incIndex = makeIncIndex();

      for (int j = 0; j < rowsPerSegment; j++) {
        InputRow row = gen.nextRow();
        if (j % 10000 == 0) {
          log.info(j + " rows generated.");
        }
        incIndex.add(row);
      }
      log.info(rowsPerSegment + " rows generated");
      incIndexes.add(incIndex);
    }

    File tmpFile = Files.createTempDir();
    log.info("Using temp dir: " + tmpFile.getAbsolutePath());
    tmpFile.deleteOnExit();

    qIndexes = new ArrayList<>();
    for (int i = 0; i < numSegments; i++) {
      File indexFile = INDEX_MERGER_V9.persist(
          incIndexes.get(i),
          tmpFile,
          new IndexSpec()
      );

      QueryableIndex qIndex = INDEX_IO.loadIndex(indexFile);
      qIndexes.add(qIndex);
    }

    factory = new TimeseriesQueryRunnerFactory(
        new TimeseriesQueryQueryToolChest(
            QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()
        ),
        new TimeseriesQueryEngine(),
        QueryBenchmarkUtil.NOOP_QUERYWATCHER
    );
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex(
        new IncrementalIndexSchema.Builder()
            .withQueryGranularity(QueryGranularities.NONE)
            .withMetrics(schemaInfo.getAggsArray())
            .withDimensionsSpec(new DimensionsSpec(null, null, null))
            .build(),
        true,
        false,
        true,
        rowsPerSegment
    );
  }

  private static <T> List<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query)
  {
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    Sequence<T> queryResult = theRunner.run(query, Maps.<String, Object>newHashMap());
    return Sequences.toList(queryResult, Lists.<T>newArrayList());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleIncrementalIndex(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "incIndex",
        new IncrementalIndexSegment(incIndexes.get(0), "incIndex")
    );

    List<Result<TimeseriesResultValue>> results = TimeseriesBenchmark.runQuery(factory, runner, query);
    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndex(Blackhole blackhole) throws Exception
  {
    final QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TimeseriesResultValue>> results = TimeseriesBenchmark.runQuery(factory, runner, query);
    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryFilteredSingleQueryableIndex(Blackhole blackhole) throws Exception
  {
    final QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    DimFilter filter = new SelectorDimFilter("dimSequential", "399", null);
    Query filteredQuery = query.withDimFilter(filter);

    List<Result<TimeseriesResultValue>> results = TimeseriesBenchmark.runQuery(factory, runner, filteredQuery);
    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndex(Blackhole blackhole) throws Exception
  {
    List<QueryRunner<Result<TimeseriesResultValue>>> singleSegmentRunners = Lists.newArrayList();
    QueryToolChest toolChest = factory.getToolchest();
    for (int i = 0; i < numSegments; i++) {
      String segmentName = "qIndex" + i;
      QueryRunner<Result<TimeseriesResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
          factory,
          segmentName,
          new QueryableIndexSegment(segmentName, qIndexes.get(i))
      );
      singleSegmentRunners.add(toolChest.preMergeQueryDecoration(runner));
    }

    QueryRunner theRunner = toolChest.postMergeQueryDecoration(
        new FinalizeResultsQueryRunner<>(
            toolChest.mergeResults(factory.mergeRunners(executorService, singleSegmentRunners)),
            toolChest
        )
    );

    Sequence<Result<TimeseriesResultValue>> queryResult = theRunner.run(query, Maps.<String, Object>newHashMap());
    List<Result<TimeseriesResultValue>> results = Sequences.toList(queryResult, Lists.<Result<TimeseriesResultValue>>newArrayList());

    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }
}
