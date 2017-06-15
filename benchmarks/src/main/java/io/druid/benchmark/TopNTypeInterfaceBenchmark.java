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

package io.druid.benchmark;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.benchmark.query.QueryBenchmarkUtil;
import io.druid.collections.StupidPool;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.hll.HyperLogLogHash;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.offheap.OffheapBufferGenerator;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleMinAggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongMaxAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.IdentityExtractionFn;
import io.druid.query.ordering.StringComparators;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.query.topn.DimensionTopNMetricSpec;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNQueryRunnerFactory;
import io.druid.query.topn.TopNResultValue;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
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
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

// Benchmark for determining the interface overhead of TopN with multiple type implementations

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class TopNTypeInterfaceBenchmark
{
  @Param({"1"})
  private int numSegments;

  @Param({"750000"})
  private int rowsPerSegment;

  @Param({"basic.A"})
  private String schemaAndQuery;

  @Param({"10"})
  private int threshold;

  private static final Logger log = new Logger(TopNTypeInterfaceBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private List<IncrementalIndex> incIndexes;
  private List<QueryableIndex> qIndexes;

  private QueryRunnerFactory factory;
  private BenchmarkSchemaInfo schemaInfo;
  private TopNQueryBuilder queryBuilder;
  private TopNQuery stringQuery;
  private TopNQuery longQuery;
  private TopNQuery floatQuery;

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

  private static final Map<String, Map<String, TopNQueryBuilder>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    Map<String, TopNQueryBuilder> basicQueries = new LinkedHashMap<>();
    BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");

    { // basic.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));
      queryAggs.add(new LongMaxAggregatorFactory("maxLongUniform", "maxLongUniform"));
      queryAggs.add(new DoubleSumAggregatorFactory("sumFloatNormal", "sumFloatNormal"));
      queryAggs.add(new DoubleMinAggregatorFactory("minFloatZipf", "minFloatZipf"));
      queryAggs.add(new HyperUniquesAggregatorFactory("hyperUniquesMet", "hyper"));

      // Use an IdentityExtractionFn to force usage of DimExtractionTopNAlgorithm
      TopNQueryBuilder queryBuilderString = new TopNQueryBuilder()
          .dataSource("blah")
          .granularity(Granularities.ALL)
          .dimension(new ExtractionDimensionSpec("dimSequential", "dimSequential", IdentityExtractionFn.getInstance()))
          .metric("sumFloatNormal")
          .intervals(intervalSpec)
          .aggregators(queryAggs);

      // DimExtractionTopNAlgorithm is always used for numeric columns
      TopNQueryBuilder queryBuilderLong = new TopNQueryBuilder()
          .dataSource("blah")
          .granularity(Granularities.ALL)
          .dimension("metLongUniform")
          .metric("sumFloatNormal")
          .intervals(intervalSpec)
          .aggregators(queryAggs);

      TopNQueryBuilder queryBuilderFloat = new TopNQueryBuilder()
          .dataSource("blah")
          .granularity(Granularities.ALL)
          .dimension("metFloatNormal")
          .metric("sumFloatNormal")
          .intervals(intervalSpec)
          .aggregators(queryAggs);

      basicQueries.put("string", queryBuilderString);
      basicQueries.put("long", queryBuilderLong);
      basicQueries.put("float", queryBuilderFloat);
    }
    { // basic.numericSort
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));

      TopNQueryBuilder queryBuilderA = new TopNQueryBuilder()
          .dataSource("blah")
          .granularity(Granularities.ALL)
          .dimension("dimUniform")
          .metric(new DimensionTopNMetricSpec(null, StringComparators.NUMERIC))
          .intervals(intervalSpec)
          .aggregators(queryAggs);

      basicQueries.put("numericSort", queryBuilderA);
    }
    { // basic.alphanumericSort
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));

      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));

      TopNQueryBuilder queryBuilderA = new TopNQueryBuilder()
          .dataSource("blah")
          .granularity(Granularities.ALL)
          .dimension("dimUniform")
          .metric(new DimensionTopNMetricSpec(null, StringComparators.ALPHANUMERIC))
          .intervals(intervalSpec)
          .aggregators(queryAggs);

      basicQueries.put("alphanumericSort", queryBuilderA);
    }

    SCHEMA_QUERY_MAP.put("basic", basicQueries);
  }


  @Setup
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT " + System.currentTimeMillis());

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));
    }

    executorService = Execs.multiThreaded(numSegments, "TopNThreadPool");

    setupQueries();

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get("basic");
    queryBuilder = SCHEMA_QUERY_MAP.get("basic").get("string");
    queryBuilder.threshold(threshold);
    stringQuery = queryBuilder.build();

    TopNQueryBuilder longBuilder =  SCHEMA_QUERY_MAP.get("basic").get("long");
    longBuilder.threshold(threshold);
    longQuery = longBuilder.build();

    TopNQueryBuilder floatBuilder =  SCHEMA_QUERY_MAP.get("basic").get("float");
    floatBuilder.threshold(threshold);
    floatQuery = floatBuilder.build();

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

    factory = new TopNQueryRunnerFactory(
        new StupidPool<>(
            "TopNBenchmark-compute-bufferPool",
            new OffheapBufferGenerator("compute", 250000000),
            0,
            Integer.MAX_VALUE
        ),
        new TopNQueryQueryToolChest(new TopNQueryConfig(), QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()),
        QueryBenchmarkUtil.NOOP_QUERYWATCHER
    );
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(schemaInfo.getAggsArray())
        .setReportParseExceptions(false)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
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
  public void querySingleQueryableIndexStringOnly(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringTwice(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringThenLong(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringThenFloat(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongOnly(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongTwice(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongThenString(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongThenFloat(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatOnly(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatTwice(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatThenString(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatThenLong(Blackhole blackhole) throws Exception
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", qIndexes.get(0))
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }
}
