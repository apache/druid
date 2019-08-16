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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.Files;
import org.apache.druid.benchmark.datagen.BenchmarkDataGenerator;
import org.apache.druid.benchmark.datagen.BenchmarkSchemaInfo;
import org.apache.druid.benchmark.datagen.BenchmarkSchemas;
import org.apache.druid.benchmark.query.QueryBenchmarkUtil;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.offheap.OffheapBufferGenerator;
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
import org.apache.druid.query.aggregation.LongMaxAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.extraction.IdentityExtractionFn;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.topn.DimensionTopNMetricSpec;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.timeline.SegmentId;
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
import java.util.concurrent.TimeUnit;

// Benchmark for determining the interface overhead of TopN with multiple type implementations

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class TopNTypeInterfaceBenchmark
{
  private static final SegmentId Q_INDEX_SEGMENT_ID = SegmentId.dummy("qIndex");
  
  @Param({"1"})
  private int numSegments;

  @Param({"750000"})
  private int rowsPerSegment;

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
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
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

    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());

    setupQueries();

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get("basic");
    queryBuilder = SCHEMA_QUERY_MAP.get("basic").get("string");
    queryBuilder.threshold(threshold);
    stringQuery = queryBuilder.build();

    TopNQueryBuilder longBuilder = SCHEMA_QUERY_MAP.get("basic").get("long");
    longBuilder.threshold(threshold);
    longQuery = longBuilder.build();

    TopNQueryBuilder floatBuilder = SCHEMA_QUERY_MAP.get("basic").get("float");
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
          new IndexSpec(),
          null
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
        new TopNQueryQueryToolChest(
            new TopNQueryConfig(),
            QueryBenchmarkUtil.noopIntervalChunkingQueryRunnerDecorator()
        ),
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

    Sequence<T> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    return queryResult.toList();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringOnly(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringTwice(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringThenLong(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringThenFloat(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongOnly(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongTwice(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongThenString(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongThenFloat(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatOnly(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatTwice(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatThenString(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatThenLong(Blackhole blackhole)
  {
    QueryRunner<Result<TopNResultValue>> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<Result<TopNResultValue>> results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(qIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = TopNTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);
    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }
}
