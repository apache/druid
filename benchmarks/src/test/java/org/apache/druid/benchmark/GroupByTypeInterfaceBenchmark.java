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
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.apache.druid.benchmark.query.QueryBenchmarkUtil;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.OnheapIncrementalIndex;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Benchmark for determining the interface overhead of GroupBy with multiple type implementations
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
public class GroupByTypeInterfaceBenchmark
{
  static {
    NullHandling.initializeForTests();
  }

  private static final SegmentId Q_INDEX_SEGMENT_ID = SegmentId.dummy("qIndex");

  @Param({"4"})
  private int numSegments;

  @Param({"4"})
  private int numProcessingThreads;

  @Param({"-1"})
  private int initialBuckets;

  @Param({"100000"})
  private int rowsPerSegment;

  @Param({"all"})
  private String queryGranularity;

  private static final Logger log = new Logger(GroupByTypeInterfaceBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  private File tmpDir;
  private IncrementalIndex anIncrementalIndex;
  private List<QueryableIndex> queryableIndexes;

  private QueryRunnerFactory<ResultRow, GroupByQuery> factory;

  private GeneratorSchemaInfo schemaInfo;
  private GroupByQuery stringQuery;
  private GroupByQuery longFloatQuery;
  private GroupByQuery floatQuery;
  private GroupByQuery longQuery;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER,
        new ColumnConfig()
        {
        }
    );
    INDEX_MERGER_V9 = new IndexMergerV9(JSON_MAPPER, INDEX_IO, OffHeapMemorySegmentWriteOutMediumFactory.instance());
  }

  private static final Map<String, Map<String, GroupByQuery>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    Map<String, GroupByQuery> basicQueries = new LinkedHashMap<>();
    GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");

    { // basic.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory(
          "sumLongSequential",
          "sumLongSequential"
      ));
      GroupByQuery queryString = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec).setDimensions(new DefaultDimensionSpec("dimSequential", null))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .build();

      GroupByQuery queryLongFloat = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(
              new DefaultDimensionSpec("metLongUniform", null),
              new DefaultDimensionSpec("metFloatNormal", null)
          )
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .build();

      GroupByQuery queryLong = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec).setDimensions(new DefaultDimensionSpec("metLongUniform", null))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .build();

      GroupByQuery queryFloat = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec).setDimensions(new DefaultDimensionSpec("metFloatNormal", null))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .build();

      basicQueries.put("string", queryString);
      basicQueries.put("longFloat", queryLongFloat);
      basicQueries.put("long", queryLong);
      basicQueries.put("float", queryFloat);
    }

    { // basic.nested
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory(
          "sumLongSequential",
          "sumLongSequential"
      ));

      GroupByQuery subqueryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimSequential", null), new DefaultDimensionSpec("dimZipf", null))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularities.DAY)
          .build();

      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource(subqueryA)
          .setQuerySegmentSpec(intervalSpec).setDimensions(new DefaultDimensionSpec("dimSequential", null))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularities.WEEK)
          .build();

      basicQueries.put("nested", queryA);
    }

    SCHEMA_QUERY_MAP.put("basic", basicQueries);
  }

  @Setup(Level.Trial)
  public void setup() throws IOException
  {
    log.info("SETUP CALLED AT %d", System.currentTimeMillis());

    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    setupQueries();

    String schemaName = "basic";

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schemaName);
    stringQuery = SCHEMA_QUERY_MAP.get(schemaName).get("string");
    longFloatQuery = SCHEMA_QUERY_MAP.get(schemaName).get("longFloat");
    longQuery = SCHEMA_QUERY_MAP.get(schemaName).get("long");
    floatQuery = SCHEMA_QUERY_MAP.get(schemaName).get("float");

    final DataGenerator dataGenerator = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED + 1,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    tmpDir = FileUtils.createTempDir();
    log.info("Using temp dir: %s", tmpDir.getAbsolutePath());

    // queryableIndexes   -> numSegments worth of on-disk segments
    // anIncrementalIndex -> the last incremental index
    anIncrementalIndex = null;
    queryableIndexes = new ArrayList<>(numSegments);

    for (int i = 0; i < numSegments; i++) {
      log.info("Generating rows for segment %d/%d", i + 1, numSegments);

      final IncrementalIndex index = makeIncIndex();

      for (int j = 0; j < rowsPerSegment; j++) {
        final InputRow row = dataGenerator.nextRow();
        if (j % 20000 == 0) {
          log.info("%,d/%,d rows generated.", i * rowsPerSegment + j, rowsPerSegment * numSegments);
        }
        index.add(row);
      }

      log.info(
          "%,d/%,d rows generated, persisting segment %d/%d.",
          (i + 1) * rowsPerSegment,
          rowsPerSegment * numSegments,
          i + 1,
          numSegments
      );

      final File file = INDEX_MERGER_V9.persist(
          index,
          new File(tmpDir, String.valueOf(i)),
          IndexSpec.DEFAULT,
          null
      );

      queryableIndexes.add(INDEX_IO.loadIndex(file));

      if (i == numSegments - 1) {
        anIncrementalIndex = index;
      } else {
        index.close();
      }
    }

    NonBlockingPool<ByteBuffer> bufferPool = new StupidPool<>(
        "GroupByBenchmark-computeBufferPool",
        new OffheapBufferGenerator("compute", 250_000_000),
        0,
        Integer.MAX_VALUE
    );

    // limit of 2 is required since we simulate both historical merge and broker merge in the same process
    BlockingPool<ByteBuffer> mergePool = new DefaultBlockingPool<>(
        new OffheapBufferGenerator("merge", 250_000_000),
        2
    );
    final GroupByQueryConfig config = new GroupByQueryConfig()
    {

      @Override
      public int getBufferGrouperInitialBuckets()
      {
        return initialBuckets;
      }

      @Override
      public HumanReadableBytes getMaxOnDiskStorage()
      {
        return HumanReadableBytes.valueOf(1_000_000_000L);
      }
    };
    config.setSingleThreaded(false);

    DruidProcessingConfig druidProcessingConfig = new DruidProcessingConfig()
    {
      @Override
      public int getNumThreads()
      {
        // Used by "v2" strategy for concurrencyHint
        return numProcessingThreads;
      }

      @Override
      public String getFormatString()
      {
        return null;
      }
    };

    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final GroupingEngine groupingEngine = new GroupingEngine(
        druidProcessingConfig,
        configSupplier,
        bufferPool,
        mergePool,
        TestHelper.makeJsonMapper(),
        new ObjectMapper(new SmileFactory()),
        QueryBenchmarkUtil.NOOP_QUERYWATCHER
    );

    factory = new GroupByQueryRunnerFactory(
        groupingEngine,
        new GroupByQueryQueryToolChest(groupingEngine)
    );
  }

  private IncrementalIndex makeIncIndex()
  {
    return new OnheapIncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(schemaInfo.getAggsArray())
        .setMaxRowCount(rowsPerSegment)
        .build();
  }

  @TearDown(Level.Trial)
  public void tearDown()
  {
    try {
      if (anIncrementalIndex != null) {
        anIncrementalIndex.close();
      }

      if (queryableIndexes != null) {
        for (QueryableIndex index : queryableIndexes) {
          index.close();
        }
      }

      if (tmpDir != null) {
        FileUtils.deleteDirectory(tmpDir);
      }
    }
    catch (IOException e) {
      log.warn(e, "Failed to tear down, temp dir was: %s", tmpDir);
      throw new RuntimeException(e);
    }
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
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongOnly(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatOnly(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexNumericOnly(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longFloatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexNumericThenString(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longFloatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongThenString(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongThenFloat(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringThenNumeric(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longFloatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringThenLong(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringTwice(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongTwice(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatTwice(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatThenLong(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatThenString(Blackhole blackhole)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    List<ResultRow> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        Q_INDEX_SEGMENT_ID,
        new QueryableIndexSegment(queryableIndexes.get(0), Q_INDEX_SEGMENT_ID)
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }
}
