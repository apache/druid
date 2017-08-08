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
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.benchmark.datagen.BenchmarkDataGenerator;
import io.druid.benchmark.datagen.BenchmarkSchemaInfo;
import io.druid.benchmark.datagen.BenchmarkSchemas;
import io.druid.benchmark.query.QueryBenchmarkUtil;
import io.druid.collections.BlockingPool;
import io.druid.collections.DefaultBlockingPool;
import io.druid.collections.NonBlockingPool;
import io.druid.collections.StupidPool;
import io.druid.concurrent.Execs;
import io.druid.data.input.InputRow;
import io.druid.data.input.Row;
import io.druid.hll.HyperLogLogHash;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.logger.Logger;
import io.druid.offheap.OffheapBufferGenerator;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.groupby.GroupByQuery;
import io.druid.query.groupby.GroupByQueryConfig;
import io.druid.query.groupby.GroupByQueryEngine;
import io.druid.query.groupby.GroupByQueryQueryToolChest;
import io.druid.query.groupby.GroupByQueryRunnerFactory;
import io.druid.query.groupby.strategy.GroupByStrategySelector;
import io.druid.query.groupby.strategy.GroupByStrategyV1;
import io.druid.query.groupby.strategy.GroupByStrategyV2;
import io.druid.query.spec.MultipleIntervalSegmentSpec;
import io.druid.query.spec.QuerySegmentSpec;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.IndexSpec;
import io.druid.segment.QueryableIndex;
import io.druid.segment.QueryableIndexSegment;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.serde.ComplexMetrics;
import org.apache.commons.io.FileUtils;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

// Benchmark for determining the interface overhead of GroupBy with multiple type implementations

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 15)
@Measurement(iterations = 30)
public class GroupByTypeInterfaceBenchmark
{
  @Param({"4"})
  private int numSegments;

  @Param({"4"})
  private int numProcessingThreads;

  @Param({"-1"})
  private int initialBuckets;

  @Param({"100000"})
  private int rowsPerSegment;

  @Param({"v2"})
  private String defaultStrategy;

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

  private QueryRunnerFactory<Row, GroupByQuery> factory;

  private BenchmarkSchemaInfo schemaInfo;
  private GroupByQuery stringQuery;
  private GroupByQuery longFloatQuery;
  private GroupByQuery floatQuery;
  private GroupByQuery longQuery;

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

  private static final Map<String, Map<String, GroupByQuery>> SCHEMA_QUERY_MAP = new LinkedHashMap<>();

  private void setupQueries()
  {
    // queries for the basic schema
    Map<String, GroupByQuery> basicQueries = new LinkedHashMap<>();
    BenchmarkSchemaInfo basicSchema = BenchmarkSchemas.SCHEMA_MAP.get("basic");

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
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(Lists.<DimensionSpec>newArrayList(
              new DefaultDimensionSpec("dimSequential", null)
          ))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .build();

      GroupByQuery queryLongFloat = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(Lists.<DimensionSpec>newArrayList(
              new DefaultDimensionSpec("metLongUniform", null),
              new DefaultDimensionSpec("metFloatNormal", null)
          ))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .build();

      GroupByQuery queryLong = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(Lists.<DimensionSpec>newArrayList(
              new DefaultDimensionSpec("metLongUniform", null)
          ))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .build();

      GroupByQuery queryFloat = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(Lists.<DimensionSpec>newArrayList(
              new DefaultDimensionSpec("metFloatNormal", null)
          ))
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
          .setDimensions(Lists.<DimensionSpec>newArrayList(
              new DefaultDimensionSpec("dimSequential", null),
              new DefaultDimensionSpec("dimZipf", null)
          ))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularities.DAY)
          .build();

      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource(subqueryA)
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(Lists.<DimensionSpec>newArrayList(
              new DefaultDimensionSpec("dimSequential", null)
          ))
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

    if (ComplexMetrics.getSerdeForType("hyperUnique") == null) {
      ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde(HyperLogLogHash.getDefault()));
    }
    executorService = Execs.multiThreaded(numProcessingThreads, "GroupByThreadPool[%d]");

    setupQueries();

    String schemaName = "basic";

    schemaInfo = BenchmarkSchemas.SCHEMA_MAP.get(schemaName);
    stringQuery = SCHEMA_QUERY_MAP.get(schemaName).get("string");
    longFloatQuery = SCHEMA_QUERY_MAP.get(schemaName).get("longFloat");
    longQuery = SCHEMA_QUERY_MAP.get(schemaName).get("long");
    floatQuery = SCHEMA_QUERY_MAP.get(schemaName).get("float");

    final BenchmarkDataGenerator dataGenerator = new BenchmarkDataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED + 1,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

    tmpDir = Files.createTempDir();
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
          new IndexSpec()
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
      public String getDefaultStrategy()
      {
        return defaultStrategy;
      }

      @Override
      public int getBufferGrouperInitialBuckets()
      {
        return initialBuckets;
      }

      @Override
      public long getMaxOnDiskStorage()
      {
        return 1_000_000_000L;
      }
    };
    config.setSingleThreaded(false);
    config.setMaxIntermediateRows(Integer.MAX_VALUE);
    config.setMaxResults(Integer.MAX_VALUE);

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
    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            QueryBenchmarkUtil.NOOP_QUERYWATCHER,
            bufferPool
        ),
        new GroupByStrategyV2(
            druidProcessingConfig,
            configSupplier,
            bufferPool,
            mergePool,
            new ObjectMapper(new SmileFactory()),
            QueryBenchmarkUtil.NOOP_QUERYWATCHER
        )
    );

    factory = new GroupByQueryRunnerFactory(
        strategySelector,
        new GroupByQueryQueryToolChest(
            strategySelector,
            QueryBenchmarkUtil.NoopIntervalChunkingQueryRunnerDecorator()
        )
    );
  }

  private IncrementalIndex makeIncIndex()
  {
    return new IncrementalIndex.Builder()
        .setSimpleTestingIndexSchema(schemaInfo.getAggsArray())
        .setReportParseExceptions(false)
        .setConcurrentEventAdd(true)
        .setMaxRowCount(rowsPerSegment)
        .buildOnheap();
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
      throw Throwables.propagate(e);
    }
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
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongOnly(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatOnly(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexNumericOnly(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longFloatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexNumericThenString(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longFloatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongThenString(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongThenFloat(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringThenNumeric(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longFloatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringThenLong(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexStringTwice(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexLongTwice(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }


  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatTwice(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatThenLong(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, longQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndexFloatThenString(Blackhole blackhole) throws Exception
  {
    QueryRunner<Row> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    List<Row> results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, floatQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }

    runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        "qIndex",
        new QueryableIndexSegment("qIndex", queryableIndexes.get(0))
    );

    results = GroupByTypeInterfaceBenchmark.runQuery(factory, runner, stringQuery);

    for (Row result : results) {
      blackhole.consume(result);
    }
  }
}
