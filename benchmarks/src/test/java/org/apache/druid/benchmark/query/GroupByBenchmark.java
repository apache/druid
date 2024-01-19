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
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleMinAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.filter.BoundDimFilter;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupingEngine;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.orderby.DefaultLimitSpec;
import org.apache.druid.query.groupby.orderby.OrderByColumnSpec;
import org.apache.druid.query.ordering.StringComparators;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.segment.IncrementalIndexSegment;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.generator.DataGenerator;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexCreator;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
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
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 2)
@Warmup(iterations = 10)
@Measurement(iterations = 25)
public class GroupByBenchmark
{
  @Param({"2", "4"})
  private int numProcessingThreads;

  @Param({"-1"})
  private int initialBuckets;

  @Param({"100000"})
  private int rowsPerSegment;

  @Param({"basic.A", "basic.nested"})
  private String schemaAndQuery;

  @Param({"all", "day"})
  private String queryGranularity;

  @Param({"force", "false"})
  private String vectorize;

  private static final Logger log = new Logger(GroupByBenchmark.class);
  private static final int RNG_SEED = 9999;
  private static final IndexMergerV9 INDEX_MERGER_V9;
  private static final IndexIO INDEX_IO;
  public static final ObjectMapper JSON_MAPPER;

  static {
    NullHandling.initializeForTests();
  }

  private AppendableIndexSpec appendableIndexSpec;
  private DataGenerator generator;
  private QueryRunnerFactory<ResultRow, GroupByQuery> factory;
  private GeneratorSchemaInfo schemaInfo;
  private GroupByQuery query;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    INDEX_IO = new IndexIO(
        JSON_MAPPER.setInjectableValues(
            new InjectableValues.Std()
                .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
                .addValue(ObjectMapper.class.getName(), JSON_MAPPER)
        ),
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
      queryAggs.add(new CountAggregatorFactory("cnt"));
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimSequential", null), new DefaultDimensionSpec("dimZipf", null))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularity.fromString(queryGranularity))
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      basicQueries.put("A", queryA);
    }

    { // basic.sorted
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(basicSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimSequential", null), new DefaultDimensionSpec("dimZipf", null))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularity.fromString(queryGranularity))
          .setLimitSpec(
              new DefaultLimitSpec(
                  Collections.singletonList(
                      new OrderByColumnSpec(
                          "sumLongSequential",
                          OrderByColumnSpec.Direction.DESCENDING,
                          StringComparators.NUMERIC
                      )
                  ),
                  100
              )
          )
          .build();

      basicQueries.put("sorted", queryA);
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
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularities.DAY)
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource(subqueryA)
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimSequential", null))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularities.WEEK)
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      basicQueries.put("nested", queryA);
    }

    { // basic.filter
      final QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
          Collections.singletonList(basicSchema.getDataInterval())
      );
      // Use multiple aggregators to see how the number of aggregators impact to the query performance
      List<AggregatorFactory> queryAggs = ImmutableList.of(
          new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"),
          new LongSumAggregatorFactory("rows", "rows"),
          new DoubleSumAggregatorFactory("sumFloatNormal", "sumFloatNormal"),
          new DoubleMinAggregatorFactory("minFloatZipf", "minFloatZipf")
      );
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimUniform", null))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularity.fromString(queryGranularity))
          .setDimFilter(new BoundDimFilter("dimUniform", "0", "100", true, true, null, null, null))
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      basicQueries.put("filter", queryA);
    }

    { // basic.singleZipf
      final QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(
          Collections.singletonList(basicSchema.getDataInterval())
      );
      // Use multiple aggregators to see how the number of aggregators impact to the query performance
      List<AggregatorFactory> queryAggs = ImmutableList.of(
          new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"),
          new LongSumAggregatorFactory("rows", "rows"),
          new DoubleSumAggregatorFactory("sumFloatNormal", "sumFloatNormal"),
          new DoubleMinAggregatorFactory("minFloatZipf", "minFloatZipf")
      );
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimZipf", null))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularity.fromString(queryGranularity))
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      basicQueries.put("singleZipf", queryA);
    }
    SCHEMA_QUERY_MAP.put("basic", basicQueries);

    // simple one column schema, for testing performance difference between querying on numeric values as Strings and
    // directly as longs
    Map<String, GroupByQuery> simpleQueries = new LinkedHashMap<>();
    GeneratorSchemaInfo simpleSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("simple");

    { // simple.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(simpleSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory(
          "rows",
          "rows"
      ));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimSequential", "dimSequential", ColumnType.STRING))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      simpleQueries.put("A", queryA);
    }
    SCHEMA_QUERY_MAP.put("simple", simpleQueries);


    Map<String, GroupByQuery> simpleLongQueries = new LinkedHashMap<>();
    GeneratorSchemaInfo simpleLongSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("simpleLong");
    { // simpleLong.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(simpleLongSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory(
          "rows",
          "rows"
      ));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimSequential", "dimSequential", ColumnType.LONG))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      simpleLongQueries.put("A", queryA);
    }
    SCHEMA_QUERY_MAP.put("simpleLong", simpleLongQueries);


    Map<String, GroupByQuery> simpleFloatQueries = new LinkedHashMap<>();
    GeneratorSchemaInfo simpleFloatSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("simpleFloat");
    { // simpleFloat.A
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(simpleFloatSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new LongSumAggregatorFactory(
          "rows",
          "rows"
      ));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("dimSequential", "dimSequential", ColumnType.FLOAT))
          .setAggregatorSpecs(queryAggs)
          .setGranularity(Granularity.fromString(queryGranularity))
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      simpleFloatQueries.put("A", queryA);
    }
    SCHEMA_QUERY_MAP.put("simpleFloat", simpleFloatQueries);

    // simple one column schema, for testing performance difference between querying on numeric values as Strings and
    // directly as longs
    Map<String, GroupByQuery> nullQueries = new LinkedHashMap<>();
    GeneratorSchemaInfo nullSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("nulls");

    { // simple-null
      QuerySegmentSpec intervalSpec = new MultipleIntervalSegmentSpec(Collections.singletonList(nullSchema.getDataInterval()));
      List<AggregatorFactory> queryAggs = new ArrayList<>();
      queryAggs.add(new DoubleSumAggregatorFactory(
          "doubleSum",
          "doubleZipf"
      ));
      GroupByQuery queryA = GroupByQuery
          .builder()
          .setDataSource("blah")
          .setQuerySegmentSpec(intervalSpec)
          .setDimensions(new DefaultDimensionSpec("stringZipf", "stringZipf", ColumnType.STRING))
          .setAggregatorSpecs(
              queryAggs
          )
          .setGranularity(Granularity.fromString(queryGranularity))
          .setContext(ImmutableMap.of("vectorize", vectorize))
          .build();

      nullQueries.put("A", queryA);
    }
    SCHEMA_QUERY_MAP.put("nulls", nullQueries);
  }

  /**
   * Setup everything common for benchmarking both the incremental-index and the queriable-index.
   */
  @Setup(Level.Trial)
  public void setup()
  {
    log.info("SETUP CALLED AT " + +System.currentTimeMillis());

    ComplexMetrics.registerSerde(HyperUniquesSerde.TYPE_NAME, new HyperUniquesSerde());

    setupQueries();

    String[] schemaQuery = schemaAndQuery.split("\\.");
    String schemaName = schemaQuery[0];
    String queryName = schemaQuery[1];

    schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schemaName);
    query = SCHEMA_QUERY_MAP.get(schemaName).get(queryName);

    generator = new DataGenerator(
        schemaInfo.getColumnSchemas(),
        RNG_SEED,
        schemaInfo.getDataInterval(),
        rowsPerSegment
    );

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

  /**
   * Setup/teardown everything specific for benchmarking the incremental-index.
   */
  @State(Scope.Benchmark)
  public static class IncrementalIndexState
  {
    @Param({"onheap", "offheap"})
    private String indexType;

    IncrementalIndex incIndex;

    @Setup(Level.Trial)
    public void setup(GroupByBenchmark global) throws JsonProcessingException
    {
      // Creates an AppendableIndexSpec that corresponds to the indexType parametrization.
      // It is used in {@code global.makeIncIndex()} to instanciate an incremental-index of the specified type.
      global.appendableIndexSpec = IncrementalIndexCreator.parseIndexType(indexType);
      incIndex = global.makeIncIndex(global.schemaInfo.isWithRollup());
      global.generator.addToIndex(incIndex, global.rowsPerSegment);
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
      if (incIndex != null) {
        incIndex.close();
      }
    }
  }

  /**
   * Setup/teardown everything specific for benchmarking the queriable-index.
   */
  @State(Scope.Benchmark)
  public static class QueryableIndexState
  {
    @Param({"4"})
    private int numSegments;

    private ExecutorService executorService;
    private File qIndexesDir;
    private List<QueryableIndex> queryableIndexes;

    @Setup(Level.Trial)
    public void setup(GroupByBenchmark global) throws IOException
    {
      global.appendableIndexSpec = new OnheapIncrementalIndex.Spec();

      executorService = Execs.multiThreaded(global.numProcessingThreads, "GroupByThreadPool[%d]");

      qIndexesDir = FileUtils.createTempDir();

      // numSegments worth of on-disk segments
      queryableIndexes = new ArrayList<>();

      for (int i = 0; i < numSegments; i++) {
        log.info("Generating rows for segment %d/%d", i + 1, numSegments);

        final IncrementalIndex incIndex = global.makeIncIndex(global.schemaInfo.isWithRollup());
        global.generator.reset(RNG_SEED + i).addToIndex(incIndex, global.rowsPerSegment);

        log.info(
            "%,d/%,d rows generated, persisting segment %d/%d.",
            (i + 1) * global.rowsPerSegment,
            global.rowsPerSegment * numSegments,
            i + 1,
            numSegments
        );

        File indexFile = INDEX_MERGER_V9.persist(
            incIndex,
            new File(qIndexesDir, String.valueOf(i)),
            IndexSpec.DEFAULT,
            null
        );
        incIndex.close();

        queryableIndexes.add(INDEX_IO.loadIndex(indexFile));
      }
    }

    @TearDown(Level.Trial)
    public void tearDown()
    {
      for (QueryableIndex index : queryableIndexes) {
        if (index != null) {
          index.close();
        }
      }
      if (qIndexesDir != null) {
        qIndexesDir.delete();
      }
    }
  }

  private IncrementalIndex makeIncIndex(boolean withRollup)
  {
    return appendableIndexSpec.builder()
        .setIndexSchema(
            new IncrementalIndexSchema.Builder()
                .withDimensionsSpec(schemaInfo.getDimensionsSpec())
                .withMetrics(schemaInfo.getAggsArray())
                .withRollup(withRollup)
                .build()
        )
        .setMaxRowCount(rowsPerSegment)
        .build();
  }

  private static <T> Sequence<T> runQuery(QueryRunnerFactory factory, QueryRunner runner, Query<T> query)
  {
    QueryToolChest toolChest = factory.getToolchest();
    QueryRunner<T> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(toolChest.preMergeQueryDecoration(runner)),
        toolChest
    );

    return theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleIncrementalIndex(Blackhole blackhole, IncrementalIndexState state)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("incIndex"),
        new IncrementalIndexSegment(state.incIndex, SegmentId.dummy("incIndex"))
    );

    final Sequence<ResultRow> results = GroupByBenchmark.runQuery(factory, runner, query);
    final ResultRow lastRow = results.accumulate(
        null,
        (accumulated, in) -> in
    );

    blackhole.consume(lastRow);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void querySingleQueryableIndex(Blackhole blackhole, QueryableIndexState state)
  {
    QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
        factory,
        SegmentId.dummy("qIndex"),
        new QueryableIndexSegment(state.queryableIndexes.get(0), SegmentId.dummy("qIndex"))
    );

    final Sequence<ResultRow> results = GroupByBenchmark.runQuery(factory, runner, query);
    final ResultRow lastRow = results.accumulate(
        null,
        (accumulated, in) -> in
    );

    blackhole.consume(lastRow);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndexX(Blackhole blackhole, QueryableIndexState state)
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = factory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(state.executorService, makeMultiRunners(state))
        ),
        (QueryToolChest) toolChest
    );

    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();
    blackhole.consume(results);
  }

  /**
   * Measure the time to produce the first ResultRow unlike {@link #queryMultiQueryableIndexX} measures total query
   * processing time. This measure is useful since the Broker can start merging as soon as the first result is returned.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndexTTFR(Blackhole blackhole, QueryableIndexState state) throws IOException
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = factory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(state.executorService, makeMultiRunners(state))
        ),
        (QueryToolChest) toolChest
    );

    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    Yielder<ResultRow> yielder = Yielders.each(queryResult);
    yielder.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndexWithSpilling(Blackhole blackhole, QueryableIndexState state)
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = factory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(state.executorService, makeMultiRunners(state))
        ),
        (QueryToolChest) toolChest
    );

    final GroupByQuery spillingQuery = query.withOverriddenContext(
        ImmutableMap.of("bufferGrouperMaxSize", 4000)
    );
    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(spillingQuery), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();
    blackhole.consume(results);
  }

  /**
   * Measure the time to produce the first ResultRow unlike {@link #queryMultiQueryableIndexWithSpilling} measures
   * total query processing time. This measure is useful since the Broker can start merging as soon as the first
   * result is returned.
   */
  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndexWithSpillingTTFR(Blackhole blackhole, QueryableIndexState state) throws IOException
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = factory.getToolchest();
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            factory.mergeRunners(state.executorService, makeMultiRunners(state))
        ),
        (QueryToolChest) toolChest
    );

    final GroupByQuery spillingQuery = query.withOverriddenContext(
        ImmutableMap.of("bufferGrouperMaxSize", 4000)
    );
    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(spillingQuery), ResponseContext.createEmpty());
    Yielder<ResultRow> yielder = Yielders.each(queryResult);
    yielder.close();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void queryMultiQueryableIndexWithSerde(Blackhole blackhole, QueryableIndexState state)
  {
    QueryToolChest<ResultRow, GroupByQuery> toolChest = factory.getToolchest();
    //noinspection unchecked
    QueryRunner<ResultRow> theRunner = new FinalizeResultsQueryRunner<>(
        toolChest.mergeResults(
            new SerializingQueryRunner<>(
                new DefaultObjectMapper(new SmileFactory(), null),
                ResultRow.class,
                toolChest.mergeResults(
                    factory.mergeRunners(state.executorService, makeMultiRunners(state))
                )
            )
        ),
        (QueryToolChest) toolChest
    );

    Sequence<ResultRow> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());
    List<ResultRow> results = queryResult.toList();
    blackhole.consume(results);
  }

  private List<QueryRunner<ResultRow>> makeMultiRunners(QueryableIndexState state)
  {
    List<QueryRunner<ResultRow>> runners = new ArrayList<>();
    for (int i = 0; i < state.numSegments; i++) {
      String segmentName = "qIndex " + i;
      QueryRunner<ResultRow> runner = QueryBenchmarkUtil.makeQueryRunner(
          factory,
          SegmentId.dummy(segmentName),
          new QueryableIndexSegment(state.queryableIndexes.get(i), SegmentId.dummy(segmentName))
      );
      runners.add(factory.getToolchest().preMergeQueryDecoration(runner));
    }
    return runners;
  }
}
