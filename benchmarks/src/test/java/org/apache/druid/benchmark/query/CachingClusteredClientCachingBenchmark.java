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

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.JsonParserIterator;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.CaffeineCache;
import org.apache.druid.client.cache.CaffeineCacheConfig;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.selector.HighestPriorityTierSelectorStrategy;
import org.apache.druid.client.selector.QueryableDruidServer;
import org.apache.druid.client.selector.RandomServerSelectorStrategy;
import org.apache.druid.client.selector.ServerSelector;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.StupidPool;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.http.DruidHttpClientConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.offheap.OffheapBufferGenerator;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DirectQueryProcessingPool;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.ResultLevelCachingQueryRunner;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import org.apache.druid.query.aggregation.hyperloglog.HyperUniquesSerde;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryEngine;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV1;
import org.apache.druid.query.groupby.strategy.GroupByStrategyV2;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.spec.QuerySegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.segment.join.MapJoinableFactory;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.LinearShardSpec;
import org.apache.druid.utils.CloseableUtils;
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
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * A benchmark test for different cache options on broker. It's copied from {@link CachingClusteredClientBenchmark} and
 * is modified to test the cache.
 * <p>
 * Key ideas:
 * 1. the {@link SimpleQueryRunner} simulates the query runner {@link org.apache.druid.client.DirectDruidClient}.
 * 2. {@link #createServer(int, boolean)} creates historical and realtime servers to simulate scanning both historical and realtime segments.
 * 3. {@link SimpleQueryRunner#prepareServerResult(QueryPlus, ResponseContext)} prepares downstream server processing results
 * and simulates serde of sequence.
 * 4. {@link SimpleQueryRunner#run(QueryPlus, ResponseContext)} simulates query processing and sequence serializing on
 * downstream process(historical or peon) by calling {@link Thread#sleep(long)} and the deserializing process on Broker
 * which acts like what {@link org.apache.druid.client.DirectDruidClient} does.
 * 5. particular queries with fixed time interval, "2000-01-01/P1D", to simulate queries with time range filter, like the
 * day so far or this week/month/year so far.
 */
@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = {"-XX:+UseG1GC", "-XX:MaxDirectMemorySize=1500m"})
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class CachingClusteredClientCachingBenchmark
{
  public static final ObjectMapper JSON_MAPPER;
  private static final Logger LOG = new Logger(CachingClusteredClientCachingBenchmark.class);
  private static final int PROCESSING_BUFFER_SIZE = 250 * 1024 * 1024;
  private static final String DATA_SOURCE = "ds";
  private static final int TEST_POOL_SIZE = (int) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.75);
  private static final boolean USE_PARALLEL_MERGE_POOL = false;

  static {
    NullHandling.initializeForTests();
  }

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), JSON_MAPPER)
            .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT)
    );
  }

  private final Closer closer = Closer.create();
  private final GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
  private final QuerySegmentSpec basicSchemaIntervalSpec = new MultipleIntervalSegmentSpec(
      Collections.singletonList(basicSchema.getDataInterval())
  );

  private final int numProcessingThreads = 4;
  private final CaffeineCache cache = CaffeineCache.create(new CaffeineCacheConfig()
  {
    @Override
    public long getSizeInBytes()
    {
      return 500 * 1024 * 1024;
    }
  });
  private final List<Query> queries = new ArrayList<>();
  @Param({"1", "3"})
  private int round;
  @Param({"20000"})
  private int rowsPerSegment;
  @Param({"5", "10"})
  private int numHistoircals;
  @Param({"0", "1"})
  private int numPeons;
  @Param({"5"})
  private int numSegmentsPerHistorial;
  @Param({"1"})
  private int numSegmentsPerPeon;
  @Param({"minute"})
  private String queryGranularity;
  @Param({"segment", "result", "segmentMerged", "noCache"})
  private String cacheOption;
  private QueryToolChestWarehouse toolChestWarehouse;
  private QueryRunnerFactoryConglomerate conglomerate;
  private CachingClusteredClient cachingClusteredClient;
  private ForkJoinPool forkJoinPool;
  private CacheConfig cacheConfig;
  private CachePopulatorStats cachePopulatorStats;
  
  private boolean useSegmentLevelCache = false;
  private boolean useResultLevelCache = false;
  private boolean useSegmentMergedResultCache = false;
  private boolean useCache = true;

  private static GroupByQueryRunnerFactory makeGroupByQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final DruidProcessingConfig processingConfig
  )
  {
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);

    NonBlockingPool<ByteBuffer> bufferPool = new StupidPool<>(
        "GroupByBenchmark-computeBufferPool",
        new OffheapBufferGenerator("compute", processingConfig.intermediateComputeSizeBytes()),
        0,
        Integer.MAX_VALUE
    );

    final BlockingPool<ByteBuffer> mergeBufferPool = new DefaultBlockingPool<>(
        new OffheapBufferGenerator("merge", processingConfig.intermediateComputeSizeBytes()),
        processingConfig.getNumMergeBuffers()
    );
    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        ),
        new GroupByStrategyV2(
            processingConfig,
            configSupplier,
            bufferPool,
            mergeBufferPool,
            mapper,
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(strategySelector);
    return new GroupByQueryRunnerFactory(strategySelector, toolChest);
  }

  private static DruidServer createServer(int nameSuiffix, boolean isHistorical)
  {
    return new DruidServer(
        "server_" + nameSuiffix,
        "127.0.0." + nameSuiffix,
        null,
        Long.MAX_VALUE,
        isHistorical ? ServerType.HISTORICAL : ServerType.REALTIME,
        "default",
        0
    );
  }

  public static void main(String[] args) throws Exception
  {
    Options opt = new OptionsBuilder()
        .include(CachingClusteredClientCachingBenchmark.class.getSimpleName())
        .forks(1)
        .syncIterations(true)
        .resultFormat(ResultFormatType.JSON)
        .result("CachingClusteredClientCachingBenchmark.json")
        .build();

    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setup()
  {
    ComplexMetrics.registerSerde("hyperUnique", new HyperUniquesSerde());

    switch (cacheOption) {
      case "segment":
        useSegmentLevelCache = true;
        break;
      case "result":
        useResultLevelCache = true;
        break;
      case "segmentMerged":
        useSegmentMergedResultCache = true;
        break;
      default:
        useCache = false;
    }
    cacheConfig = new CacheConfig()
    {
      @Override
      public boolean isPopulateCache()
      {
        return useCache && useSegmentLevelCache;
      }

      @Override
      public boolean isUseCache()
      {
        return isPopulateCache();
      }

      @Override
      public boolean isPopulateResultLevelCache()
      {
        return useCache && useResultLevelCache;
      }

      @Override
      public boolean isUseResultLevelCache()
      {

        return isPopulateResultLevelCache();
      }

      @Override
      public boolean isPopulateSegmentMergedResultCache()
      {
        return useCache && useSegmentMergedResultCache;
      }

      @Override
      public boolean isUseSegmentMergedResultCache()
      {
        return isPopulateSegmentMergedResultCache();
      }

      @Override
      public int getMaxEntrySize()
      {
        return 50 * 1024 * 1024;
      }
    };

    queries.add(groupByQuery());
    queries.add(timeseriesQuery());
    queries.add(topNQuery());

    final DruidProcessingConfig processingConfig = new DruidProcessingConfig()
    {
      @Override
      public String getFormatString()
      {
        return null;
      }

      @Override
      public int intermediateComputeSizeBytes()
      {
        return PROCESSING_BUFFER_SIZE;
      }

      @Override
      public int getNumMergeBuffers()
      {
        return 1;
      }

      @Override
      public int getNumThreads()
      {
        return numProcessingThreads;
      }

      @Override
      public boolean useParallelMergePool()
      {
        return USE_PARALLEL_MERGE_POOL;
      }
    };

    conglomerate = new DefaultQueryRunnerFactoryConglomerate(
        ImmutableMap.<Class<? extends Query>, QueryRunnerFactory>builder()
                    .put(
                        TimeseriesQuery.class,
                        new TimeseriesQueryRunnerFactory(
                            new TimeseriesQueryQueryToolChest(),
                            new TimeseriesQueryEngine(),
                            QueryRunnerTestHelper.NOOP_QUERYWATCHER
                        )
                    )
                    .put(
                        TopNQuery.class,
                        new TopNQueryRunnerFactory(
                            new StupidPool<>(
                                "TopNBenchmark-compute-bufferPool",
                                new OffheapBufferGenerator("compute", processingConfig.intermediateComputeSizeBytes()),
                                0,
                                Integer.MAX_VALUE
                            ),
                            new TopNQueryQueryToolChest(new TopNQueryConfig()),
                            QueryRunnerTestHelper.NOOP_QUERYWATCHER
                        )
                    )
                    .put(
                        GroupByQuery.class,
                        makeGroupByQueryRunnerFactory(
                            GroupByQueryRunnerTest.DEFAULT_MAPPER,
                            new GroupByQueryConfig()
                            {
                              @Override
                              public String getDefaultStrategy()
                              {
                                return GroupByStrategySelector.STRATEGY_V2;
                              }
                            },
                            processingConfig
                        )
                    )
                    .build()
    );

    toolChestWarehouse = new QueryToolChestWarehouse()
    {
      @Override
      public <T, QueryType extends Query<T>> QueryToolChest<T, QueryType> getToolChest(final QueryType query)
      {
        return conglomerate.findFactory(query).getToolchest();
      }
    };

    SimpleServerView serverView = new SimpleServerView();
    int serverSuffx = 1;
    int shardId = 1;
    for (int i = 0; i < numHistoircals; i++) {
      DruidServer server = createServer(serverSuffx++, true);
      List<NonnullPair<DataSegment, QueryableIndex>> segments = new ArrayList<>();
      for (int j = 0; j < numSegmentsPerHistorial; j++) {
        segments.add(generateSegment(shardId++));
      }
      serverView.addServer(server, segments);
    }
    for (int i = 0; i < numPeons; i++) {
      DruidServer server = createServer(serverSuffx++, false);
      List<NonnullPair<DataSegment, QueryableIndex>> segments = new ArrayList<>();
      for (int j = 0; j < numSegmentsPerPeon; j++) {
        segments.add(generateSegment(shardId++));
      }
      serverView.addServer(server, segments);
    }

    forkJoinPool = new ForkJoinPool(
        TEST_POOL_SIZE,
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null,
        true
    );
    cachePopulatorStats = new CachePopulatorStats();
    cachingClusteredClient = new CachingClusteredClient(
        toolChestWarehouse,
        serverView,
        cache,
        JSON_MAPPER,
        new ForegroundCachePopulator(JSON_MAPPER, cachePopulatorStats, cacheConfig.getMaxEntrySize()),
        cacheConfig,
        new DruidHttpClientConfig(),
        processingConfig,
        forkJoinPool,
        QueryStackTests.DEFAULT_NOOP_SCHEDULER,
        new MapJoinableFactory(ImmutableSet.of(), ImmutableMap.of()),
        new NoopServiceEmitter()
    );

    // prepare server results
    LOG.info("Starting preparing server results");
    long start = System.currentTimeMillis();
    for (Query query : queries) {
      for (SimpleQueryRunner runner : serverView.queryRunners) {
        //noinspection unchecked
        if (cacheConfig.isUseCache() && runner.isHistoricalRunner() && !(query instanceof GroupByQuery)) {
          query = query.withOverriddenContext(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, true));
        } else {
          query = query.withOverriddenContext(ImmutableMap.of(QueryContexts.BY_SEGMENT_KEY, false));
        }
        runner.prepareServerResult(QueryPlus.wrap(query), ResponseContext.createEmpty());
      }
    }
    LOG.info("Finish preparing server results. Total time: %s milliseconds", System.currentTimeMillis() - start);
  }

  private NonnullPair<DataSegment, QueryableIndex> generateSegment(int shardId)
  {
    final DataSegment dataSegment = DataSegment.builder()
                                               .dataSource(DATA_SOURCE)
                                               .interval(basicSchema.getDataInterval())
                                               .version("1")
                                               .shardSpec(new LinearShardSpec(shardId))
                                               .size(0)
                                               .build();
    final SegmentGenerator segmentGenerator = closer.register(new SegmentGenerator());
    LOG.info(
        "Starting benchmark setup using cacheDir[%s], rows[%,d].",
        segmentGenerator.getCacheDir(),
        rowsPerSegment
    );
    final QueryableIndex index = segmentGenerator.generate(
        dataSegment,
        basicSchema,
        Granularities.NONE,
        rowsPerSegment
    );
    return new NonnullPair<>(dataSegment, index);
  }

  @TearDown(Level.Iteration)
  public void tearDownIteration()
  {
    CachePopulatorStats.Snapshot snapshot = cachePopulatorStats.snapshot();
    LOG.info(
        "CachePopulatorStats: { okCounter: %d, errorCounter: %d, oversizedCounter: %d }",
        snapshot.getNumOk(),
        snapshot.getNumError(),
        snapshot.getNumOversized()
    );
    cache.getCache().invalidateAll();
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException
  {
    closer.close();
    forkJoinPool.shutdownNow();
  }

  private Query timeseriesQuery()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(DATA_SOURCE)
                 .intervals(basicSchemaIntervalSpec)
                 .aggregators(
                     new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"),
                     new HyperUniquesAggregatorFactory("hyperUniquesMet", "hyper")
                 )
                 .granularity(Granularity.fromString(queryGranularity))
                 .randomQueryId()
                 .build();
  }

  private Query topNQuery()
  {
    return new TopNQueryBuilder()
        .dataSource(DATA_SOURCE)
        .intervals(basicSchemaIntervalSpec)
        .dimension(new DefaultDimensionSpec("dimZipf", null))
        .aggregators(
            new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"),
            new HyperUniquesAggregatorFactory("hyperUniquesMet", "hyper")
        )
        .granularity(Granularity.fromString(queryGranularity))
        .metric("sumLongSequential")
        .threshold(10_000) // we are primarily measuring 'broker' merge time, so collect a significant number of results
        .randomQueryId()
        .build();
  }

  private Query groupByQuery()
  {
    return GroupByQuery
        .builder()
        .setDataSource(DATA_SOURCE)
        .setQuerySegmentSpec(basicSchemaIntervalSpec)
        .setDimensions(
            new DefaultDimensionSpec("dimZipf", null),
            new DefaultDimensionSpec("dimSequential", null)
        )
        .setAggregatorSpecs(
            new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"),
            new HyperUniquesAggregatorFactory("hyperUniquesMet", "hyper")
        )
        .setGranularity(Granularity.fromString(queryGranularity))
        .randomQueryId()
        .build();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void runQueries(Blackhole blackhole)
  {
    for (int i = 0; i < round; i++) {
      for (Query query : queries) {
        //noinspection unchecked
        final QueryToolChest toolChest = toolChestWarehouse.getToolChest(query);
        //noinspection unchecked
        QueryRunner theRunner = new FluentQueryRunnerBuilder(toolChest)
            .create(cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals()))
            .applyPreMergeDecoration()
            .mergeResults()
            .applyPostMergeDecoration()
            .map(
                runner ->
                    new ResultLevelCachingQueryRunner(
                        (QueryRunner) runner,
                        toolChest,
                        query,
                        JSON_MAPPER,
                        cache,
                        cacheConfig
                    )
            );

        //noinspection unchecked
        Sequence queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());

        for (Object result : queryResult.toList()) {
          blackhole.consume(result);
        }
      }
    }
  }

  private static class SimpleQueryRunner implements QueryRunner<Object>
  {
    private final QueryRunnerFactoryConglomerate conglomerate;
    private final List<QueryableIndexSegment> segments;
    private final Map<String, NonnullPair<Sequence, Long>> queryIdToSequences = new HashMap<>();
    private final boolean isHistoricalRunner;

    public SimpleQueryRunner(
        QueryRunnerFactoryConglomerate conglomerate,
        List<QueryableIndexSegment> segments,
        boolean isHistoricalRunner
    )
    {
      this.conglomerate = conglomerate;
      this.segments = segments;
      this.isHistoricalRunner = isHistoricalRunner;
    }

    public boolean isHistoricalRunner()
    {
      return isHistoricalRunner;
    }

    public void prepareServerResult(QueryPlus<Object> queryPlus, ResponseContext responseContext)
    {
      long start = System.currentTimeMillis();
      final QueryRunnerFactory factory = conglomerate.findFactory(queryPlus.getQuery());

      //noinspection unchecked
      Sequence sequence = factory.getToolchest().mergeResults(
          factory.mergeRunners(
              DirectQueryProcessingPool.INSTANCE,
              segments.stream()
                      .map(
                          segment -> new BySegmentQueryRunner(
                              segment.getId(),
                              segment.getDataInterval().getStart(),
                              factory.createRunner(segment)
                          )
                      )
                      .collect(Collectors.toList())
          )
      ).run(queryPlus, responseContext);

      queryIdToSequences.put(
          queryPlus.getQuery().getId(),
          new NonnullPair<>(
              simulateSequenceSerde(
                  Sequences.simple(sequence.toList()),
                  factory.getToolchest(),
                  queryPlus.getQuery()
              ),
              // query processing time on downstream server(historical or peon process)
              System.currentTimeMillis() - start
          )
      );
    }

    private <T> Sequence<T> simulateSequenceSerde(Sequence sequence, QueryToolChest toolChest, Query query)
    {
      try {
        // serialize sequence to bytes which will be collected by Broker
        final Yielder<?> yielder = Yielders.each(sequence);
        ObjectWriter jsonWriter = toolChest.decorateObjectMapper(JSON_MAPPER, query).writer();
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        jsonWriter.writeValue(outputStream, yielder);
        final byte[] sequenceBytes = outputStream.toByteArray();

        // deserialize sequence from bytes on Broker
        boolean isBySegment = QueryContexts.isBySegment(query);
        final JavaType queryResultType = isBySegment
                                         ? toolChest.getBySegmentResultType()
                                         : toolChest.getBaseResultType();
        Sequence<T> retVal = new BaseSequence<>(
            new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
            {
              @Override
              public JsonParserIterator<T> make()
              {
                return new JsonParserIterator<T>(
                    queryResultType,
                    new Future<InputStream>()
                    {
                      @Override
                      public boolean cancel(boolean mayInterruptIfRunning)
                      {
                        return false;
                      }

                      @Override
                      public boolean isCancelled()
                      {
                        return false;
                      }

                      @Override
                      public boolean isDone()
                      {
                        return false;
                      }

                      @Override
                      public InputStream get()
                      {
                        return new ByteArrayInputStream(sequenceBytes);
                      }

                      @Override
                      public InputStream get(long timeout, TimeUnit unit)
                      {
                        return get();
                      }
                    },
                    "url",
                    query,
                    "host",
                    toolChest.decorateObjectMapper(JSON_MAPPER, query)
                );
              }

              @Override
              public void cleanup(JsonParserIterator<T> iterFromMake)
              {
                CloseableUtils.closeAndWrapExceptions(iterFromMake);
              }
            }
        );
        if (!isBySegment) {
          retVal = Sequences.map(
              retVal,
              toolChest.makePreComputeManipulatorFn(
                  query,
                  MetricManipulatorFns.deserializing()
              )
          );
        }
        return retVal;
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Sequence<Object> run(QueryPlus<Object> queryPlus, ResponseContext responseContext)
    {
      NonnullPair<Sequence, Long> pair = queryIdToSequences.get(queryPlus.getQuery().getId());
      try {
        Thread.sleep(pair.rhs);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return pair.lhs;
    }
  }

  private static class SingleSegmentDruidServer extends QueryableDruidServer<SimpleQueryRunner>
  {
    SingleSegmentDruidServer(DruidServer server, SimpleQueryRunner runner)
    {
      super(server, runner);
    }
  }

  private class SimpleServerView implements TimelineServerView
  {
    final List<SimpleQueryRunner> queryRunners = new ArrayList<>();
    private final TierSelectorStrategy tierSelectorStrategy = new HighestPriorityTierSelectorStrategy(
        new RandomServerSelectorStrategy()
    );
    // server -> queryRunner
    private final Map<DruidServer, SingleSegmentDruidServer> servers = new HashMap<>();
    // segmentId -> serverSelector
    private final Map<String, ServerSelector> selectors = new HashMap<>();
    // dataSource -> version -> serverSelector
    private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();

    void addServer(DruidServer server, List<NonnullPair<DataSegment, QueryableIndex>> segments)
    {
      SimpleQueryRunner runner = new SimpleQueryRunner(
          conglomerate,
          segments.stream()
                  .map(seg -> new QueryableIndexSegment(seg.rhs, seg.lhs.getId()))
                  .collect(Collectors.toList()),
          server.isSegmentReplicationTarget()
      );
      queryRunners.add(runner);
      servers.put(
          server,
          new SingleSegmentDruidServer(
              server,
              runner
          )
      );
      segments.forEach(pair -> addSegmentToServer(server, pair.lhs));
    }

    void addSegmentToServer(DruidServer server, DataSegment segment)
    {
      final ServerSelector selector = selectors.computeIfAbsent(
          segment.getId().toString(),
          k -> new ServerSelector(segment, tierSelectorStrategy)
      );
      selector.addServerAndUpdateSegment(servers.get(server), segment);
      timelines.computeIfAbsent(segment.getDataSource(), k -> new VersionedIntervalTimeline<>(Ordering.natural()))
               .add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
    }

    @Override
    public Optional<? extends TimelineLookup<String, ServerSelector>> getTimeline(DataSourceAnalysis analysis)
    {
      return Optional.ofNullable(timelines.get(analysis.getBaseTableDataSource().get().getName()));
    }

    @Override
    public List<ImmutableDruidServer> getDruidServers()
    {
      return Collections.emptyList();
    }

    @Override
    public <T> QueryRunner<T> getQueryRunner(DruidServer server)
    {
      final SingleSegmentDruidServer queryableDruidServer = Preconditions.checkNotNull(servers.get(server), "server");
      return (QueryRunner<T>) queryableDruidServer.getQueryRunner();
    }

    @Override
    public void registerTimelineCallback(Executor exec, TimelineCallback callback)
    {
      // do nothing
    }

    @Override
    public void registerServerRemovedCallback(Executor exec, ServerRemovedCallback callback)
    {
      // do nothing
    }

    @Override
    public void registerSegmentCallback(Executor exec, SegmentCallback callback)
    {
      // do nothing
    }
  }
}
