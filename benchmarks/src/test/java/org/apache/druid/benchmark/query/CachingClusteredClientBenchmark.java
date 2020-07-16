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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.client.TimelineServerView;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.BySegmentQueryRunner;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.DruidProcessingConfig;
import org.apache.druid.query.Druids;
import org.apache.druid.query.FinalizeResultsQueryRunner;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryConfig;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryEngine;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByQueryRunnerFactory;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
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
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNQueryRunnerFactory;
import org.apache.druid.query.topn.TopNResultValue;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.QueryableIndexSegment;
import org.apache.druid.segment.generator.GeneratorBasicSchemas;
import org.apache.druid.segment.generator.GeneratorSchemaInfo;
import org.apache.druid.segment.generator.SegmentGenerator;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.server.coordination.ServerType;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.TimelineLookup;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.apache.druid.timeline.partition.LinearShardSpec;
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1, jvmArgsAppend = "-XX:+UseG1GC")
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class CachingClusteredClientBenchmark
{
  private static final Logger LOG = new Logger(CachingClusteredClientBenchmark.class);
  private static final int PROCESSING_BUFFER_SIZE = 10 * 1024 * 1024; // ~10MB
  private static final String DATA_SOURCE = "ds";

  public static final ObjectMapper JSON_MAPPER;

  static {
    NullHandling.initializeForTests();
  }

  @Param({"8", "24"})
  private int numServers;

  @Param({"0", "1", "4"})
  private int parallelism;

  @Param({"75000"})
  private int rowsPerSegment;

  @Param({"all", "minute"})
  private String queryGranularity;

  private QueryToolChestWarehouse toolChestWarehouse;
  private QueryRunnerFactoryConglomerate conglomerate;
  private CachingClusteredClient cachingClusteredClient;
  private ExecutorService processingPool;
  private ForkJoinPool forkJoinPool;

  private boolean parallelCombine;

  private Query query;

  private final Closer closer = Closer.create();

  private final GeneratorSchemaInfo basicSchema = GeneratorBasicSchemas.SCHEMA_MAP.get("basic");
  private final QuerySegmentSpec basicSchemaIntervalSpec = new MultipleIntervalSegmentSpec(
      Collections.singletonList(basicSchema.getDataInterval())
  );

  private final int numProcessingThreads = 4;

  static {
    JSON_MAPPER = new DefaultObjectMapper();
    JSON_MAPPER.setInjectableValues(
        new Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), JSON_MAPPER)
            .addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT)
    );
  }

  @Setup(Level.Trial)
  public void setup()
  {
    final String schemaName = "basic";

    parallelCombine = parallelism > 0;

    GeneratorSchemaInfo schemaInfo = GeneratorBasicSchemas.SCHEMA_MAP.get(schemaName);

    Map<DataSegment, QueryableIndex> queryableIndexes = Maps.newHashMapWithExpectedSize(numServers);

    for (int i = 0; i < numServers; i++) {

      final DataSegment dataSegment = DataSegment.builder()
                                                 .dataSource(DATA_SOURCE)
                                                 .interval(schemaInfo.getDataInterval())
                                                 .version("1")
                                                 .shardSpec(new LinearShardSpec(i))
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
          schemaInfo,
          Granularities.NONE,
          rowsPerSegment
      );
      queryableIndexes.put(dataSegment, index);
    }

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
        return true;
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
                        "TopNQueryRunnerFactory-bufferPool",
                        () -> ByteBuffer.allocate(PROCESSING_BUFFER_SIZE)
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
    for (Entry<DataSegment, QueryableIndex> entry : queryableIndexes.entrySet()) {
      serverView.addServer(
          createServer(serverSuffx++),
          entry.getKey(),
          entry.getValue()
      );
    }

    processingPool = Execs.multiThreaded(processingConfig.getNumThreads(), "caching-clustered-client-benchmark");
    forkJoinPool = new ForkJoinPool(
        (int) Math.ceil(Runtime.getRuntime().availableProcessors() * 0.75),
        ForkJoinPool.defaultForkJoinWorkerThreadFactory,
        null,
        true
    );
    cachingClusteredClient = new CachingClusteredClient(
        toolChestWarehouse,
        serverView,
        MapCache.create(0),
        JSON_MAPPER,
        new ForegroundCachePopulator(JSON_MAPPER, new CachePopulatorStats(), 0),
        new CacheConfig(),
        new DruidHttpClientConfig(),
        processingConfig,
        forkJoinPool,
        QueryStackTests.DEFAULT_NOOP_SCHEDULER
    );
  }

  private static GroupByQueryRunnerFactory makeGroupByQueryRunnerFactory(
      final ObjectMapper mapper,
      final GroupByQueryConfig config,
      final DruidProcessingConfig processingConfig
  )
  {
    final Supplier<GroupByQueryConfig> configSupplier = Suppliers.ofInstance(config);
    final Supplier<ByteBuffer> bufferSupplier =
        () -> ByteBuffer.allocateDirect(processingConfig.intermediateComputeSizeBytes());

    final NonBlockingPool<ByteBuffer> bufferPool = new StupidPool<>(
        "GroupByQueryEngine-bufferPool",
        bufferSupplier
    );
    final BlockingPool<ByteBuffer> mergeBufferPool = new DefaultBlockingPool<>(
        bufferSupplier,
        processingConfig.getNumMergeBuffers()
    );
    final GroupByStrategySelector strategySelector = new GroupByStrategySelector(
        configSupplier,
        new GroupByStrategyV1(
            configSupplier,
            new GroupByQueryEngine(configSupplier, bufferPool),
            QueryRunnerTestHelper.NOOP_QUERYWATCHER,
            bufferPool
        ),
        new GroupByStrategyV2(
            processingConfig,
            configSupplier,
            QueryConfig::new,
            bufferPool,
            mergeBufferPool,
            mapper,
            QueryRunnerTestHelper.NOOP_QUERYWATCHER
        )
    );
    final GroupByQueryQueryToolChest toolChest = new GroupByQueryQueryToolChest(strategySelector);
    return new GroupByQueryRunnerFactory(strategySelector, toolChest);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws IOException
  {
    closer.close();
    processingPool.shutdown();
    forkJoinPool.shutdownNow();
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void timeseriesQuery(Blackhole blackhole)
  {
    query = Druids.newTimeseriesQueryBuilder()
                  .dataSource(DATA_SOURCE)
                  .intervals(basicSchemaIntervalSpec)
                  .aggregators(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"))
                  .granularity(Granularity.fromString(queryGranularity))
                  .context(
                      ImmutableMap.of(
                          QueryContexts.BROKER_PARALLEL_MERGE_KEY, parallelCombine,
                          QueryContexts.BROKER_PARALLELISM, parallelism
                      )
                  )
                  .build();

    final List<Result<TimeseriesResultValue>> results = runQuery();

    for (Result<TimeseriesResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void topNQuery(Blackhole blackhole)
  {
    query = new TopNQueryBuilder()
        .dataSource(DATA_SOURCE)
        .intervals(basicSchemaIntervalSpec)
        .dimension(new DefaultDimensionSpec("dimZipf", null))
        .aggregators(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"))
        .granularity(Granularity.fromString(queryGranularity))
        .metric("sumLongSequential")
        .threshold(10_000) // we are primarily measuring 'broker' merge time, so collect a significant number of results
        .context(
            ImmutableMap.of(
                QueryContexts.BROKER_PARALLEL_MERGE_KEY, parallelCombine,
                QueryContexts.BROKER_PARALLELISM, parallelism
            )
        )
        .build();

    final List<Result<TopNResultValue>> results = runQuery();

    for (Result<TopNResultValue> result : results) {
      blackhole.consume(result);
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void groupByQuery(Blackhole blackhole)
  {
    query = GroupByQuery
        .builder()
        .setDataSource(DATA_SOURCE)
        .setQuerySegmentSpec(basicSchemaIntervalSpec)
        .setDimensions(
            new DefaultDimensionSpec("dimZipf", null),
            new DefaultDimensionSpec("dimSequential", null)
        )
        .setAggregatorSpecs(new LongSumAggregatorFactory("sumLongSequential", "sumLongSequential"))
        .setGranularity(Granularity.fromString(queryGranularity))
        .setContext(
            ImmutableMap.of(
                QueryContexts.BROKER_PARALLEL_MERGE_KEY, parallelCombine,
                QueryContexts.BROKER_PARALLELISM, parallelism
            )
        )
        .build();

    final List<ResultRow> results = runQuery();

    for (ResultRow result : results) {
      blackhole.consume(result);
    }
  }

  private <T> List<T> runQuery()
  {
    //noinspection unchecked
    QueryRunner<T> theRunner = new FluentQueryRunnerBuilder<T>(toolChestWarehouse.getToolChest(query))
        .create(cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals()))
        .applyPreMergeDecoration()
        .mergeResults()
        .applyPostMergeDecoration();

    //noinspection unchecked
    Sequence<T> queryResult = theRunner.run(QueryPlus.wrap(query), ResponseContext.createEmpty());

    return queryResult.toList();
  }

  private class SimpleServerView implements TimelineServerView
  {
    private final TierSelectorStrategy tierSelectorStrategy = new HighestPriorityTierSelectorStrategy(
        new RandomServerSelectorStrategy()
    );
    // server -> queryRunner
    private final Map<DruidServer, SingleSegmentDruidServer> servers = new HashMap<>();
    // segmentId -> serverSelector
    private final Map<String, ServerSelector> selectors = new HashMap<>();
    // dataSource -> version -> serverSelector
    private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines = new HashMap<>();

    void addServer(DruidServer server, DataSegment dataSegment, QueryableIndex queryableIndex)
    {
      servers.put(
          server,
          new SingleSegmentDruidServer(
              server,
              new SimpleQueryRunner(
                  conglomerate,
                  dataSegment.getId(),
                  queryableIndex
              )
          )
      );
      addSegmentToServer(server, dataSegment);
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

  private static class SimpleQueryRunner implements QueryRunner<Object>
  {
    private final QueryRunnerFactoryConglomerate conglomerate;
    private final QueryableIndexSegment segment;

    public SimpleQueryRunner(
        QueryRunnerFactoryConglomerate conglomerate,
        SegmentId segmentId,
        QueryableIndex queryableIndex
    )
    {
      this.conglomerate = conglomerate;
      this.segment = new QueryableIndexSegment(queryableIndex, segmentId);
    }

    @Override
    public Sequence<Object> run(QueryPlus<Object> queryPlus, ResponseContext responseContext)
    {
      final QueryRunnerFactory factory = conglomerate.findFactory(queryPlus.getQuery());
      //noinspection unchecked
      return factory.getToolchest().preMergeQueryDecoration(
          new FinalizeResultsQueryRunner<>(
              new BySegmentQueryRunner<>(
                  segment.getId(),
                  segment.getDataInterval().getStart(),
                  factory.createRunner(segment)
              ),
              factory.getToolchest()
          )
      ).run(queryPlus, responseContext);
    }
  }

  private static class SingleSegmentDruidServer extends QueryableDruidServer<SimpleQueryRunner>
  {
    SingleSegmentDruidServer(DruidServer server, SimpleQueryRunner runner)
    {
      super(server, runner);
    }
  }

  private static DruidServer createServer(int nameSuiffix)
  {
    return new DruidServer(
        "server_" + nameSuiffix,
        "127.0.0." + nameSuiffix,
        null,
        Long.MAX_VALUE,
        ServerType.HISTORICAL,
        "default",
        0
    );
  }
}
