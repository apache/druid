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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.indexer.granularity.UniformGranularitySpec;
import org.apache.druid.jackson.AggregatorsModule;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.LoggingEmitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.DefaultGenericQueryMetricsFactory;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.Druids;
import org.apache.druid.query.ForwardingQueryProcessingPool;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.expression.TestExprMacroTable;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.TestGroupByBuffers;
import org.apache.druid.query.metadata.SegmentMetadataQueryConfig;
import org.apache.druid.query.metadata.SegmentMetadataQueryQueryToolChest;
import org.apache.druid.query.metadata.SegmentMetadataQueryRunnerFactory;
import org.apache.druid.query.metadata.metadata.ListColumnIncluderator;
import org.apache.druid.query.metadata.metadata.SegmentMetadataQuery;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryConfig;
import org.apache.druid.query.scan.ScanQueryEngine;
import org.apache.druid.query.scan.ScanQueryQueryToolChest;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryEngine;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesQueryRunnerFactory;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.incremental.SimpleRowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.SegmentGenerationMetrics;
import org.apache.druid.segment.realtime.appenderator.Appenderator;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.appenderator.Appenderators;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.segment.realtime.appenderator.TestAppenderatorConfig;
import org.apache.druid.segment.realtime.sink.Committers;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.NoopDataSegmentAnnouncer;
import org.apache.druid.timeline.DataSegment;
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

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SinkQuerySegmentWalkerBenchmark
{
  private static final String DATASOURCE = "foo";
  private static final List<String> QUERY_COLUMNS = ImmutableList.of("__time", "dim", "count", "met");
  private static final MultipleIntervalSegmentSpec QUERY_INTERVALS =
      new MultipleIntervalSegmentSpec(ImmutableList.of(Intervals.of("2000/2001")));
  private static final String SET_PROCESSING_THREAD_NAMES = "setProcessingThreadNames";

  @Param({"timeseries", "scan", "segmentMetadata", "groupBy"})
  private String queryType;

  @Param({"false", "true"})
  private boolean setProcessingThreadNames;

  @Param({"10", "50", "100", "200"})
  private int numFireHydrants;

  private final LoggingEmitter loggingEmitter = new LoggingEmitter(new Logger(LoggingEmitter.class), LoggingEmitter.Level.INFO, new DefaultObjectMapper());
  private final ServiceEmitter serviceEmitter = new ServiceEmitter("test", "test", loggingEmitter);
  private File cacheDir;

  private ExecutorService queryExecutor;
  private Appenderator appenderator;
  private TestGroupByBuffers groupByBuffers;

  @Setup(Level.Trial)
  public void setup() throws Exception
  {
    final String userConfiguredCacheDir = System.getProperty("druid.benchmark.cacheDir", System.getenv("DRUID_BENCHMARK_CACHE_DIR"));
    cacheDir = new File(userConfiguredCacheDir);
    FileUtils.deleteDirectory(cacheDir);
    final ObjectMapper objectMapper = makeObjectMapper();
    final IndexIO indexIO = new IndexIO(
        objectMapper,
        new ColumnConfig()
        {
        }
    );
    final IndexMergerV9 indexMerger = new IndexMergerV9(
        objectMapper,
        indexIO,
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
    final DataSchema schema = makeDataSchema();
    final RowIngestionMeters rowIngestionMeters = new SimpleRowIngestionMeters();
    final AppenderatorConfig tuningConfig = makeTuningConfig();

    queryExecutor = Execs.singleThreaded("queryExecutor(%d)");
    groupByBuffers = TestGroupByBuffers.createDefault();

    serviceEmitter.start();
    EmittingLogger.registerEmitter(serviceEmitter);

    final QueryRunnerFactoryConglomerate conglomerate = makeQueryRunnerFactoryConglomerate();
    appenderator = Appenderators.createRealtime(
        null,
        schema.getDataSource(),
        schema,
        tuningConfig,
        new SegmentGenerationMetrics(),
        makeDataSegmentPusher(),
        objectMapper,
        indexIO,
        indexMerger,
        conglomerate,
        new NoopDataSegmentAnnouncer(),
        serviceEmitter,
        new ForwardingQueryProcessingPool(queryExecutor),
        MapCache.create(2048),
        new CacheConfig(),
        new CachePopulatorStats(),
        NoopPolicyEnforcer.instance(),
        rowIngestionMeters,
        new ParseExceptionHandler(rowIngestionMeters, false, Integer.MAX_VALUE, 0),
        CentralizedDatasourceSchemaConfig.create(),
        interval -> {}
    );
    appenderator.startJob();

    final SegmentIdWithShardSpec segmentIdWithShardSpec = new SegmentIdWithShardSpec(
        DATASOURCE,
        Intervals.of("2000/2001"),
        "A",
        new LinearShardSpec(0)
    );

    for (int i = 0; i < numFireHydrants; i++) {
      final MapBasedInputRow inputRow = new MapBasedInputRow(
          DateTimes.of("2000").getMillis(),
          ImmutableList.of("dim"),
          ImmutableMap.of(
              "dim",
              "bar_" + i,
              "met",
              1
          )
      );
      appenderator.add(segmentIdWithShardSpec, inputRow, Suppliers.ofInstance(Committers.nil()));
    }
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    try {
      if (appenderator != null) {
        appenderator.close();
      }
    }
    finally {
      if (queryExecutor != null) {
        queryExecutor.shutdownNow();
      }
      try {
        if (groupByBuffers != null) {
          groupByBuffers.close();
        }
      }
      finally {
        FileUtils.deleteDirectory(cacheDir);
      }
    }
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MILLISECONDS)
  public void runSinkQuery(Blackhole blackhole) throws Exception
  {
    final Query<?> query = makeQuery();
    final List<?> results = QueryPlus.wrap(query).run(appenderator, ResponseContext.createEmpty()).toList();
    blackhole.consume(results);

    serviceEmitter.flush();
  }

  private Query<?> makeQuery()
  {
    switch (queryType) {
      case "timeseries":
        return makeTimeseriesQuery();
      case "scan":
        return makeScanQuery();
      case "segmentMetadata":
        return makeSegmentMetadataQuery();
      case "groupBy":
        return makeGroupByQuery();
      default:
        throw new IllegalStateException("Unsupported query type[" + queryType + "]");
    }
  }

  private QueryRunnerFactoryConglomerate makeQueryRunnerFactoryConglomerate()
  {
    return DefaultQueryRunnerFactoryConglomerate.buildFromQueryRunnerFactories(
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
                        ScanQuery.class,
                        new ScanQueryRunnerFactory(
                            new ScanQueryQueryToolChest(DefaultGenericQueryMetricsFactory.instance()),
                            new ScanQueryEngine(),
                            new ScanQueryConfig()
                        )
                    )
                    .put(
                        SegmentMetadataQuery.class,
                        new SegmentMetadataQueryRunnerFactory(
                            new SegmentMetadataQueryQueryToolChest(new SegmentMetadataQueryConfig()),
                            QueryRunnerTestHelper.NOOP_QUERYWATCHER
                        )
                    )
                    .put(
                        GroupByQuery.class,
                        GroupByQueryRunnerTest.makeQueryRunnerFactory(new GroupByQueryConfig(), groupByBuffers)
                    )
                    .build()
    );
  }

  private TimeseriesQuery makeTimeseriesQuery()
  {
    return Druids.newTimeseriesQueryBuilder()
                 .dataSource(DATASOURCE)
                 .intervals(QUERY_INTERVALS)
                 .aggregators(makeAggregators())
                 .granularity(Granularities.DAY)
                 .context(makeQueryContext())
                 .build();
  }

  private ScanQuery makeScanQuery()
  {
    return Druids.newScanQueryBuilder()
                 .dataSource(DATASOURCE)
                 .intervals(QUERY_INTERVALS)
                 .columns(QUERY_COLUMNS)
                 .resultFormat(ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
                 .context(makeQueryContext())
                 .build();
  }

  private SegmentMetadataQuery makeSegmentMetadataQuery()
  {
    return Druids.newSegmentMetadataQueryBuilder()
                 .dataSource(DATASOURCE)
                 .intervals(QUERY_INTERVALS)
                 .toInclude(new ListColumnIncluderator(QUERY_COLUMNS))
                 .analysisTypes(
                     SegmentMetadataQuery.AnalysisType.CARDINALITY,
                     SegmentMetadataQuery.AnalysisType.SIZE,
                     SegmentMetadataQuery.AnalysisType.INTERVAL,
                     SegmentMetadataQuery.AnalysisType.MINMAX,
                     SegmentMetadataQuery.AnalysisType.AGGREGATORS
                 )
                 .merge(true)
                 .context(makeQueryContext())
                 .build();
  }

  private GroupByQuery makeGroupByQuery()
  {
    return GroupByQuery.builder()
                       .setDataSource(DATASOURCE)
                       .setInterval("2000/2001")
                       .setGranularity(Granularities.ALL)
                       .setAggregatorSpecs(makeAggregators())
                       .setContext(makeQueryContext())
                       .build();
  }

  private List<AggregatorFactory> makeAggregators()
  {
    return Arrays.asList(
        new LongSumAggregatorFactory("count", "count"),
        new LongSumAggregatorFactory("met", "met")
    );
  }

  private Map<String, Object> makeQueryContext()
  {
    return ImmutableMap.of(SET_PROCESSING_THREAD_NAMES, setProcessingThreadNames);
  }

  private static ObjectMapper makeObjectMapper()
  {
    final ObjectMapper objectMapper = new DefaultObjectMapper();
    objectMapper.registerSubtypes(LinearShardSpec.class);
    objectMapper.registerModules(new AggregatorsModule());
    objectMapper.registerModules(new BuiltInTypesModule().getJacksonModules());
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class.getName(), TestExprMacroTable.INSTANCE)
            .addValue(ObjectMapper.class.getName(), objectMapper)
    );
    return objectMapper;
  }

  private static DataSchema makeDataSchema()
  {
    return DataSchema.builder()
                     .withDataSource(DATASOURCE)
                     .withTimestamp(new TimestampSpec("ts", "auto", null))
                     .withDimensions(DimensionsSpec.EMPTY)
                     .withAggregators(
                         new CountAggregatorFactory("count"),
                         new LongSumAggregatorFactory("met", "met")
                     )
                     .withGranularity(new UniformGranularitySpec(Granularities.MINUTE, Granularities.NONE, null))
                     .build();
  }

  private AppenderatorConfig makeTuningConfig()
  {
    return new TestAppenderatorConfig(
        TuningConfig.DEFAULT_APPENDABLE_INDEX,
        1,
        Runtime.getRuntime().totalMemory() / 3,
        false,
        IndexSpec.getDefault(),
        0,
        false,
        0L,
        OffHeapMemorySegmentWriteOutMediumFactory.instance(),
        IndexMerger.UNLIMITED_MAX_COLUMNS_TO_MERGE,
        cacheDir,
        false
    );
  }

  private static DataSegmentPusher makeDataSegmentPusher()
  {
    return new DataSegmentPusher()
    {
      @Override
      public DataSegment push(File file, DataSegment segment, boolean useUniquePath)
      {
        return segment;
      }

      @Override
      public Map<String, Object> makeLoadSpec(URI uri)
      {
        throw new UnsupportedOperationException();
      }
    };
  }
}
