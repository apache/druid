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

package org.apache.druid.segment.realtime.appenderator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.segment.BaseProgressIndicator;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMerger;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.IndexableAdapter;
import org.apache.druid.segment.ProgressIndicator;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.ParseExceptionHandler;
import org.apache.druid.segment.incremental.RowIngestionMeters;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.FireDepartmentMetrics;
import org.apache.druid.segment.realtime.plumber.Sink;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.timeline.VersionedIntervalTimeline;
import org.joda.time.Interval;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Manages {@link Appenderator} instances for the CliIndexer task execution service, which runs all tasks in
 * a single process.
 *
 * This class keeps a map of {@link DatasourceBundle} objects, keyed by datasource name. Each bundle contains:
 * - A per-datasource {@link SinkQuerySegmentWalker} (with an associated per-datasource timeline)
 * - A map that associates a taskId with a list of Appenderators created for that task
 *
 * Access to the datasource bundle map and the task->appenderator maps is synchronized. The methods
 * on this class can be called concurrently from multiple task threads. If there are no remaining
 * appenderators for a given datasource, the corresponding bundle will be removed from the bundle map.
 *
 * Appenderators created by this class will use the shared per-datasource SinkQuerySegmentWalkers.
 *
 * The per-datasource SinkQuerySegmentWalkers share a common queryExecutorService.
 *
 * Each task that requests an Appenderator from this AppenderatorsManager will receive a heap memory limit
 * equal to {@link WorkerConfig#globalIngestionHeapLimitBytes} evenly divided by {@link WorkerConfig#capacity}.
 * This assumes that each task will only ingest to one Appenderator simultaneously.
 *
 * The Appenderators created by this class share an executor pool for {@link IndexMerger} persist
 * and merge operations, with concurrent operations limited to `druid.worker.capacity` divided 2. This limit is imposed
 * to reduce overall memory usage.
 */
public class UnifiedIndexerAppenderatorsManager implements AppenderatorsManager
{
  private final Logger LOG = new Logger(UnifiedIndexerAppenderatorsManager.class);

  private final Map<String, DatasourceBundle> datasourceBundles = new HashMap<>();

  private final QueryProcessingPool queryProcessingPool;
  private final JoinableFactoryWrapper joinableFactoryWrapper;
  private final WorkerConfig workerConfig;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;
  private final ObjectMapper objectMapper;
  private final ServiceEmitter serviceEmitter;
  private final Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider;

  private ListeningExecutorService mergeExecutor;

  @Inject
  public UnifiedIndexerAppenderatorsManager(
      QueryProcessingPool queryProcessingPool,
      JoinableFactoryWrapper joinableFactoryWrapper,
      WorkerConfig workerConfig,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      ObjectMapper objectMapper,
      ServiceEmitter serviceEmitter,
      Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider
  )
  {
    this.queryProcessingPool = queryProcessingPool;
    this.joinableFactoryWrapper = joinableFactoryWrapper;
    this.workerConfig = workerConfig;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.cachePopulatorStats = cachePopulatorStats;
    this.objectMapper = objectMapper;
    this.serviceEmitter = serviceEmitter;
    this.queryRunnerFactoryConglomerateProvider = queryRunnerFactoryConglomerateProvider;

    this.mergeExecutor = MoreExecutors.listeningDecorator(
        Execs.multiThreaded(workerConfig.getNumConcurrentMerges(), "unified-indexer-merge-pool-%d")
    );
  }

  @Override
  public Appenderator createRealtimeAppenderatorForTask(
      SegmentLoaderConfig segmentLoaderConfig,
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      QueryRunnerFactoryConglomerate conglomerate,
      DataSegmentAnnouncer segmentAnnouncer,
      ServiceEmitter emitter,
      QueryProcessingPool queryProcessingPool,
      JoinableFactory joinableFactory,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    synchronized (this) {
      DatasourceBundle datasourceBundle = datasourceBundles.computeIfAbsent(
          schema.getDataSource(),
          DatasourceBundle::new
      );

      Appenderator appenderator = new StreamAppenderator(
          null,
          taskId,
          schema,
          rewriteAppenderatorConfigMemoryLimits(config),
          metrics,
          dataSegmentPusher,
          objectMapper,
          segmentAnnouncer,
          datasourceBundle.getWalker(),
          indexIO,
          wrapIndexMerger(indexMerger),
          cache,
          rowIngestionMeters,
          parseExceptionHandler,
          useMaxMemoryEstimates,
          centralizedDatasourceSchemaConfig
      );

      datasourceBundle.addAppenderator(taskId, appenderator);
      return appenderator;
    }
  }

  @Override
  public Appenderator createOfflineAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates
  )
  {
    synchronized (this) {
      DatasourceBundle datasourceBundle = datasourceBundles.computeIfAbsent(
          schema.getDataSource(),
          DatasourceBundle::new
      );

      Appenderator appenderator = Appenderators.createOffline(
          taskId,
          schema,
          rewriteAppenderatorConfigMemoryLimits(config),
          metrics,
          dataSegmentPusher,
          objectMapper,
          indexIO,
          wrapIndexMerger(indexMerger),
          rowIngestionMeters,
          parseExceptionHandler,
          useMaxMemoryEstimates
      );
      datasourceBundle.addAppenderator(taskId, appenderator);
      return appenderator;
    }
  }

  @Override
  public Appenderator createOpenSegmentsOfflineAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates
  )
  {
    synchronized (this) {
      DatasourceBundle datasourceBundle = datasourceBundles.computeIfAbsent(
          schema.getDataSource(),
          DatasourceBundle::new
      );

      Appenderator appenderator = Appenderators.createOpenSegmentsOffline(
          taskId,
          schema,
          rewriteAppenderatorConfigMemoryLimits(config),
          metrics,
          dataSegmentPusher,
          objectMapper,
          indexIO,
          wrapIndexMerger(indexMerger),
          rowIngestionMeters,
          parseExceptionHandler,
          useMaxMemoryEstimates
      );
      datasourceBundle.addAppenderator(taskId, appenderator);
      return appenderator;
    }
  }

  @Override
  public Appenderator createClosedSegmentsOfflineAppenderatorForTask(
      String taskId,
      DataSchema schema,
      AppenderatorConfig config,
      FireDepartmentMetrics metrics,
      DataSegmentPusher dataSegmentPusher,
      ObjectMapper objectMapper,
      IndexIO indexIO,
      IndexMerger indexMerger,
      RowIngestionMeters rowIngestionMeters,
      ParseExceptionHandler parseExceptionHandler,
      boolean useMaxMemoryEstimates
  )
  {
    synchronized (this) {
      DatasourceBundle datasourceBundle = datasourceBundles.computeIfAbsent(
          schema.getDataSource(),
          DatasourceBundle::new
      );

      Appenderator appenderator = Appenderators.createClosedSegmentsOffline(
          taskId,
          schema,
          rewriteAppenderatorConfigMemoryLimits(config),
          metrics,
          dataSegmentPusher,
          objectMapper,
          indexIO,
          wrapIndexMerger(indexMerger),
          rowIngestionMeters,
          parseExceptionHandler,
          useMaxMemoryEstimates
      );
      datasourceBundle.addAppenderator(taskId, appenderator);
      return appenderator;
    }
  }

  @Override
  public void removeAppenderatorsForTask(
      String taskId,
      String dataSource
  )
  {
    synchronized (this) {
      DatasourceBundle datasourceBundle = datasourceBundles.get(dataSource);
      if (datasourceBundle == null) {
        // Not a warning, because not all tasks use Appenderators.
        LOG.debug("Could not find datasource bundle for [%s], task [%s]", dataSource, taskId);
      } else {
        List<Appenderator> existingAppenderators = datasourceBundle.taskAppenderatorMap.remove(taskId);
        if (existingAppenderators == null) {
          // Not a warning, because not all tasks use Appenderators.
          LOG.debug("Tried to remove appenderators for task [%s] but none were found.", taskId);
        }
        if (datasourceBundle.taskAppenderatorMap.isEmpty()) {
          datasourceBundles.remove(dataSource);
        }
      }
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(
      Query<T> query,
      Iterable<Interval> intervals
  )
  {
    return getBundle(query).getWalker().getQueryRunnerForIntervals(query, intervals);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(
      Query<T> query,
      Iterable<SegmentDescriptor> specs
  )
  {
    return getBundle(query).getWalker().getQueryRunnerForSegments(query, specs);
  }

  @VisibleForTesting
  <T> DatasourceBundle getBundle(final Query<T> query)
  {
    final DataSourceAnalysis analysis = query.getDataSource().getAnalysis();

    final TableDataSource table =
        analysis.getBaseTableDataSource()
                .orElseThrow(() -> new ISE("Cannot handle datasource: %s", query.getDataSource()));

    final DatasourceBundle bundle;

    synchronized (this) {
      bundle = datasourceBundles.get(table.getName());
    }

    if (bundle == null) {
      throw new IAE("Could not find segment walker for datasource [%s]", table.getName());
    }

    return bundle;
  }

  @Override
  public boolean shouldTaskMakeNodeAnnouncements()
  {
    return false;
  }

  @Override
  public void shutdown()
  {
    if (mergeExecutor != null) {
      mergeExecutor.shutdownNow();
      mergeExecutor = null;
    }
  }

  @VisibleForTesting
  public Map<String, DatasourceBundle> getDatasourceBundles()
  {
    return datasourceBundles;
  }

  public WorkerConfig getWorkerConfig()
  {
    return workerConfig;
  }

  private AppenderatorConfig rewriteAppenderatorConfigMemoryLimits(AppenderatorConfig baseConfig)
  {
    long perWorkerLimit = workerConfig.getGlobalIngestionHeapLimitBytes() / workerConfig.getCapacity();
    return new MemoryParameterOverridingAppenderatorConfig(baseConfig, perWorkerLimit);
  }

  @VisibleForTesting
  public class DatasourceBundle
  {
    private final SinkQuerySegmentWalker walker;
    private final Map<String, List<Appenderator>> taskAppenderatorMap;

    public DatasourceBundle(
        String dataSource
    )
    {
      this.taskAppenderatorMap = new HashMap<>();

      VersionedIntervalTimeline<String, Sink> sinkTimeline = new VersionedIntervalTimeline<>(
          String.CASE_INSENSITIVE_ORDER
      );
      this.walker = new SinkQuerySegmentWalker(
          dataSource,
          sinkTimeline,
          objectMapper,
          serviceEmitter,
          queryRunnerFactoryConglomerateProvider.get(),
          queryProcessingPool,
          Preconditions.checkNotNull(cache, "cache"),
          cacheConfig,
          cachePopulatorStats
      );
    }

    public SinkQuerySegmentWalker getWalker()
    {
      return walker;
    }

    public void addAppenderator(String taskId, Appenderator appenderator)
    {
      taskAppenderatorMap.computeIfAbsent(
          taskId,
          myTaskId -> new ArrayList<>()
      ).add(appenderator);
    }
  }

  /**
   * This is a wrapper around AppenderatorConfig that overrides the {@link AppenderatorConfig#getMaxBytesInMemory()}
   * and {@link AppenderatorConfig#getMaxRowsInMemory()} parameters.
   *
   * Row-based limits are disabled by setting maxRowsInMemory to an essentially unlimited value.
   * maxBytesInMemory is overridden with the provided value. These overrides replace whatever the user has specified.
   */
  private static class MemoryParameterOverridingAppenderatorConfig implements AppenderatorConfig
  {
    private final AppenderatorConfig baseConfig;
    private final long newMaxBytesInMemory;

    public MemoryParameterOverridingAppenderatorConfig(
        AppenderatorConfig baseConfig,
        long newMaxBytesInMemory
    )
    {
      this.baseConfig = baseConfig;
      this.newMaxBytesInMemory = newMaxBytesInMemory;
    }

    @Override
    public boolean isReportParseExceptions()
    {
      return baseConfig.isReportParseExceptions();
    }

    @Override
    public AppendableIndexSpec getAppendableIndexSpec()
    {
      return baseConfig.getAppendableIndexSpec();
    }

    @Override
    public int getMaxRowsInMemory()
    {
      return Integer.MAX_VALUE; // unlimited, rely on maxBytesInMemory instead
    }

    @Override
    public long getMaxBytesInMemory()
    {
      return newMaxBytesInMemory;
    }

    @Override
    public boolean isSkipBytesInMemoryOverheadCheck()
    {
      return baseConfig.isSkipBytesInMemoryOverheadCheck();
    }

    @Override
    public int getMaxPendingPersists()
    {
      return baseConfig.getMaxPendingPersists();
    }

    @Nullable
    @Override
    public Integer getMaxRowsPerSegment()
    {
      return baseConfig.getMaxRowsPerSegment();
    }

    @Nullable
    @Override
    public Long getMaxTotalRows()
    {
      return baseConfig.getMaxTotalRows();
    }

    @Override
    public PartitionsSpec getPartitionsSpec()
    {
      return baseConfig.getPartitionsSpec();
    }

    @Override
    public Period getIntermediatePersistPeriod()
    {
      return baseConfig.getIntermediatePersistPeriod();
    }

    @Override
    public IndexSpec getIndexSpec()
    {
      return baseConfig.getIndexSpec();
    }

    @Override
    public IndexSpec getIndexSpecForIntermediatePersists()
    {
      return baseConfig.getIndexSpecForIntermediatePersists();
    }

    @Override
    public File getBasePersistDirectory()
    {
      return baseConfig.getBasePersistDirectory();
    }

    @Override
    public AppenderatorConfig withBasePersistDirectory(File basePersistDirectory)
    {
      return this;
    }

    @Nullable
    @Override
    public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
    {
      return baseConfig.getSegmentWriteOutMediumFactory();
    }

    @Override
    public int getNumPersistThreads()
    {
      return baseConfig.getNumPersistThreads();
    }
  }

  private IndexMerger wrapIndexMerger(IndexMerger baseMerger)
  {
    return new LimitedPoolIndexMerger(baseMerger, mergeExecutor);
  }


  /**
   * This wrapper around IndexMerger limits concurrent calls to the merge/persist methods used by
   * {@link StreamAppenderator} with a shared executor service. Merge/persist methods that are not used by
   * AppenderatorImpl will throw an exception if called.
   */
  public static class LimitedPoolIndexMerger implements IndexMerger
  {
    private static final String ERROR_MSG = "Shouldn't be called";

    private final IndexMerger baseMerger;

    private final ListeningExecutorService mergeExecutor;

    public LimitedPoolIndexMerger(
        IndexMerger baseMerger,
        ListeningExecutorService mergeExecutor
    )
    {
      this.baseMerger = baseMerger;
      this.mergeExecutor = mergeExecutor;
    }

    @Override
    public File persist(
        IncrementalIndex index,
        Interval dataInterval,
        File outDir,
        IndexSpec indexSpec,
        ProgressIndicator progress,
        @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory
    )
    {
      ListenableFuture<File> mergeFuture = mergeExecutor.submit(
          () ->
              baseMerger.persist(
                  index,
                  dataInterval,
                  outDir,
                  indexSpec,
                  progress,
                  segmentWriteOutMediumFactory
              )
      );

      try {
        return mergeFuture.get();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public File merge(
        List<IndexableAdapter> indexes,
        boolean rollup,
        AggregatorFactory[] metricAggs,
        File outDir,
        DimensionsSpec dimensionsSpec,
        IndexSpec indexSpec,
        int maxColumnsToMerge
    )
    {
      // Only used in certain tests. No need to implement.
      throw new UOE(ERROR_MSG);
    }

    @Override
    public File mergeQueryableIndex(
        List<QueryableIndex> indexes,
        boolean rollup,
        AggregatorFactory[] metricAggs,
        @Nullable DimensionsSpec dimensionsSpec,
        File outDir,
        IndexSpec indexSpec,
        IndexSpec indexSpecForIntermediatePersists,
        ProgressIndicator progress,
        @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
        int maxColumnsToMerge
    )
    {
      ListenableFuture<File> mergeFuture = mergeExecutor.submit(
          () ->
              baseMerger.mergeQueryableIndex(
                  indexes,
                  rollup,
                  metricAggs,
                  dimensionsSpec,
                  outDir,
                  indexSpec,
                  indexSpecForIntermediatePersists,
                  new BaseProgressIndicator(),
                  segmentWriteOutMediumFactory,
                  maxColumnsToMerge
              )
      );

      try {
        return mergeFuture.get();
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
