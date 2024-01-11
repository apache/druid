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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.inject.Provider;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexing.common.actions.SegmentTransactionalInsertAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProvider;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentCacheManager;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.tasklogs.TaskLogPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.JvmUtils;
import org.apache.druid.utils.RuntimeInfo;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolbox
{
  private final SegmentLoaderConfig segmentLoaderConfig;
  private final TaskConfig config;
  private final DruidNode taskExecutorNode;
  private final TaskActionClient taskActionClient;
  private final ServiceEmitter emitter;
  private final DataSegmentPusher segmentPusher;
  private final DataSegmentKiller dataSegmentKiller;
  private final DataSegmentArchiver dataSegmentArchiver;
  private final DataSegmentMover dataSegmentMover;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentHandoffNotifierFactory handoffNotifierFactory;
  /**
   * Using Provider, not {@link QueryRunnerFactoryConglomerate} directly, to not require {@link
   * org.apache.druid.indexing.overlord.TaskRunner} implementations that create TaskToolboxes to inject query stuff eagerly,
   * because it may be unavailable, e. g. for batch tasks running in Spark or Hadoop.
   */
  private final Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider;
  @Nullable
  private final Provider<MonitorScheduler> monitorSchedulerProvider;
  private final QueryProcessingPool queryProcessingPool;
  private final JoinableFactory joinableFactory;
  private final SegmentCacheManager segmentCacheManager;
  private final ObjectMapper jsonMapper;
  private final File taskWorkDir;
  private final IndexIO indexIO;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;
  private final IndexMergerV9 indexMergerV9;
  private final TaskReportFileWriter taskReportFileWriter;

  private final DruidNodeAnnouncer druidNodeAnnouncer;
  private final DruidNode druidNode;
  private final LookupNodeService lookupNodeService;
  private final DataNodeService dataNodeService;

  private final AuthorizerMapper authorizerMapper;
  private final ChatHandlerProvider chatHandlerProvider;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final AppenderatorsManager appenderatorsManager;
  private final OverlordClient overlordClient;
  private final CoordinatorClient coordinatorClient;

  // Used by only native parallel tasks
  private final IntermediaryDataManager intermediaryDataManager;
  private final ParallelIndexSupervisorTaskClientProvider supervisorTaskClientProvider;
  private final ShuffleClient shuffleClient;

  private final TaskLogPusher taskLogPusher;
  private final String attemptId;
  private final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

  public TaskToolbox(
      SegmentLoaderConfig segmentLoaderConfig,
      TaskConfig config,
      DruidNode taskExecutorNode,
      TaskActionClient taskActionClient,
      ServiceEmitter emitter,
      DataSegmentPusher segmentPusher,
      DataSegmentKiller dataSegmentKiller,
      DataSegmentMover dataSegmentMover,
      DataSegmentArchiver dataSegmentArchiver,
      DataSegmentAnnouncer segmentAnnouncer,
      DataSegmentServerAnnouncer serverAnnouncer,
      SegmentHandoffNotifierFactory handoffNotifierFactory,
      Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider,
      QueryProcessingPool queryProcessingPool,
      JoinableFactory joinableFactory,
      @Nullable Provider<MonitorScheduler> monitorSchedulerProvider,
      SegmentCacheManager segmentCacheManager,
      ObjectMapper jsonMapper,
      File taskWorkDir,
      IndexIO indexIO,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      IndexMergerV9 indexMergerV9,
      DruidNodeAnnouncer druidNodeAnnouncer,
      DruidNode druidNode,
      LookupNodeService lookupNodeService,
      DataNodeService dataNodeService,
      TaskReportFileWriter taskReportFileWriter,
      IntermediaryDataManager intermediaryDataManager,
      AuthorizerMapper authorizerMapper,
      ChatHandlerProvider chatHandlerProvider,
      RowIngestionMetersFactory rowIngestionMetersFactory,
      AppenderatorsManager appenderatorsManager,
      OverlordClient overlordClient,
      CoordinatorClient coordinatorClient,
      ParallelIndexSupervisorTaskClientProvider supervisorTaskClientProvider,
      ShuffleClient shuffleClient,
      TaskLogPusher taskLogPusher,
      String attemptId,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    this.segmentLoaderConfig = segmentLoaderConfig;
    this.config = config;
    this.taskExecutorNode = taskExecutorNode;
    this.taskActionClient = taskActionClient;
    this.emitter = emitter;
    this.segmentPusher = segmentPusher;
    this.dataSegmentKiller = dataSegmentKiller;
    this.dataSegmentMover = dataSegmentMover;
    this.dataSegmentArchiver = dataSegmentArchiver;
    this.segmentAnnouncer = segmentAnnouncer;
    this.serverAnnouncer = serverAnnouncer;
    this.handoffNotifierFactory = handoffNotifierFactory;
    this.queryRunnerFactoryConglomerateProvider = queryRunnerFactoryConglomerateProvider;
    this.queryProcessingPool = queryProcessingPool;
    this.joinableFactory = joinableFactory;
    this.monitorSchedulerProvider = monitorSchedulerProvider;
    this.segmentCacheManager = segmentCacheManager;
    this.jsonMapper = jsonMapper;
    this.taskWorkDir = taskWorkDir;
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.cachePopulatorStats = cachePopulatorStats;
    this.indexMergerV9 = Preconditions.checkNotNull(indexMergerV9, "Null IndexMergerV9");
    this.druidNodeAnnouncer = druidNodeAnnouncer;
    this.druidNode = druidNode;
    this.lookupNodeService = lookupNodeService;
    this.dataNodeService = dataNodeService;
    this.taskReportFileWriter = taskReportFileWriter;
    this.taskReportFileWriter.setObjectMapper(this.jsonMapper);
    this.intermediaryDataManager = intermediaryDataManager;
    this.authorizerMapper = authorizerMapper;
    this.chatHandlerProvider = chatHandlerProvider;
    this.rowIngestionMetersFactory = rowIngestionMetersFactory;
    this.appenderatorsManager = appenderatorsManager;
    this.overlordClient = overlordClient;
    this.coordinatorClient = coordinatorClient;
    this.supervisorTaskClientProvider = supervisorTaskClientProvider;
    this.shuffleClient = shuffleClient;
    this.taskLogPusher = taskLogPusher;
    this.attemptId = attemptId;
    this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
  }

  public SegmentLoaderConfig getSegmentLoaderConfig()
  {
    return segmentLoaderConfig;
  }

  public TaskConfig getConfig()
  {
    return config;
  }

  public DruidNode getTaskExecutorNode()
  {
    return taskExecutorNode;
  }

  public TaskActionClient getTaskActionClient()
  {
    return taskActionClient;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public DataSegmentPusher getSegmentPusher()
  {
    return segmentPusher;
  }

  public DataSegmentKiller getDataSegmentKiller()
  {
    return dataSegmentKiller;
  }

  public DataSegmentMover getDataSegmentMover()
  {
    return dataSegmentMover;
  }

  public DataSegmentArchiver getDataSegmentArchiver()
  {
    return dataSegmentArchiver;
  }

  public DataSegmentAnnouncer getSegmentAnnouncer()
  {
    return segmentAnnouncer;
  }

  public DataSegmentServerAnnouncer getDataSegmentServerAnnouncer()
  {
    return serverAnnouncer;
  }

  public SegmentHandoffNotifierFactory getSegmentHandoffNotifierFactory()
  {
    return handoffNotifierFactory;
  }

  public QueryRunnerFactoryConglomerate getQueryRunnerFactoryConglomerate()
  {
    return queryRunnerFactoryConglomerateProvider.get();
  }

  public QueryProcessingPool getQueryProcessingPool()
  {
    return queryProcessingPool;
  }

  public JoinableFactory getJoinableFactory()
  {
    return joinableFactory;
  }

  @Nullable
  public MonitorScheduler getMonitorScheduler()
  {
    return monitorSchedulerProvider == null ? null : monitorSchedulerProvider.get();
  }

  /**
   * Adds a monitor to the monitorScheduler if it is configured
   *
   * @param monitor
   */
  public void addMonitor(Monitor monitor)
  {
    MonitorScheduler scheduler = getMonitorScheduler();
    if (scheduler != null) {
      scheduler.addMonitor(monitor);
    }
  }

  /**
   * Adds a monitor to the monitorScheduler if it is configured
   *
   * @param monitor
   */
  public void removeMonitor(Monitor monitor)
  {
    MonitorScheduler scheduler = getMonitorScheduler();
    if (scheduler != null) {
      scheduler.removeMonitor(monitor);
    }
  }

  public ObjectMapper getJsonMapper()
  {
    return jsonMapper;
  }

  public SegmentCacheManager getSegmentCacheManager()
  {
    return segmentCacheManager;
  }

  public void publishSegments(Iterable<DataSegment> segments) throws IOException
  {
    // Request segment pushes for each set
    final Multimap<Interval, DataSegment> segmentMultimap = Multimaps.index(
        segments,
        DataSegment::getInterval
    );
    for (final Collection<DataSegment> segmentCollection : segmentMultimap.asMap().values()) {
      getTaskActionClient().submit(
          SegmentTransactionalInsertAction.appendAction(
              ImmutableSet.copyOf(segmentCollection), null, null
          )
      );
    }
  }

  public IndexIO getIndexIO()
  {
    return indexIO;
  }

  public Cache getCache()
  {
    return cache;
  }

  public CacheConfig getCacheConfig()
  {
    return cacheConfig;
  }

  public CachePopulatorStats getCachePopulatorStats()
  {
    return cachePopulatorStats;
  }

  public IndexMergerV9 getIndexMergerV9()
  {
    return indexMergerV9;
  }

  public File getIndexingTmpDir()
  {
    final File tmpDir = new File(taskWorkDir, "indexing-tmp");
    try {
      FileUtils.mkdirp(tmpDir);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    return tmpDir;
  }

  public File getMergeDir()
  {
    return new File(taskWorkDir, "merge");
  }

  public File getPersistDir()
  {
    return new File(taskWorkDir, "persist");
  }

  public DruidNodeAnnouncer getDruidNodeAnnouncer()
  {
    return druidNodeAnnouncer;
  }

  public LookupNodeService getLookupNodeService()
  {
    return lookupNodeService;
  }

  public DataNodeService getDataNodeService()
  {
    return dataNodeService;
  }

  public DruidNode getDruidNode()
  {
    return druidNode;
  }

  public TaskReportFileWriter getTaskReportFileWriter()
  {
    return taskReportFileWriter;
  }

  public IntermediaryDataManager getIntermediaryDataManager()
  {
    return intermediaryDataManager;
  }

  public AuthorizerMapper getAuthorizerMapper()
  {
    return authorizerMapper;
  }

  public ChatHandlerProvider getChatHandlerProvider()
  {
    return chatHandlerProvider;
  }

  public RowIngestionMetersFactory getRowIngestionMetersFactory()
  {
    return rowIngestionMetersFactory;
  }

  public AppenderatorsManager getAppenderatorsManager()
  {
    return appenderatorsManager;
  }

  public OverlordClient getOverlordClient()
  {
    return overlordClient;
  }

  public CoordinatorClient getCoordinatorClient()
  {
    return coordinatorClient;
  }

  public ParallelIndexSupervisorTaskClientProvider getSupervisorTaskClientProvider()
  {
    return supervisorTaskClientProvider;
  }

  public ShuffleClient getShuffleClient()
  {
    return shuffleClient;
  }

  public TaskLogPusher getTaskLogPusher()
  {
    return taskLogPusher;
  }

  public String getAttemptId()
  {
    return attemptId;
  }

  /**
   * Get {@link RuntimeInfo} adjusted for this particular task. When running in a task JVM launched by a MiddleManager,
   * this is the same as the baseline {@link RuntimeInfo}. When running in an Indexer, it is adjusted based on
   * {@code druid.worker.capacity}.
   */
  public RuntimeInfo getAdjustedRuntimeInfo()
  {
    return createAdjustedRuntimeInfo(JvmUtils.getRuntimeInfo(), appenderatorsManager);
  }

  public CentralizedDatasourceSchemaConfig getCentralizedTableSchemaConfig()
  {
    return centralizedDatasourceSchemaConfig;
  }

  /**
   * Create {@link AdjustedRuntimeInfo} based on the given {@link RuntimeInfo} and {@link AppenderatorsManager}. This
   * is a way to allow code to properly apportion the amount of processors and heap available to the entire JVM.
   * When running in an Indexer, other tasks share the same JVM, so this must be accounted for.
   */
  public static RuntimeInfo createAdjustedRuntimeInfo(
      final RuntimeInfo runtimeInfo,
      final AppenderatorsManager appenderatorsManager
  )
  {
    if (appenderatorsManager instanceof UnifiedIndexerAppenderatorsManager) {
      // CliIndexer. Each JVM runs multiple tasks; adjust.
      return new AdjustedRuntimeInfo(
          runtimeInfo,
          ((UnifiedIndexerAppenderatorsManager) appenderatorsManager).getWorkerConfig().getCapacity()
      );
    } else {
      // CliPeon (assumed to be launched by CliMiddleManager).
      // Each JVM runs a single task. ForkingTaskRunner sets XX:ActiveProcessorCount so each task already sees
      // an adjusted number of processors from the baseline RuntimeInfo. So, we return it directly.
      return runtimeInfo;
    }
  }

  public static class Builder
  {
    private SegmentLoaderConfig segmentLoaderConfig;
    private TaskConfig config;
    private DruidNode taskExecutorNode;
    private TaskActionClient taskActionClient;
    private ServiceEmitter emitter;
    private DataSegmentPusher segmentPusher;
    private DataSegmentKiller dataSegmentKiller;
    private DataSegmentMover dataSegmentMover;
    private DataSegmentArchiver dataSegmentArchiver;
    private DataSegmentAnnouncer segmentAnnouncer;
    private DataSegmentServerAnnouncer serverAnnouncer;
    private SegmentHandoffNotifierFactory handoffNotifierFactory;
    private Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider;
    private QueryProcessingPool queryProcessingPool;
    private JoinableFactory joinableFactory;
    private Provider<MonitorScheduler> monitorSchedulerProvider;
    private SegmentCacheManager segmentCacheManager;
    private ObjectMapper jsonMapper;
    private File taskWorkDir;
    private IndexIO indexIO;
    private Cache cache;
    private CacheConfig cacheConfig;
    private CachePopulatorStats cachePopulatorStats;
    private IndexMergerV9 indexMergerV9;
    private DruidNodeAnnouncer druidNodeAnnouncer;
    private DruidNode druidNode;
    private LookupNodeService lookupNodeService;
    private DataNodeService dataNodeService;
    private TaskReportFileWriter taskReportFileWriter;
    private AuthorizerMapper authorizerMapper;
    private ChatHandlerProvider chatHandlerProvider;
    private RowIngestionMetersFactory rowIngestionMetersFactory;
    private AppenderatorsManager appenderatorsManager;
    private OverlordClient overlordClient;
    private CoordinatorClient coordinatorClient;
    private IntermediaryDataManager intermediaryDataManager;
    private ParallelIndexSupervisorTaskClientProvider supervisorTaskClientProvider;
    private ShuffleClient shuffleClient;
    private TaskLogPusher taskLogPusher;
    private String attemptId;
    private CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

    public Builder()
    {
    }

    public Builder(TaskToolbox other)
    {
      this.segmentLoaderConfig = other.segmentLoaderConfig;
      this.config = other.config;
      this.taskExecutorNode = other.taskExecutorNode;
      this.taskActionClient = other.taskActionClient;
      this.emitter = other.emitter;
      this.segmentPusher = other.segmentPusher;
      this.dataSegmentKiller = other.dataSegmentKiller;
      this.dataSegmentMover = other.dataSegmentMover;
      this.dataSegmentArchiver = other.dataSegmentArchiver;
      this.segmentAnnouncer = other.segmentAnnouncer;
      this.serverAnnouncer = other.serverAnnouncer;
      this.handoffNotifierFactory = other.handoffNotifierFactory;
      this.queryRunnerFactoryConglomerateProvider = other.queryRunnerFactoryConglomerateProvider;
      this.queryProcessingPool = other.queryProcessingPool;
      this.joinableFactory = other.joinableFactory;
      this.monitorSchedulerProvider = other.monitorSchedulerProvider;
      this.segmentCacheManager = other.segmentCacheManager;
      this.jsonMapper = other.jsonMapper;
      this.taskWorkDir = other.taskWorkDir;
      this.indexIO = other.indexIO;
      this.cache = other.cache;
      this.cacheConfig = other.cacheConfig;
      this.cachePopulatorStats = other.cachePopulatorStats;
      this.indexMergerV9 = other.indexMergerV9;
      this.druidNodeAnnouncer = other.druidNodeAnnouncer;
      this.druidNode = other.druidNode;
      this.lookupNodeService = other.lookupNodeService;
      this.dataNodeService = other.dataNodeService;
      this.taskReportFileWriter = other.taskReportFileWriter;
      this.authorizerMapper = other.authorizerMapper;
      this.chatHandlerProvider = other.chatHandlerProvider;
      this.rowIngestionMetersFactory = other.rowIngestionMetersFactory;
      this.appenderatorsManager = other.appenderatorsManager;
      this.overlordClient = other.overlordClient;
      this.coordinatorClient = other.coordinatorClient;
      this.intermediaryDataManager = other.intermediaryDataManager;
      this.supervisorTaskClientProvider = other.supervisorTaskClientProvider;
      this.shuffleClient = other.shuffleClient;
      this.centralizedDatasourceSchemaConfig = other.centralizedDatasourceSchemaConfig;
    }

    public Builder config(final SegmentLoaderConfig segmentLoaderConfig)
    {
      this.segmentLoaderConfig = segmentLoaderConfig;
      return this;
    }

    public Builder config(final TaskConfig config)
    {
      this.config = config;
      return this;
    }

    public Builder taskExecutorNode(final DruidNode taskExecutorNode)
    {
      this.taskExecutorNode = taskExecutorNode;
      return this;
    }

    public Builder taskActionClient(final TaskActionClient taskActionClient)
    {
      this.taskActionClient = taskActionClient;
      return this;
    }

    public Builder emitter(final ServiceEmitter emitter)
    {
      this.emitter = emitter;
      return this;
    }

    public Builder segmentPusher(final DataSegmentPusher segmentPusher)
    {
      this.segmentPusher = segmentPusher;
      return this;
    }

    public Builder dataSegmentKiller(final DataSegmentKiller dataSegmentKiller)
    {
      this.dataSegmentKiller = dataSegmentKiller;
      return this;
    }

    public Builder dataSegmentMover(final DataSegmentMover dataSegmentMover)
    {
      this.dataSegmentMover = dataSegmentMover;
      return this;
    }

    public Builder dataSegmentArchiver(final DataSegmentArchiver dataSegmentArchiver)
    {
      this.dataSegmentArchiver = dataSegmentArchiver;
      return this;
    }

    public Builder segmentAnnouncer(final DataSegmentAnnouncer segmentAnnouncer)
    {
      this.segmentAnnouncer = segmentAnnouncer;
      return this;
    }

    public Builder serverAnnouncer(final DataSegmentServerAnnouncer serverAnnouncer)
    {
      this.serverAnnouncer = serverAnnouncer;
      return this;
    }

    public Builder handoffNotifierFactory(final SegmentHandoffNotifierFactory handoffNotifierFactory)
    {
      this.handoffNotifierFactory = handoffNotifierFactory;
      return this;
    }

    public Builder queryRunnerFactoryConglomerateProvider(final Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider)
    {
      this.queryRunnerFactoryConglomerateProvider = queryRunnerFactoryConglomerateProvider;
      return this;
    }

    public Builder queryProcessingPool(final QueryProcessingPool queryProcessingPool)
    {
      this.queryProcessingPool = queryProcessingPool;
      return this;
    }

    public Builder joinableFactory(final JoinableFactory joinableFactory)
    {
      this.joinableFactory = joinableFactory;
      return this;
    }

    public Builder monitorSchedulerProvider(final Provider<MonitorScheduler> monitorSchedulerProvider)
    {
      this.monitorSchedulerProvider = monitorSchedulerProvider;
      return this;
    }

    public Builder segmentCacheManager(final SegmentCacheManager segmentCacheManager)
    {
      this.segmentCacheManager = segmentCacheManager;
      return this;
    }

    public Builder jsonMapper(final ObjectMapper jsonMapper)
    {
      this.jsonMapper = jsonMapper;
      return this;
    }

    public Builder taskWorkDir(final File taskWorkDir)
    {
      this.taskWorkDir = taskWorkDir;
      return this;
    }

    public Builder indexIO(final IndexIO indexIO)
    {
      this.indexIO = indexIO;
      return this;
    }

    public Builder cache(final Cache cache)
    {
      this.cache = cache;
      return this;
    }

    public Builder cacheConfig(final CacheConfig cacheConfig)
    {
      this.cacheConfig = cacheConfig;
      return this;
    }

    public Builder cachePopulatorStats(final CachePopulatorStats cachePopulatorStats)
    {
      this.cachePopulatorStats = cachePopulatorStats;
      return this;
    }

    public Builder indexMergerV9(final IndexMergerV9 indexMergerV9)
    {
      this.indexMergerV9 = indexMergerV9;
      return this;
    }

    public Builder druidNodeAnnouncer(final DruidNodeAnnouncer druidNodeAnnouncer)
    {
      this.druidNodeAnnouncer = druidNodeAnnouncer;
      return this;
    }

    public Builder druidNode(final DruidNode druidNode)
    {
      this.druidNode = druidNode;
      return this;
    }

    public Builder lookupNodeService(final LookupNodeService lookupNodeService)
    {
      this.lookupNodeService = lookupNodeService;
      return this;
    }

    public Builder dataNodeService(final DataNodeService dataNodeService)
    {
      this.dataNodeService = dataNodeService;
      return this;
    }

    public Builder taskReportFileWriter(final TaskReportFileWriter taskReportFileWriter)
    {
      this.taskReportFileWriter = taskReportFileWriter;
      return this;
    }

    public Builder authorizerMapper(final AuthorizerMapper authorizerMapper)
    {
      this.authorizerMapper = authorizerMapper;
      return this;
    }

    public Builder chatHandlerProvider(final ChatHandlerProvider chatHandlerProvider)
    {
      this.chatHandlerProvider = chatHandlerProvider;
      return this;
    }

    public Builder rowIngestionMetersFactory(final RowIngestionMetersFactory rowIngestionMetersFactory)
    {
      this.rowIngestionMetersFactory = rowIngestionMetersFactory;
      return this;
    }

    public Builder appenderatorsManager(final AppenderatorsManager appenderatorsManager)
    {
      this.appenderatorsManager = appenderatorsManager;
      return this;
    }

    public Builder overlordClient(final OverlordClient overlordClient)
    {
      this.overlordClient = overlordClient;
      return this;
    }

    public Builder coordinatorClient(final CoordinatorClient coordinatorClient)
    {
      this.coordinatorClient = coordinatorClient;
      return this;
    }

    public Builder intermediaryDataManager(final IntermediaryDataManager intermediaryDataManager)
    {
      this.intermediaryDataManager = intermediaryDataManager;
      return this;
    }

    public Builder supervisorTaskClientProvider(final ParallelIndexSupervisorTaskClientProvider supervisorTaskClientProvider)
    {
      this.supervisorTaskClientProvider = supervisorTaskClientProvider;
      return this;
    }

    public Builder shuffleClient(final ShuffleClient shuffleClient)
    {
      this.shuffleClient = shuffleClient;
      return this;
    }

    public Builder taskLogPusher(final TaskLogPusher taskLogPusher)
    {
      this.taskLogPusher = taskLogPusher;
      return this;
    }

    public Builder attemptId(final String attemptId)
    {
      this.attemptId = attemptId;
      return this;
    }

    public Builder centralizedTableSchemaConfig(final CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
    {
      this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
      return this;
    }

    public TaskToolbox build()
    {
      return new TaskToolbox(
          segmentLoaderConfig,
          config,
          taskExecutorNode,
          taskActionClient,
          emitter,
          segmentPusher,
          dataSegmentKiller,
          dataSegmentMover,
          dataSegmentArchiver,
          segmentAnnouncer,
          serverAnnouncer,
          handoffNotifierFactory,
          queryRunnerFactoryConglomerateProvider,
          queryProcessingPool,
          joinableFactory,
          monitorSchedulerProvider,
          segmentCacheManager,
          jsonMapper,
          taskWorkDir,
          indexIO,
          cache,
          cacheConfig,
          cachePopulatorStats,
          indexMergerV9,
          druidNodeAnnouncer,
          druidNode,
          lookupNodeService,
          dataNodeService,
          taskReportFileWriter,
          intermediaryDataManager,
          authorizerMapper,
          chatHandlerProvider,
          rowIngestionMetersFactory,
          appenderatorsManager,
          overlordClient,
          coordinatorClient,
          supervisorTaskClientProvider,
          shuffleClient,
          taskLogPusher,
          attemptId,
          centralizedDatasourceSchemaConfig
      );
    }
  }
}
