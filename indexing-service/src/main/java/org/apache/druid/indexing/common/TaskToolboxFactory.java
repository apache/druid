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
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.guice.annotations.AttemptId;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Parent;
import org.apache.druid.guice.annotations.RemoteChatHandler;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProvider;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9Factory;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.tasklogs.TaskLogPusher;

import java.io.File;
import java.util.function.Function;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolboxFactory
{
  private final SegmentLoaderConfig segmentLoaderConfig;
  private final TaskConfig config;
  private final DruidNode taskExecutorNode;
  private final TaskActionClientFactory taskActionClientFactory;
  private final ServiceEmitter emitter;
  private final DataSegmentPusher segmentPusher;
  private final DataSegmentKiller dataSegmentKiller;
  private final DataSegmentMover dataSegmentMover;
  private final DataSegmentArchiver dataSegmentArchiver;
  private final DataSegmentAnnouncer segmentAnnouncer;
  private final DataSegmentServerAnnouncer serverAnnouncer;
  private final SegmentHandoffNotifierFactory handoffNotifierFactory;
  private final Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider;
  private final QueryProcessingPool queryProcessingPool;
  private final JoinableFactory joinableFactory;
  private final Provider<MonitorScheduler> monitorSchedulerProvider;
  private final SegmentCacheManagerFactory segmentCacheManagerFactory;
  private final ObjectMapper jsonMapper;
  private final IndexIO indexIO;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;
  private final IndexMergerV9Factory indexMergerV9Factory;
  private final DruidNodeAnnouncer druidNodeAnnouncer;
  private final DruidNode druidNode;
  private final LookupNodeService lookupNodeService;
  private final DataNodeService dataNodeService;
  private final TaskReportFileWriter taskReportFileWriter;
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

  @Inject
  public TaskToolboxFactory(
      SegmentLoaderConfig segmentLoadConfig,
      TaskConfig config,
      @Parent DruidNode taskExecutorNode,
      TaskActionClientFactory taskActionClientFactory,
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
      Provider<MonitorScheduler> monitorSchedulerProvider,
      SegmentCacheManagerFactory segmentCacheManagerFactory,
      @Json ObjectMapper jsonMapper,
      IndexIO indexIO,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      IndexMergerV9Factory indexMergerV9Factory,
      DruidNodeAnnouncer druidNodeAnnouncer,
      @RemoteChatHandler DruidNode druidNode,
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
      @AttemptId String attemptId,
      CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig
  )
  {
    this.segmentLoaderConfig = segmentLoadConfig;
    this.config = config;
    this.taskExecutorNode = taskExecutorNode;
    this.taskActionClientFactory = taskActionClientFactory;
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
    this.segmentCacheManagerFactory = segmentCacheManagerFactory;
    this.jsonMapper = jsonMapper;
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.cachePopulatorStats = cachePopulatorStats;
    this.indexMergerV9Factory = indexMergerV9Factory;
    this.druidNodeAnnouncer = druidNodeAnnouncer;
    this.druidNode = druidNode;
    this.lookupNodeService = lookupNodeService;
    this.dataNodeService = dataNodeService;
    this.taskReportFileWriter = taskReportFileWriter;
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

  public TaskToolbox build(Task task)
  {
    return build(config, task);
  }

  public TaskToolbox build(Function<TaskConfig, TaskConfig> decoratorFn, Task task)
  {
    return build(decoratorFn.apply(config), task);
  }

  public TaskToolbox build(TaskConfig config, Task task)
  {
    final File taskWorkDir = config.getTaskWorkDir(task.getId());
    return new TaskToolbox.Builder()
        .config(config)
        .config(segmentLoaderConfig)
        .taskExecutorNode(taskExecutorNode)
        .taskActionClient(taskActionClientFactory.create(task))
        .emitter(emitter)
        .segmentPusher(segmentPusher)
        .dataSegmentKiller(dataSegmentKiller)
        .dataSegmentMover(dataSegmentMover)
        .dataSegmentArchiver(dataSegmentArchiver)
        .segmentAnnouncer(segmentAnnouncer)
        .serverAnnouncer(serverAnnouncer)
        .handoffNotifierFactory(handoffNotifierFactory)
        .queryRunnerFactoryConglomerateProvider(queryRunnerFactoryConglomerateProvider)
        .queryProcessingPool(queryProcessingPool)
        .joinableFactory(joinableFactory)
        .monitorSchedulerProvider(monitorSchedulerProvider)
        .segmentCacheManager(segmentCacheManagerFactory.manufacturate(taskWorkDir))
        .jsonMapper(jsonMapper)
        .taskWorkDir(taskWorkDir)
        .indexIO(indexIO)
        .cache(cache)
        .cacheConfig(cacheConfig)
        .cachePopulatorStats(cachePopulatorStats)
        .indexMergerV9(
            indexMergerV9Factory.create(
                task.getContextValue(Tasks.STORE_EMPTY_COLUMNS_KEY, config.isStoreEmptyColumns())
            )
        )
        .druidNodeAnnouncer(druidNodeAnnouncer)
        .druidNode(druidNode)
        .lookupNodeService(lookupNodeService)
        .dataNodeService(dataNodeService)
        .taskReportFileWriter(taskReportFileWriter)
        .intermediaryDataManager(intermediaryDataManager)
        .authorizerMapper(authorizerMapper)
        .chatHandlerProvider(chatHandlerProvider)
        .rowIngestionMetersFactory(rowIngestionMetersFactory)
        .appenderatorsManager(appenderatorsManager)
        // Most tasks are written in such a way that if an Overlord or Coordinator RPC fails, the task fails.
        // Set the retry policy to "about an hour", so tasks are resilient to brief Coordinator/Overlord problems.
        // Calls will still eventually fail if problems persist.
        .overlordClient(overlordClient.withRetryPolicy(StandardRetryPolicy.aboutAnHour()))
        .coordinatorClient(coordinatorClient.withRetryPolicy(StandardRetryPolicy.aboutAnHour()))
        .supervisorTaskClientProvider(supervisorTaskClientProvider)
        .shuffleClient(shuffleClient)
        .taskLogPusher(taskLogPusher)
        .attemptId(attemptId)
        .centralizedTableSchemaConfig(centralizedDatasourceSchemaConfig)
        .build();
  }
}
