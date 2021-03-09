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
import org.apache.druid.com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.indexing.IndexingServiceClient;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Parent;
import org.apache.druid.guice.annotations.Processing;
import org.apache.druid.guice.annotations.RemoteChatHandler;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.IndexTaskClientFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClient;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.security.AuthorizerMapper;

import java.io.File;
import java.util.concurrent.ExecutorService;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolboxFactory
{
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
  private final ExecutorService queryExecutorService;
  private final JoinableFactory joinableFactory;
  private final Provider<MonitorScheduler> monitorSchedulerProvider;
  private final SegmentLoaderFactory segmentLoaderFactory;
  private final ObjectMapper jsonMapper;
  private final IndexIO indexIO;
  private final Cache cache;
  private final CacheConfig cacheConfig;
  private final CachePopulatorStats cachePopulatorStats;
  private final IndexMergerV9 indexMergerV9;
  private final DruidNodeAnnouncer druidNodeAnnouncer;
  private final DruidNode druidNode;
  private final LookupNodeService lookupNodeService;
  private final DataNodeService dataNodeService;
  private final TaskReportFileWriter taskReportFileWriter;
  private final AuthorizerMapper authorizerMapper;
  private final ChatHandlerProvider chatHandlerProvider;
  private final RowIngestionMetersFactory rowIngestionMetersFactory;
  private final AppenderatorsManager appenderatorsManager;
  private final IndexingServiceClient indexingServiceClient;
  private final CoordinatorClient coordinatorClient;

  // Used by only native parallel tasks
  private final IntermediaryDataManager intermediaryDataManager;
  private final IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> supervisorTaskClientFactory;
  private final ShuffleClient shuffleClient;

  @Inject
  public TaskToolboxFactory(
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
      @Processing ExecutorService queryExecutorService,
      JoinableFactory joinableFactory,
      Provider<MonitorScheduler> monitorSchedulerProvider,
      SegmentLoaderFactory segmentLoaderFactory,
      @Json ObjectMapper jsonMapper,
      IndexIO indexIO,
      Cache cache,
      CacheConfig cacheConfig,
      CachePopulatorStats cachePopulatorStats,
      IndexMergerV9 indexMergerV9,
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
      IndexingServiceClient indexingServiceClient,
      CoordinatorClient coordinatorClient,
      IndexTaskClientFactory<ParallelIndexSupervisorTaskClient> supervisorTaskClientFactory,
      ShuffleClient shuffleClient
  )
  {
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
    this.queryExecutorService = queryExecutorService;
    this.joinableFactory = joinableFactory;
    this.monitorSchedulerProvider = monitorSchedulerProvider;
    this.segmentLoaderFactory = segmentLoaderFactory;
    this.jsonMapper = jsonMapper;
    this.indexIO = Preconditions.checkNotNull(indexIO, "Null IndexIO");
    this.cache = cache;
    this.cacheConfig = cacheConfig;
    this.cachePopulatorStats = cachePopulatorStats;
    this.indexMergerV9 = indexMergerV9;
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
    this.indexingServiceClient = indexingServiceClient;
    this.coordinatorClient = coordinatorClient;
    this.supervisorTaskClientFactory = supervisorTaskClientFactory;
    this.shuffleClient = shuffleClient;
  }

  public TaskToolbox build(Task task)
  {
    final File taskWorkDir = config.getTaskWorkDir(task.getId());
    return new TaskToolbox(
        config,
        taskExecutorNode,
        taskActionClientFactory.create(task),
        emitter,
        segmentPusher,
        dataSegmentKiller,
        dataSegmentMover,
        dataSegmentArchiver,
        segmentAnnouncer,
        serverAnnouncer,
        handoffNotifierFactory,
        queryRunnerFactoryConglomerateProvider,
        queryExecutorService,
        joinableFactory,
        monitorSchedulerProvider,
        segmentLoaderFactory.manufacturate(taskWorkDir),
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
        indexingServiceClient,
        coordinatorClient,
        supervisorTaskClientFactory,
        shuffleClient
    );
  }
}
