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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Provider;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.coordinator.CoordinatorClient;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.discovery.DataNodeService;
import org.apache.druid.discovery.DruidNodeAnnouncer;
import org.apache.druid.discovery.LookupNodeService;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskReportFileWriter;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTaskClientProvider;
import org.apache.druid.indexing.common.task.batch.parallel.ShuffleClient;
import org.apache.druid.indexing.worker.shuffle.IntermediaryDataManager;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9Factory;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.incremental.RowIngestionMetersFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.ChatHandlerProvider;
import org.apache.druid.segment.writeout.OnHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.tasklogs.TaskLogPusher;

public class TestTaskToolboxFactory extends TaskToolboxFactory
{
  /**
   * We use a constructor that takes a builder instead of having the builder build the object so that
   * implementations can override methods on this class if they need to.
   *
   * @param bob the builder
   */
  public TestTaskToolboxFactory(
      Builder bob
  )
  {
    super(
        null,
        bob.config,
        bob.taskExecutorNode,
        bob.taskActionClientFactory,
        bob.emitter,
        bob.segmentPusher,
        bob.dataSegmentKiller,
        bob.dataSegmentMover,
        bob.dataSegmentArchiver,
        bob.segmentAnnouncer,
        bob.serverAnnouncer,
        bob.handoffNotifierFactory,
        bob.queryRunnerFactoryConglomerateProvider,
        bob.queryProcessingPool,
        bob.joinableFactory,
        bob.monitorSchedulerProvider,
        bob.segmentCacheManagerFactory,
        bob.jsonMapper,
        bob.indexIO,
        bob.cache,
        bob.cacheConfig,
        bob.cachePopulatorStats,
        bob.indexMergerV9Factory,
        bob.druidNodeAnnouncer,
        bob.druidNode,
        bob.lookupNodeService,
        bob.dataNodeService,
        bob.taskReportFileWriter,
        bob.intermediaryDataManager,
        bob.authorizerMapper,
        bob.chatHandlerProvider,
        bob.rowIngestionMetersFactory,
        bob.appenderatorsManager,
        bob.overlordClient,
        bob.coordinatorClient,
        bob.supervisorTaskClientProvider,
        bob.shuffleClient,
        bob.taskLogPusher,
        bob.attemptId,
        bob.centralizedDatasourceSchemaConfig
    );
  }

  public static class Builder
  {
    private TaskConfig config = new TaskConfigBuilder().build();
    private DruidNode taskExecutorNode;
    private TaskActionClientFactory taskActionClientFactory = task -> null;
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
    private ObjectMapper jsonMapper = TestHelper.JSON_MAPPER;
    private IndexIO indexIO = TestHelper.getTestIndexIO();
    private SegmentCacheManagerFactory segmentCacheManagerFactory = new SegmentCacheManagerFactory(jsonMapper);
    private Cache cache;
    private CacheConfig cacheConfig;
    private CachePopulatorStats cachePopulatorStats;
    private IndexMergerV9Factory indexMergerV9Factory = new IndexMergerV9Factory(jsonMapper, indexIO, OnHeapMemorySegmentWriteOutMediumFactory.instance());
    private DruidNodeAnnouncer druidNodeAnnouncer;
    private DruidNode druidNode;
    private LookupNodeService lookupNodeService;
    private DataNodeService dataNodeService;
    private TaskReportFileWriter taskReportFileWriter = new NoopTestTaskReportFileWriter();
    private IntermediaryDataManager intermediaryDataManager;
    private AuthorizerMapper authorizerMapper;
    private ChatHandlerProvider chatHandlerProvider;
    private RowIngestionMetersFactory rowIngestionMetersFactory;
    private AppenderatorsManager appenderatorsManager;
    private OverlordClient overlordClient = new NoopOverlordClient();
    private CoordinatorClient coordinatorClient = new NoopCoordinatorClient();
    private ParallelIndexSupervisorTaskClientProvider supervisorTaskClientProvider;
    private ShuffleClient shuffleClient;
    private TaskLogPusher taskLogPusher;
    private String attemptId;
    private CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig;

    public Builder setConfig(TaskConfig config)
    {
      this.config = config;
      return this;
    }

    public Builder setTaskExecutorNode(DruidNode taskExecutorNode)
    {
      this.taskExecutorNode = taskExecutorNode;
      return this;
    }

    public Builder setTaskActionClientFactory(TaskActionClientFactory taskActionClientFactory)
    {
      this.taskActionClientFactory = taskActionClientFactory;
      return this;
    }

    public Builder setEmitter(ServiceEmitter emitter)
    {
      this.emitter = emitter;
      return this;
    }

    public Builder setSegmentPusher(DataSegmentPusher segmentPusher)
    {
      this.segmentPusher = segmentPusher;
      return this;
    }

    public Builder setDataSegmentKiller(DataSegmentKiller dataSegmentKiller)
    {
      this.dataSegmentKiller = dataSegmentKiller;
      return this;
    }

    public Builder setDataSegmentMover(DataSegmentMover dataSegmentMover)
    {
      this.dataSegmentMover = dataSegmentMover;
      return this;
    }

    public Builder setDataSegmentArchiver(DataSegmentArchiver dataSegmentArchiver)
    {
      this.dataSegmentArchiver = dataSegmentArchiver;
      return this;
    }

    public Builder setSegmentAnnouncer(DataSegmentAnnouncer segmentAnnouncer)
    {
      this.segmentAnnouncer = segmentAnnouncer;
      return this;
    }

    public Builder setServerAnnouncer(DataSegmentServerAnnouncer serverAnnouncer)
    {
      this.serverAnnouncer = serverAnnouncer;
      return this;
    }

    public Builder setHandoffNotifierFactory(SegmentHandoffNotifierFactory handoffNotifierFactory)
    {
      this.handoffNotifierFactory = handoffNotifierFactory;
      return this;
    }

    public Builder setQueryRunnerFactoryConglomerateProvider(Provider<QueryRunnerFactoryConglomerate> queryRunnerFactoryConglomerateProvider)
    {
      this.queryRunnerFactoryConglomerateProvider = queryRunnerFactoryConglomerateProvider;
      return this;
    }

    public Builder setQueryProcessingPool(QueryProcessingPool queryProcessingPool)
    {
      this.queryProcessingPool = queryProcessingPool;
      return this;
    }

    public Builder setJoinableFactory(JoinableFactory joinableFactory)
    {
      this.joinableFactory = joinableFactory;
      return this;
    }

    public Builder setMonitorSchedulerProvider(Provider<MonitorScheduler> monitorSchedulerProvider)
    {
      this.monitorSchedulerProvider = monitorSchedulerProvider;
      return this;
    }

    public Builder setSegmentCacheManagerFactory(SegmentCacheManagerFactory segmentCacheManagerFactory)
    {
      this.segmentCacheManagerFactory = segmentCacheManagerFactory;
      return this;
    }

    public Builder setJsonMapper(ObjectMapper jsonMapper)
    {
      this.jsonMapper = jsonMapper;
      return this;
    }

    public Builder setIndexIO(IndexIO indexIO)
    {
      this.indexIO = indexIO;
      return this;
    }

    public Builder setCache(Cache cache)
    {
      this.cache = cache;
      return this;
    }

    public Builder setCacheConfig(CacheConfig cacheConfig)
    {
      this.cacheConfig = cacheConfig;
      return this;
    }

    public Builder setCachePopulatorStats(CachePopulatorStats cachePopulatorStats)
    {
      this.cachePopulatorStats = cachePopulatorStats;
      return this;
    }

    public Builder setIndexMergerV9Factory(IndexMergerV9Factory indexMergerV9Factory)
    {
      this.indexMergerV9Factory = indexMergerV9Factory;
      return this;
    }

    public Builder setDruidNodeAnnouncer(DruidNodeAnnouncer druidNodeAnnouncer)
    {
      this.druidNodeAnnouncer = druidNodeAnnouncer;
      return this;
    }

    public Builder setDruidNode(DruidNode druidNode)
    {
      this.druidNode = druidNode;
      return this;
    }

    public Builder setLookupNodeService(LookupNodeService lookupNodeService)
    {
      this.lookupNodeService = lookupNodeService;
      return this;
    }

    public Builder setDataNodeService(DataNodeService dataNodeService)
    {
      this.dataNodeService = dataNodeService;
      return this;
    }

    public Builder setTaskReportFileWriter(TaskReportFileWriter taskReportFileWriter)
    {
      this.taskReportFileWriter = taskReportFileWriter;
      return this;
    }

    public Builder setIntermediaryDataManager(IntermediaryDataManager intermediaryDataManager)
    {
      this.intermediaryDataManager = intermediaryDataManager;
      return this;
    }

    public Builder setAuthorizerMapper(AuthorizerMapper authorizerMapper)
    {
      this.authorizerMapper = authorizerMapper;
      return this;
    }

    public Builder setChatHandlerProvider(ChatHandlerProvider chatHandlerProvider)
    {
      this.chatHandlerProvider = chatHandlerProvider;
      return this;
    }

    public Builder setRowIngestionMetersFactory(RowIngestionMetersFactory rowIngestionMetersFactory)
    {
      this.rowIngestionMetersFactory = rowIngestionMetersFactory;
      return this;
    }

    public Builder setAppenderatorsManager(AppenderatorsManager appenderatorsManager)
    {
      this.appenderatorsManager = appenderatorsManager;
      return this;
    }

    public Builder setOverlordClient(OverlordClient overlordClient)
    {
      this.overlordClient = overlordClient;
      return this;
    }

    public Builder setCoordinatorClient(CoordinatorClient coordinatorClient)
    {
      this.coordinatorClient = coordinatorClient;
      return this;
    }

    public Builder setSupervisorTaskClientProvider(ParallelIndexSupervisorTaskClientProvider supervisorTaskClientProvider)
    {
      this.supervisorTaskClientProvider = supervisorTaskClientProvider;
      return this;
    }

    public Builder setShuffleClient(ShuffleClient shuffleClient)
    {
      this.shuffleClient = shuffleClient;
      return this;
    }

    public Builder setTaskLogPusher(TaskLogPusher taskLogPusher)
    {
      this.taskLogPusher = taskLogPusher;
      return this;
    }

    public Builder setAttemptId(String attemptId)
    {
      this.attemptId = attemptId;
      return this;
    }

    public void setCentralizedTableSchemaConfig(CentralizedDatasourceSchemaConfig centralizedDatasourceSchemaConfig)
    {
      this.centralizedDatasourceSchemaConfig = centralizedDatasourceSchemaConfig;
    }
  }
}
