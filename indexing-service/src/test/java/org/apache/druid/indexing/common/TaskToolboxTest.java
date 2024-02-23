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
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.stats.DropwizardRowIngestionMetersFactory;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.IndexMergerV9Factory;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.SegmentLocalCacheManager;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorsManager;
import org.apache.druid.segment.realtime.appenderator.UnifiedIndexerAppenderatorsManager;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.utils.RuntimeInfo;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.Mockito;

import java.io.IOException;

@SuppressWarnings("DoNotMock")
public class TaskToolboxTest
{

  private TaskToolboxFactory taskToolbox = null;
  private TaskActionClientFactory mockTaskActionClientFactory = EasyMock.createMock(TaskActionClientFactory.class);
  private ServiceEmitter mockEmitter = EasyMock.createMock(ServiceEmitter.class);
  private DataSegmentPusher mockSegmentPusher = EasyMock.createMock(DataSegmentPusher.class);
  private DataSegmentKiller mockDataSegmentKiller = EasyMock.createMock(DataSegmentKiller.class);
  private DataSegmentMover mockDataSegmentMover = EasyMock.createMock(DataSegmentMover.class);
  private DataSegmentArchiver mockDataSegmentArchiver = EasyMock.createMock(DataSegmentArchiver.class);
  private DataSegmentAnnouncer mockSegmentAnnouncer = EasyMock.createMock(DataSegmentAnnouncer.class);
  private SegmentHandoffNotifierFactory mockHandoffNotifierFactory = EasyMock.createNiceMock(
      SegmentHandoffNotifierFactory.class
  );
  private QueryRunnerFactoryConglomerate mockQueryRunnerFactoryConglomerate
      = EasyMock.createMock(QueryRunnerFactoryConglomerate.class);
  private MonitorScheduler mockMonitorScheduler = EasyMock.createMock(MonitorScheduler.class);
  private QueryProcessingPool mockQueryProcessingPool = EasyMock.createMock(QueryProcessingPool.class);
  private ObjectMapper ObjectMapper = new ObjectMapper();
  private SegmentCacheManagerFactory mockSegmentCacheManagerFactory = EasyMock.createMock(SegmentCacheManagerFactory.class);
  private SegmentLocalCacheManager mockSegmentLoaderLocalCacheManager = EasyMock.createMock(SegmentLocalCacheManager.class);
  private Task task = EasyMock.createMock(Task.class);
  private IndexMergerV9Factory mockIndexMergerV9 = EasyMock.createMock(IndexMergerV9Factory.class);
  private IndexIO mockIndexIO = EasyMock.createMock(IndexIO.class);
  private Cache mockCache = EasyMock.createMock(Cache.class);
  private CacheConfig mockCacheConfig = EasyMock.createMock(CacheConfig.class);
  private SegmentLoaderConfig segmentLoaderConfig = EasyMock.createMock(SegmentLoaderConfig.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    EasyMock.expect(task.getId()).andReturn("task_id").anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn("task_ds").anyTimes();
    EasyMock.expect(task.getContextValue(Tasks.STORE_EMPTY_COLUMNS_KEY, true)).andReturn(true).anyTimes();
    IndexMergerV9 indexMergerV9 = EasyMock.createMock(IndexMergerV9.class);
    EasyMock.expect(mockIndexMergerV9.create(true)).andReturn(indexMergerV9).anyTimes();
    EasyMock.replay(task, mockHandoffNotifierFactory, mockIndexMergerV9);

    TaskConfig taskConfig = new TaskConfigBuilder()
        .setBaseDir(temporaryFolder.newFile().toString())
        .setDefaultRowFlushBoundary(50000)
        .setBatchProcessingMode(TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name())
        .build();

    taskToolbox = new TaskToolboxFactory(
        segmentLoaderConfig,
        taskConfig,
        new DruidNode("druid/middlemanager", "localhost", false, 8091, null, true, false),
        mockTaskActionClientFactory,
        mockEmitter,
        mockSegmentPusher,
        mockDataSegmentKiller,
        mockDataSegmentMover,
        mockDataSegmentArchiver,
        mockSegmentAnnouncer,
        EasyMock.createNiceMock(DataSegmentServerAnnouncer.class),
        mockHandoffNotifierFactory,
        () -> mockQueryRunnerFactoryConglomerate,
        mockQueryProcessingPool,
        NoopJoinableFactory.INSTANCE,
        () -> mockMonitorScheduler,
        mockSegmentCacheManagerFactory,
        ObjectMapper,
        mockIndexIO,
        mockCache,
        mockCacheConfig,
        new CachePopulatorStats(),
        mockIndexMergerV9,
        null,
        null,
        null,
        null,
        new NoopTestTaskReportFileWriter(),
        null,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        new NoopChatHandlerProvider(),
        new DropwizardRowIngestionMetersFactory(),
        new TestAppenderatorsManager(),
        new NoopOverlordClient(),
        new NoopCoordinatorClient(),
        null,
        null,
        null,
        "1",
        CentralizedDatasourceSchemaConfig.create()
    );
  }

  @Test
  public void testGetDataSegmentArchiver()
  {
    Assert.assertEquals(mockDataSegmentArchiver, taskToolbox.build(task).getDataSegmentArchiver());
  }

  @Test
  public void testGetSegmentLoaderConfig()
  {
    Assert.assertEquals(segmentLoaderConfig, taskToolbox.build(task).getSegmentLoaderConfig());
  }

  @Test
  public void testGetSegmentAnnouncer()
  {
    Assert.assertEquals(mockSegmentAnnouncer, taskToolbox.build(task).getSegmentAnnouncer());
  }

  @Test
  public void testGetQueryRunnerFactoryConglomerate()
  {
    Assert.assertEquals(
        mockQueryRunnerFactoryConglomerate,
        taskToolbox.build(task).getQueryRunnerFactoryConglomerate()
    );
  }

  @Test
  public void testGetQueryProcessingPool()
  {
    Assert.assertEquals(mockQueryProcessingPool, taskToolbox.build(task).getQueryProcessingPool());
  }

  @Test
  public void testGetMonitorScheduler()
  {
    Assert.assertEquals(mockMonitorScheduler, taskToolbox.build(task).getMonitorScheduler());
  }

  @Test
  public void testGetObjectMapper()
  {
    Assert.assertEquals(ObjectMapper, taskToolbox.build(task).getJsonMapper());
  }

  @Test
  public void testGetEmitter()
  {
    Assert.assertEquals(mockEmitter, taskToolbox.build(task).getEmitter());
  }

  @Test
  public void testGetDataSegmentKiller()
  {
    Assert.assertEquals(mockDataSegmentKiller, taskToolbox.build(task).getDataSegmentKiller());
  }

  @Test
  public void testGetDataSegmentMover()
  {
    Assert.assertEquals(mockDataSegmentMover, taskToolbox.build(task).getDataSegmentMover());
  }

  @Test
  public void testGetCache()
  {
    Assert.assertEquals(mockCache, taskToolbox.build(task).getCache());
  }

  @Test
  public void testGetCacheConfig()
  {
    Assert.assertEquals(mockCacheConfig, taskToolbox.build(task).getCacheConfig());
  }

  @Test
  public void testCreateAdjustedRuntimeInfoForMiddleManager()
  {
    final AppenderatorsManager appenderatorsManager = Mockito.mock(AppenderatorsManager.class);

    final DruidProcessingConfigTest.MockRuntimeInfo runtimeInfo =
        new DruidProcessingConfigTest.MockRuntimeInfo(12, 1_000_000, 2_000_000);
    final RuntimeInfo adjustedRuntimeInfo = TaskToolbox.createAdjustedRuntimeInfo(runtimeInfo, appenderatorsManager);

    Assert.assertEquals(
        runtimeInfo.getAvailableProcessors(),
        adjustedRuntimeInfo.getAvailableProcessors()
    );

    Assert.assertEquals(
        runtimeInfo.getMaxHeapSizeBytes(),
        adjustedRuntimeInfo.getMaxHeapSizeBytes()
    );

    Assert.assertEquals(
        runtimeInfo.getDirectMemorySizeBytes(),
        adjustedRuntimeInfo.getDirectMemorySizeBytes()
    );

    Mockito.verifyNoMoreInteractions(appenderatorsManager);
  }

  @Test
  public void testCreateAdjustedRuntimeInfoForIndexer()
  {
    // UnifiedIndexerAppenderatorsManager class is used on Indexers.
    final UnifiedIndexerAppenderatorsManager appenderatorsManager =
        Mockito.mock(UnifiedIndexerAppenderatorsManager.class);

    final int numWorkers = 3;
    final DruidProcessingConfigTest.MockRuntimeInfo runtimeInfo =
        new DruidProcessingConfigTest.MockRuntimeInfo(12, 1_000_000, 2_000_000);

    Mockito.when(appenderatorsManager.getWorkerConfig()).thenReturn(new WorkerConfig()
    {
      @Override
      public int getCapacity()
      {
        return 3;
      }
    });

    final RuntimeInfo adjustedRuntimeInfo = TaskToolbox.createAdjustedRuntimeInfo(runtimeInfo, appenderatorsManager);

    Assert.assertEquals(
        runtimeInfo.getAvailableProcessors() / numWorkers,
        adjustedRuntimeInfo.getAvailableProcessors()
    );

    Assert.assertEquals(
        runtimeInfo.getMaxHeapSizeBytes() / numWorkers,
        adjustedRuntimeInfo.getMaxHeapSizeBytes()
    );

    Assert.assertEquals(
        runtimeInfo.getDirectMemorySizeBytes() / numWorkers,
        adjustedRuntimeInfo.getDirectMemorySizeBytes()
    );

    Mockito.verify(appenderatorsManager).getWorkerConfig();
    Mockito.verifyNoMoreInteractions(appenderatorsManager);
  }
}
