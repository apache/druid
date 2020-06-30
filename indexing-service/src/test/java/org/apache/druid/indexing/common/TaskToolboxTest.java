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
import com.google.common.collect.ImmutableList;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.metrics.MonitorScheduler;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.DataSegmentArchiver;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.DataSegmentMover;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.segment.loading.SegmentLoaderLocalCacheManager;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordination.DataSegmentAnnouncer;
import org.apache.druid.server.coordination.DataSegmentServerAnnouncer;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

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
  private ExecutorService mockQueryExecutorService = EasyMock.createMock(ExecutorService.class);
  private ObjectMapper ObjectMapper = new ObjectMapper();
  private SegmentLoaderFactory mockSegmentLoaderFactory = EasyMock.createMock(SegmentLoaderFactory.class);
  private SegmentLoaderLocalCacheManager mockSegmentLoaderLocalCacheManager = EasyMock.createMock(SegmentLoaderLocalCacheManager.class);
  private Task task = EasyMock.createMock(Task.class);
  private IndexMergerV9 mockIndexMergerV9 = EasyMock.createMock(IndexMergerV9.class);
  private IndexIO mockIndexIO = EasyMock.createMock(IndexIO.class);
  private Cache mockCache = EasyMock.createMock(Cache.class);
  private CacheConfig mockCacheConfig = EasyMock.createMock(CacheConfig.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    EasyMock.expect(task.getId()).andReturn("task_id").anyTimes();
    EasyMock.expect(task.getDataSource()).andReturn("task_ds").anyTimes();
    EasyMock.replay(task, mockHandoffNotifierFactory);

    taskToolbox = new TaskToolboxFactory(
        new TaskConfig(temporaryFolder.newFile().toString(), null, null, 50000, null, false, null, null, null),
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
        mockQueryExecutorService,
        NoopJoinableFactory.INSTANCE,
        () -> mockMonitorScheduler,
        mockSegmentLoaderFactory,
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
        null
    );
  }

  @Test
  public void testGetDataSegmentArchiver()
  {
    Assert.assertEquals(mockDataSegmentArchiver, taskToolbox.build(task).getDataSegmentArchiver());
  }

  @Test
  public void testGetSegmentAnnouncer()
  {
    Assert.assertEquals(mockSegmentAnnouncer, taskToolbox.build(task).getSegmentAnnouncer());
  }

  @Test
  public void testGetQueryRunnerFactoryConglomerate()
  {
    Assert.assertEquals(mockQueryRunnerFactoryConglomerate, taskToolbox.build(task).getQueryRunnerFactoryConglomerate());
  }

  @Test
  public void testGetQueryExecutorService()
  {
    Assert.assertEquals(mockQueryExecutorService, taskToolbox.build(task).getQueryExecutorService());
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
  public void testFetchSegments() throws SegmentLoadingException, IOException
  {
    File expectedFile = temporaryFolder.newFile();
    EasyMock
        .expect(mockSegmentLoaderFactory.manufacturate(EasyMock.anyObject()))
        .andReturn(mockSegmentLoaderLocalCacheManager).anyTimes();
    EasyMock
        .expect(mockSegmentLoaderLocalCacheManager.getSegmentFiles(EasyMock.anyObject()))
        .andReturn(expectedFile).anyTimes();
    EasyMock.replay(mockSegmentLoaderFactory, mockSegmentLoaderLocalCacheManager);
    DataSegment dataSegment = DataSegment.builder().dataSource("source").interval(Intervals.of("2012-01-01/P1D")).version("1").size(1).build();
    List<DataSegment> segments = ImmutableList.of
        (
            dataSegment
        );
    Map actualFetchedSegment = taskToolbox.build(task).fetchSegments(segments);
    Assert.assertEquals(expectedFile, actualFetchedSegment.get(dataSegment));
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
}
