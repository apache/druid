/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.metrics.MonitorScheduler;
import io.druid.client.FilteredServerView;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.Task;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.DataSegmentMover;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.OmniSegmentLoader;
import io.druid.segment.loading.DataSegmentArchiver;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.server.coordination.DataSegmentAnnouncer;
import io.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
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
  private FilteredServerView mockNewSegmentServerView = EasyMock.createMock(FilteredServerView.class);
  private QueryRunnerFactoryConglomerate mockQueryRunnerFactoryConglomerate
      = EasyMock.createMock(QueryRunnerFactoryConglomerate.class);
  private MonitorScheduler mockMonitorScheduler = EasyMock.createMock(MonitorScheduler.class);
  private ExecutorService mockQueryExecutorService = EasyMock.createMock(ExecutorService.class);
  private ObjectMapper ObjectMapper = new ObjectMapper();
  private OmniSegmentLoader mockOmniSegmentLoader = EasyMock.createMock(OmniSegmentLoader.class);
  private Task task = EasyMock.createMock(Task.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Before
  public void setUp() throws IOException
  {
    EasyMock.expect(task.getId()).andReturn("task_id").anyTimes();
    EasyMock.replay(task);

    taskToolbox = new TaskToolboxFactory(
        new TaskConfig(temporaryFolder.newFile().toString(), null, null, 50000, null),
        mockTaskActionClientFactory,
        mockEmitter,
        mockSegmentPusher,
        mockDataSegmentKiller,
        mockDataSegmentMover,
        mockDataSegmentArchiver,
        mockSegmentAnnouncer,
        mockNewSegmentServerView,
        mockQueryRunnerFactoryConglomerate,
        mockQueryExecutorService,
        mockMonitorScheduler,
        new SegmentLoaderFactory(mockOmniSegmentLoader),
        ObjectMapper
    );
  }

  @Test
  public void testGetDataSegmentArchiver()
  {
    Assert.assertEquals(mockDataSegmentArchiver,taskToolbox.build(task).getDataSegmentArchiver());
  }

  @Test
  public void testGetSegmentAnnouncer()
  {
    Assert.assertEquals(mockSegmentAnnouncer,taskToolbox.build(task).getSegmentAnnouncer());
  }

  @Test
  public void testGetNewSegmentServerView()
  {
    Assert.assertEquals(mockNewSegmentServerView,taskToolbox.build(task).getNewSegmentServerView());
  }

  @Test
  public void testGetQueryRunnerFactoryConglomerate()
  {
    Assert.assertEquals(mockQueryRunnerFactoryConglomerate,taskToolbox.build(task).getQueryRunnerFactoryConglomerate());
  }

  @Test
  public void testGetQueryExecutorService()
  {
    Assert.assertEquals(mockQueryExecutorService,taskToolbox.build(task).getQueryExecutorService());
  }

  @Test
  public void testGetMonitorScheduler()
  {
    Assert.assertEquals(mockMonitorScheduler,taskToolbox.build(task).getMonitorScheduler());
  }

  @Test
  public void testGetObjectMapper()
  {
    Assert.assertEquals(ObjectMapper,taskToolbox.build(task).getObjectMapper());
  }

  @Test
  public void testFetchSegments() throws SegmentLoadingException, IOException
  {
    File expectedFile = temporaryFolder.newFile();
    EasyMock.expect(mockOmniSegmentLoader.getSegmentFiles((DataSegment)EasyMock.anyObject()))
        .andReturn(expectedFile).anyTimes();
    EasyMock.expect(mockOmniSegmentLoader.withConfig((SegmentLoaderConfig)EasyMock.anyObject()))
        .andReturn(mockOmniSegmentLoader).anyTimes();
    EasyMock.replay(mockOmniSegmentLoader);
    DataSegment dataSegment = DataSegment.builder().dataSource("source").interval(new Interval("2012-01-01/P1D")).version("1").size(1).build();
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
    Assert.assertEquals(mockEmitter,taskToolbox.build(task).getEmitter());
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
}
