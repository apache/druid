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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.discovery.DruidLeaderClient;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentLoaderFactory;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.TestTasks;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.overlord.TestTaskRunner;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.loading.SegmentLoaderConfig;
import org.apache.druid.segment.loading.StorageLocationConfig;
import org.apache.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 */
public class WorkerTaskManagerTest
{
  private final TaskLocation location = TaskLocation.create("localhost", 1, 2);
  private final ObjectMapper jsonMapper;
  private final IndexMergerV9 indexMergerV9;
  private final IndexIO indexIO;

  private WorkerTaskManager workerTaskManager;

  public WorkerTaskManagerTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    TestTasks.registerSubtypes(jsonMapper);
    indexMergerV9 = testUtils.getTestIndexMergerV9();
    indexIO = testUtils.getTestIndexIO();
  }

  private WorkerTaskManager createWorkerTaskManager()
  {
    TaskConfig taskConfig = new TaskConfig(
        FileUtils.createTempDir().toString(),
        null,
        null,
        0,
        null,
        false,
        null,
        null,
        null
    );
    TaskActionClientFactory taskActionClientFactory = EasyMock.createNiceMock(TaskActionClientFactory.class);
    TaskActionClient taskActionClient = EasyMock.createNiceMock(TaskActionClient.class);
    EasyMock.expect(taskActionClientFactory.create(EasyMock.anyObject())).andReturn(taskActionClient).anyTimes();
    SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    EasyMock.replay(taskActionClientFactory, taskActionClient, notifierFactory);

    final SegmentLoaderConfig loaderConfig = new SegmentLoaderConfig()
    {
      @Override
      public List<StorageLocationConfig> getLocations()
      {
        return Collections.emptyList();
      }
    };

    return new WorkerTaskManager(
        jsonMapper,
        new TestTaskRunner(
            new TaskToolboxFactory(
                taskConfig,
                null,
                taskActionClientFactory,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                notifierFactory,
                null,
                null,
                NoopJoinableFactory.INSTANCE,
                null,
                new SegmentLoaderFactory(null, jsonMapper),
                jsonMapper,
                indexIO,
                null,
                null,
                null,
                indexMergerV9,
                null,
                null,
                null,
                null,
                new NoopTestTaskReportFileWriter(),
                null
            ),
            taskConfig,
            location
        ),
        taskConfig,
        EasyMock.createNiceMock(DruidLeaderClient.class)
    )
    {
      @Override
      protected void taskStarted(String taskId)
      {
      }

      @Override
      protected void taskAnnouncementChanged(TaskAnnouncement announcement)
      {
      }
    };
  }

  @Before
  public void setUp()
  {
    workerTaskManager = createWorkerTaskManager();
  }

  @After
  public void tearDown() throws Exception
  {
    workerTaskManager.stop();
  }

  @Test(timeout = 60_000L)
  public void testTaskRun() throws Exception
  {
    Task task1 = createNoopTask("task1-assigned-via-assign-dir");
    Task task2 = createNoopTask("task2-completed-already");
    Task task3 = createNoopTask("task3-assigned-explicitly");

    workerTaskManager.getAssignedTaskDir().mkdirs();
    workerTaskManager.getCompletedTaskDir().mkdirs();

    // create a task in assigned task directory, to simulate MM shutdown right after a task was assigned.
    jsonMapper.writeValue(new File(workerTaskManager.getAssignedTaskDir(), task1.getId()), task1);

    // simulate an already completed task
    jsonMapper.writeValue(
        new File(workerTaskManager.getCompletedTaskDir(), task2.getId()),
        TaskAnnouncement.create(
            task2,
            TaskStatus.success(task2.getId()),
            location
        )
    );

    workerTaskManager.start();

    Assert.assertTrue(workerTaskManager.getCompletedTasks().get(task2.getId()).getTaskStatus().isSuccess());

    while (!workerTaskManager.getCompletedTasks().containsKey(task1.getId())) {
      Thread.sleep(100);
    }
    Assert.assertTrue(workerTaskManager.getCompletedTasks().get(task1.getId()).getTaskStatus().isSuccess());
    Assert.assertTrue(new File(workerTaskManager.getCompletedTaskDir(), task1.getId()).exists());
    Assert.assertFalse(new File(workerTaskManager.getAssignedTaskDir(), task1.getId()).exists());

    ChangeRequestsSnapshot<WorkerHistoryItem> baseHistory = workerTaskManager
        .getChangesSince(new ChangeRequestHistory.Counter(-1, 0))
        .get();

    Assert.assertFalse(baseHistory.isResetCounter());
    Assert.assertEquals(3, baseHistory.getRequests().size());
    Assert.assertFalse(((WorkerHistoryItem.Metadata) baseHistory.getRequests().get(0)).isDisabled());

    WorkerHistoryItem.TaskUpdate baseUpdate1 = (WorkerHistoryItem.TaskUpdate) baseHistory.getRequests().get(1);
    WorkerHistoryItem.TaskUpdate baseUpdate2 = (WorkerHistoryItem.TaskUpdate) baseHistory.getRequests().get(2);

    Assert.assertTrue(baseUpdate1.getTaskAnnouncement().getTaskStatus().isSuccess());
    Assert.assertTrue(baseUpdate2.getTaskAnnouncement().getTaskStatus().isSuccess());

    Assert.assertEquals(
        ImmutableSet.of(task1.getId(), task2.getId()),
        ImmutableSet.of(
            baseUpdate1.getTaskAnnouncement().getTaskStatus().getId(),
            baseUpdate2.getTaskAnnouncement().getTaskStatus().getId()
        )
    );

    // assign another task
    workerTaskManager.assignTask(task3);

    while (!workerTaskManager.getCompletedTasks().containsKey(task3.getId())) {
      Thread.sleep(100);
    }

    Assert.assertTrue(workerTaskManager.getCompletedTasks().get(task3.getId()).getTaskStatus().isSuccess());
    Assert.assertTrue(new File(workerTaskManager.getCompletedTaskDir(), task3.getId()).exists());
    Assert.assertFalse(new File(workerTaskManager.getAssignedTaskDir(), task3.getId()).exists());

    ChangeRequestsSnapshot<WorkerHistoryItem> changes = workerTaskManager.getChangesSince(baseHistory.getCounter())
                                                                         .get();
    Assert.assertFalse(changes.isResetCounter());
    Assert.assertEquals(4, changes.getRequests().size());

    WorkerHistoryItem.TaskUpdate update1 = (WorkerHistoryItem.TaskUpdate) changes.getRequests().get(0);
    Assert.assertEquals(task3.getId(), update1.getTaskAnnouncement().getTaskStatus().getId());
    Assert.assertTrue(update1.getTaskAnnouncement().getTaskStatus().isRunnable());
    Assert.assertNull(update1.getTaskAnnouncement().getTaskLocation().getHost());

    WorkerHistoryItem.TaskUpdate update2 = (WorkerHistoryItem.TaskUpdate) changes.getRequests().get(1);
    Assert.assertEquals(task3.getId(), update2.getTaskAnnouncement().getTaskStatus().getId());
    Assert.assertTrue(update2.getTaskAnnouncement().getTaskStatus().isRunnable());
    Assert.assertNull(update2.getTaskAnnouncement().getTaskLocation().getHost());

    WorkerHistoryItem.TaskUpdate update3 = (WorkerHistoryItem.TaskUpdate) changes.getRequests().get(2);
    Assert.assertEquals(task3.getId(), update3.getTaskAnnouncement().getTaskStatus().getId());
    Assert.assertTrue(update3.getTaskAnnouncement().getTaskStatus().isRunnable());
    Assert.assertNotNull(update3.getTaskAnnouncement().getTaskLocation().getHost());

    WorkerHistoryItem.TaskUpdate update4 = (WorkerHistoryItem.TaskUpdate) changes.getRequests().get(3);
    Assert.assertEquals(task3.getId(), update4.getTaskAnnouncement().getTaskStatus().getId());
    Assert.assertTrue(update4.getTaskAnnouncement().getTaskStatus().isSuccess());
    Assert.assertNotNull(update4.getTaskAnnouncement().getTaskLocation().getHost());
  }

  private NoopTask createNoopTask(String id)
  {
    return new NoopTask(id, null, null, 100, 0, null, null, ImmutableMap.of(Tasks.PRIORITY_KEY, 0));
  }
}
