/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.discovery.DruidLeaderClient;
import io.druid.indexer.TaskLocation;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestTasks;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.Tasks;
import io.druid.indexing.overlord.ThreadPoolTaskRunner;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.DruidNode;
import io.druid.server.coordination.ChangeRequestHistory;
import io.druid.server.coordination.ChangeRequestsSnapshot;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 */
public class WorkerTaskManagerTest
{
  private static final DruidNode DUMMY_NODE = new DruidNode("dummy", "dummy", 9000, null, true, false);

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
        Files.createTempDir().toString(),
        null,
        null,
        0,
        null,
        false,
        null,
        null
    );
    TaskActionClientFactory taskActionClientFactory = EasyMock.createNiceMock(TaskActionClientFactory.class);
    TaskActionClient taskActionClient = EasyMock.createNiceMock(TaskActionClient.class);
    EasyMock.expect(taskActionClientFactory.create(EasyMock.<Task>anyObject())).andReturn(taskActionClient).anyTimes();
    SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    EasyMock.replay(taskActionClientFactory, taskActionClient, notifierFactory);

    return new WorkerTaskManager(
        jsonMapper,
        new ThreadPoolTaskRunner(
            new TaskToolboxFactory(
                taskConfig,
                taskActionClientFactory,
                null, null, null, null, null, null, null, notifierFactory, null, null, null, new SegmentLoaderFactory(
                new SegmentLoaderLocalCacheManager(
                    null,
                    new SegmentLoaderConfig()
                    {
                      @Override
                      public List<StorageLocationConfig> getLocations()
                      {
                        return Lists.newArrayList();
                      }
                    },
                    jsonMapper
                )
            ),
                jsonMapper,
                indexIO,
                null,
                null,
                indexMergerV9,
                null,
                null,
                null,
                null
            ),
            taskConfig,
            new NoopServiceEmitter(),
            DUMMY_NODE,
            new ServerConfig()
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

  @Test(timeout = 10000L)
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
            TaskLocation.create("localhost", 1, 2)
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

    ChangeRequestsSnapshot<WorkerHistoryItem> baseHistory = workerTaskManager.getChangesSince(
        new ChangeRequestHistory.Counter(
            -1,
            0
        )).get();

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
    return new NoopTask(id, null, 100, 0, null, null, ImmutableMap.of(Tasks.PRIORITY_KEY, 0));
  }
}
