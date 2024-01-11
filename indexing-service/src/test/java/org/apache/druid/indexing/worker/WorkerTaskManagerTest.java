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
import com.google.common.util.concurrent.Futures;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.TestTasks;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.overlord.TestTaskRunner;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9Factory;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.security.AuthTestUtils;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

/**
 *
 */
@RunWith(Parameterized.class)
public class WorkerTaskManagerTest
{
  private final TaskLocation location = TaskLocation.create("localhost", 1, 2);
  private final TestUtils testUtils;
  private final ObjectMapper jsonMapper;
  private final IndexMergerV9Factory indexMergerV9Factory;
  private final IndexIO indexIO;

  private final boolean restoreTasksOnRestart;

  private WorkerTaskManager workerTaskManager;
  private OverlordClient overlordClient;

  public WorkerTaskManagerTest(boolean restoreTasksOnRestart)
  {
    testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    TestTasks.registerSubtypes(jsonMapper);
    indexMergerV9Factory = testUtils.getIndexMergerV9Factory();
    indexIO = testUtils.getTestIndexIO();
    this.restoreTasksOnRestart = restoreTasksOnRestart;
  }

  @Parameterized.Parameters(name = "restoreTasksOnRestart = {0}")
  public static Collection<Object[]> getParameters()
  {
    Object[][] parameters = new Object[][]{{false}, {true}};

    return Arrays.asList(parameters);
  }

  private WorkerTaskManager createWorkerTaskManager()
  {
    TaskConfig taskConfig = new TaskConfigBuilder()
        .setBaseDir(FileUtils.createTempDir().toString())
        .setDefaultRowFlushBoundary(0)
        .setRestoreTasksOnRestart(restoreTasksOnRestart)
        .setBatchProcessingMode(TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name())
        .build();

    TaskActionClientFactory taskActionClientFactory = EasyMock.createNiceMock(TaskActionClientFactory.class);
    TaskActionClient taskActionClient = EasyMock.createNiceMock(TaskActionClient.class);
    EasyMock.expect(taskActionClientFactory.create(EasyMock.anyObject())).andReturn(taskActionClient).anyTimes();
    SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    EasyMock.replay(taskActionClientFactory, taskActionClient, notifierFactory);
    overlordClient = EasyMock.createMock(OverlordClient.class);

    return new WorkerTaskManager(
        jsonMapper,
        new TestTaskRunner(
            new TaskToolboxFactory(
                null,
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
                new SegmentCacheManagerFactory(jsonMapper),
                jsonMapper,
                indexIO,
                null,
                null,
                null,
                indexMergerV9Factory,
                null,
                null,
                null,
                null,
                new NoopTestTaskReportFileWriter(),
                null,
                AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                new NoopChatHandlerProvider(),
                testUtils.getRowIngestionMetersFactory(),
                new TestAppenderatorsManager(),
                overlordClient,
                new NoopCoordinatorClient(),
                null,
                null,
                null,
                "1",
                CentralizedDatasourceSchemaConfig.create()
            ),
            taskConfig,
            location
        ),
        taskConfig,
        overlordClient
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
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);
    Task task1 = createNoopTask("task1-assigned-via-assign-dir");
    Task task2 = createNoopTask("task2-completed-already");
    Task task3 = createNoopTask("task3-assigned-explicitly");

    FileUtils.mkdirp(workerTaskManager.getAssignedTaskDir());
    FileUtils.mkdirp(workerTaskManager.getCompletedTaskDir());

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

  @Test(timeout = 30_000L)
  public void testTaskStatusWhenTaskRunnerFutureThrowsException() throws Exception
  {
    Task task = new NoopTask("id", null, null, 100, 0, ImmutableMap.of(Tasks.PRIORITY_KEY, 0))
    {
      @Override
      public TaskStatus runTask(TaskToolbox toolbox)
      {
        throw new Error("task failure test");
      }
    };
    workerTaskManager.start();
    workerTaskManager.assignTask(task);

    Map<String, TaskAnnouncement> completeTasks;
    do {
      completeTasks = workerTaskManager.getCompletedTasks();
      Thread.sleep(10);
    } while (completeTasks.isEmpty());

    Assert.assertEquals(1, completeTasks.size());
    TaskAnnouncement announcement = completeTasks.get(task.getId());
    Assert.assertNotNull(announcement);
    Assert.assertEquals(TaskState.FAILED, announcement.getStatus());
    Assert.assertEquals(
        "Failed to run task with an exception. See middleManager or indexer logs for more details.",
        announcement.getTaskStatus().getErrorMsg()
    );
  }

  @Test(timeout = 30_000L)
  public void test_completedTasksCleanup_running() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(task.getId(), TaskStatus.running(task.getId()))))
            .once();
    EasyMock.replay(overlordClient);

    workerTaskManager.doCompletedTasksCleanup();
    Assert.assertEquals(1, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test(timeout = 30_000L)
  public void test_completedTasksCleanup_noStatus() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(Futures.immediateFuture(Collections.emptyMap()))
            .once();
    EasyMock.replay(overlordClient);

    // Missing status (empty map) means we clean up the task. The idea is that this means the Overlord has *never*
    // heard of it, so we should forget about it.
    workerTaskManager.doCompletedTasksCleanup();
    Assert.assertEquals(0, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test(timeout = 30_000L)
  public void test_completedTasksCleanup_success() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(task.getId(), TaskStatus.success(task.getId()))))
            .once();
    EasyMock.replay(overlordClient);

    workerTaskManager.doCompletedTasksCleanup();
    Assert.assertEquals(0, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test(timeout = 30_000L)
  public void test_completedTasksCleanup_404error() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(
                Futures.immediateFailedFuture(
                    new HttpResponseException(
                        new StringFullResponseHolder(
                            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND),
                            StandardCharsets.UTF_8
                        )
                    )
                )
            )
            .once();
    EasyMock.replay(overlordClient);

    // Ending size zero, because 404 means we assume the Overlord does not have the taskStatuses API. In this case
    // we remove all completed task statuses periodically regardless of Overlord confirmation.
    workerTaskManager.doCompletedTasksCleanup();
    Assert.assertEquals(0, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test(timeout = 30_000L)
  public void test_completedTasksCleanup_500error() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(
                Futures.immediateFailedFuture(
                    new HttpResponseException(
                        new StringFullResponseHolder(
                            new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.INTERNAL_SERVER_ERROR),
                            StandardCharsets.UTF_8
                        )
                    )
                )
            )
            .once();
    EasyMock.replay(overlordClient);

    // HTTP 500 is ignored and no cleanup happens.
    workerTaskManager.doCompletedTasksCleanup();
    Assert.assertEquals(1, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test(timeout = 30_000L)
  public void test_completedTasksCleanup_ioException() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(Futures.immediateFailedFuture(new IOException()))
            .once();
    EasyMock.replay(overlordClient);

    // IOException is ignored and no cleanup happens.
    workerTaskManager.doCompletedTasksCleanup();
    Assert.assertEquals(1, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  private NoopTask createNoopTask(String id)
  {
    return new NoopTask(id, null, null, 100, 0, ImmutableMap.of(Tasks.PRIORITY_KEY, 0));
  }

  /**
   * Start the {@link #workerTaskManager}, submit a {@link NoopTask}, wait for it to be complete. Common preamble
   * for various tests of {@link WorkerTaskManager#doCompletedTasksCleanup()}.
   */
  private Task setUpCompletedTasksCleanupTest() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    final Task task = new NoopTask("id", null, null, 100, 0, ImmutableMap.of(Tasks.PRIORITY_KEY, 0));

    // Scheduled scheduleCompletedTasksCleanup will not run, because initialDelay is 1 minute, which is longer than
    // the 30-second timeout of this test case.
    workerTaskManager.start();
    workerTaskManager.assignTask(task);

    Map<String, TaskAnnouncement> completeTasks;
    do {
      completeTasks = workerTaskManager.getCompletedTasks();
      Thread.sleep(10);
    } while (completeTasks.isEmpty());

    Assert.assertEquals(1, completeTasks.size());
    TaskAnnouncement announcement = completeTasks.get(task.getId());
    Assert.assertNotNull(announcement);
    Assert.assertEquals(TaskState.SUCCESS, announcement.getStatus());

    EasyMock.reset(overlordClient);
    return task;
  }
}
