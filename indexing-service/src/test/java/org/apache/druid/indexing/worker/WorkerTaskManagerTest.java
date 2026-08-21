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
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.apache.druid.query.policy.NoopPolicyEnforcer;
import org.apache.druid.rpc.HttpResponseException;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9Factory;
import org.apache.druid.segment.TestIndex;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.ChatHandlerProvider;
import org.apache.druid.server.coordination.ChangeRequestHistory;
import org.apache.druid.server.coordination.ChangeRequestsSnapshot;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.apache.druid.utils.JvmUtils;
import org.easymock.EasyMock;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.Parameter;
import org.junit.jupiter.params.ParameterizedClass;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
@ParameterizedClass
@MethodSource("getParameters")
public class WorkerTaskManagerTest
{
  private final TaskLocation location = TaskLocation.create("localhost", 1, 2);
  private final TestUtils testUtils;
  private final ObjectMapper jsonMapper;
  private final IndexMergerV9Factory indexMergerV9Factory;
  private final IndexIO indexIO;

  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolderExtension = TemporaryFolderExtension.perTest();
  private final File tempDir = temporaryFolderExtension.getRoot();

  @Parameter(0)
  public boolean restoreTasksOnRestart;

  private WorkerTaskManager workerTaskManager;
  private OverlordClient overlordClient;

  public WorkerTaskManagerTest()
  {
    testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    TestTasks.registerSubtypes(jsonMapper);
    indexMergerV9Factory = testUtils.getIndexMergerV9Factory();
    indexIO = testUtils.getTestIndexIO();
  }

  public static Collection<Object[]> getParameters()
  {
    Object[][] parameters = new Object[][]{{false}, {true}};

    return Arrays.asList(parameters);
  }

  private WorkerTaskManager createWorkerTaskManager()
  {
    return createWorkerTaskManager(tempDir, new WorkerConfig());
  }

  private WorkerTaskManager createWorkerTaskManager(File baseDir)
  {
    return createWorkerTaskManager(baseDir, new WorkerConfig());
  }

  private WorkerTaskManager createWorkerTaskManager(File baseDir, WorkerConfig workerConfig)
  {
    TaskConfig taskConfig = new TaskConfigBuilder()
        .setBaseDir(baseDir.toString())
        .setRestoreTasksOnRestart(restoreTasksOnRestart)
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
                NoopPolicyEnforcer.instance(),
                null,
                null,
                null,
                null,
                null,
                notifierFactory,
                null,
                null,
                null,
                NoopJoinableFactory.INSTANCE,
                null,
                SegmentCacheManagerFactory.createWithOwnedPool(TestIndex.INDEX_IO, jsonMapper),
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
                null,
                new NoopTestTaskReportFileWriter(),
                null,
                AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                new ChatHandlerProvider(),
                testUtils.getRowIngestionMetersFactory(),
                new TestAppenderatorsManager(),
                overlordClient,
                new NoopCoordinatorClient(),
                null,
                null,
                null,
                "1",
                CentralizedDatasourceSchemaConfig.create(),
                JvmUtils.getRuntimeInfo()
            ),
            taskConfig,
            location
        ),
        taskConfig,
        workerConfig,
        overlordClient
    );
  }

  @BeforeEach
  public void setUp()
  {
    workerTaskManager = createWorkerTaskManager();
  }

  @AfterEach
  public void tearDown() throws Exception
  {
    workerTaskManager.stop();
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS)
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

    Assertions.assertTrue(workerTaskManager.getCompletedTasks().get(task2.getId()).getTaskStatus().isSuccess());

    while (!workerTaskManager.getCompletedTasks().containsKey(task1.getId())) {
      Thread.sleep(100);
    }
    Assertions.assertTrue(workerTaskManager.getCompletedTasks().get(task1.getId()).getTaskStatus().isSuccess());
    Assertions.assertTrue(new File(workerTaskManager.getCompletedTaskDir(), task1.getId()).exists());
    Assertions.assertFalse(new File(workerTaskManager.getAssignedTaskDir(), task1.getId()).exists());

    ChangeRequestsSnapshot<WorkerHistoryItem> baseHistory = workerTaskManager
        .getChangesSince(new ChangeRequestHistory.Counter(-1, 0))
        .get();

    Assertions.assertFalse(baseHistory.isResetCounter());
    Assertions.assertEquals(3, baseHistory.getRequests().size());
    Assertions.assertFalse(((WorkerHistoryItem.Metadata) baseHistory.getRequests().get(0)).isDisabled());

    WorkerHistoryItem.TaskUpdate baseUpdate1 = (WorkerHistoryItem.TaskUpdate) baseHistory.getRequests().get(1);
    WorkerHistoryItem.TaskUpdate baseUpdate2 = (WorkerHistoryItem.TaskUpdate) baseHistory.getRequests().get(2);

    Assertions.assertTrue(baseUpdate1.getTaskAnnouncement().getTaskStatus().isSuccess());
    Assertions.assertTrue(baseUpdate2.getTaskAnnouncement().getTaskStatus().isSuccess());

    Assertions.assertEquals(
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

    Assertions.assertTrue(workerTaskManager.getCompletedTasks().get(task3.getId()).getTaskStatus().isSuccess());
    Assertions.assertTrue(new File(workerTaskManager.getCompletedTaskDir(), task3.getId()).exists());
    Assertions.assertFalse(new File(workerTaskManager.getAssignedTaskDir(), task3.getId()).exists());

    ChangeRequestsSnapshot<WorkerHistoryItem> changes = workerTaskManager.getChangesSince(baseHistory.getCounter())
                                                                         .get();
    Assertions.assertFalse(changes.isResetCounter());
    Assertions.assertEquals(4, changes.getRequests().size());

    WorkerHistoryItem.TaskUpdate update1 = (WorkerHistoryItem.TaskUpdate) changes.getRequests().get(0);
    Assertions.assertEquals(task3.getId(), update1.getTaskAnnouncement().getTaskStatus().getId());
    Assertions.assertTrue(update1.getTaskAnnouncement().getTaskStatus().isRunnable());
    Assertions.assertNull(update1.getTaskAnnouncement().getTaskLocation().getHost());

    WorkerHistoryItem.TaskUpdate update2 = (WorkerHistoryItem.TaskUpdate) changes.getRequests().get(1);
    Assertions.assertEquals(task3.getId(), update2.getTaskAnnouncement().getTaskStatus().getId());
    Assertions.assertTrue(update2.getTaskAnnouncement().getTaskStatus().isRunnable());
    Assertions.assertNull(update2.getTaskAnnouncement().getTaskLocation().getHost());

    WorkerHistoryItem.TaskUpdate update3 = (WorkerHistoryItem.TaskUpdate) changes.getRequests().get(2);
    Assertions.assertEquals(task3.getId(), update3.getTaskAnnouncement().getTaskStatus().getId());
    Assertions.assertTrue(update3.getTaskAnnouncement().getTaskStatus().isRunnable());
    Assertions.assertNotNull(update3.getTaskAnnouncement().getTaskLocation().getHost());

    WorkerHistoryItem.TaskUpdate update4 = (WorkerHistoryItem.TaskUpdate) changes.getRequests().get(3);
    Assertions.assertEquals(task3.getId(), update4.getTaskAnnouncement().getTaskStatus().getId());
    Assertions.assertTrue(update4.getTaskAnnouncement().getTaskStatus().isSuccess());
    Assertions.assertNotNull(update4.getTaskAnnouncement().getTaskLocation().getHost());
  }

  @Test
  @Timeout(value = 30_000L, unit = TimeUnit.MILLISECONDS)
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

    Assertions.assertEquals(1, completeTasks.size());
    TaskAnnouncement announcement = completeTasks.get(task.getId());
    Assertions.assertNotNull(announcement);
    Assertions.assertEquals(TaskState.FAILED, announcement.getStatus());
    Assertions.assertEquals(
        "Failed to run task with an exception. See middleManager or indexer logs for more details.",
        announcement.getTaskStatus().getErrorMsg()
    );
  }

  @Test
  @Timeout(value = 30_000L, unit = TimeUnit.MILLISECONDS)
  public void test_completedTasksCleanup_running() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(task.getId(), TaskStatus.running(task.getId()))))
            .once();
    EasyMock.replay(overlordClient);

    workerTaskManager.doCompletedTasksCleanup();
    Assertions.assertEquals(1, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test
  @Timeout(value = 30_000L, unit = TimeUnit.MILLISECONDS)
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
    Assertions.assertEquals(0, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test
  @Timeout(value = 30_000L, unit = TimeUnit.MILLISECONDS)
  public void test_completedTasksCleanup_success() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(Futures.immediateFuture(ImmutableMap.of(task.getId(), TaskStatus.success(task.getId()))))
            .once();
    EasyMock.replay(overlordClient);

    workerTaskManager.doCompletedTasksCleanup();
    Assertions.assertEquals(0, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test
  @Timeout(value = 30_000L, unit = TimeUnit.MILLISECONDS)
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
    Assertions.assertEquals(0, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test
  @Timeout(value = 30_000L, unit = TimeUnit.MILLISECONDS)
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
    Assertions.assertEquals(1, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  @Test
  @Timeout(value = 30_000L, unit = TimeUnit.MILLISECONDS)
  public void test_completedTasksCleanup_ioException() throws Exception
  {
    final Task task = setUpCompletedTasksCleanupTest();

    EasyMock.expect(overlordClient.taskStatuses(Collections.singleton(task.getId())))
            .andReturn(Futures.immediateFailedFuture(new IOException()))
            .once();
    EasyMock.replay(overlordClient);

    // IOException is ignored and no cleanup happens.
    workerTaskManager.doCompletedTasksCleanup();
    Assertions.assertEquals(1, workerTaskManager.getCompletedTasks().size());

    EasyMock.verify(overlordClient);
  }

  private NoopTask createNoopTask(String id)
  {
    return new NoopTask(id, null, null, 100, 0, ImmutableMap.of(Tasks.PRIORITY_KEY, 0));
  }

  private NoopTask createNoopTask(String id, String dataSource)
  {
    return new NoopTask(id, null, dataSource, 100, 0, ImmutableMap.of(Tasks.PRIORITY_KEY, 0));
  }

  private NoopTask createNoopFailingTask(String id, String dataSource)
  {
    return new NoopTask(id, null, dataSource, 100, 0, ImmutableMap.of(Tasks.PRIORITY_KEY, 0))
    {
      @Override
      public TaskStatus runTask(TaskToolbox toolbox) throws Exception
      {
        Thread.sleep(getRunTime());
        return TaskStatus.failure(getId(), "Failed to complete the task");
      }
    };
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

    Assertions.assertEquals(1, completeTasks.size());
    TaskAnnouncement announcement = completeTasks.get(task.getId());
    Assertions.assertNotNull(announcement);
    Assertions.assertEquals(TaskState.SUCCESS, announcement.getStatus());

    EasyMock.reset(overlordClient);
    return task;
  }

  @Test
  public void getWorkerTaskStatsTest() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    Task task1 = createNoopTask("task1", "wikipedia");
    Task task2 = createNoopTask("task2", "wikipedia");
    Task task3 = createNoopFailingTask("task3", "animals");

    workerTaskManager.start();
    // befor assigning tasks we should get no running tasks
    Assertions.assertEquals(workerTaskManager.getWorkerRunningTasks().size(), 0L);

    workerTaskManager.assignTask(task1);
    workerTaskManager.assignTask(task2);
    workerTaskManager.assignTask(task3);

    Thread.sleep(25);
    //should return all 3 tasks as running
    Assertions.assertEquals(workerTaskManager.getWorkerRunningTasks(), ImmutableMap.of(
        "wikipedia", 2L,
        "animals", 1L
    ));

    Map<String, Long> runningTasks;
    do {
      runningTasks = workerTaskManager.getWorkerRunningTasks();
      Thread.sleep(10);
    } while (!runningTasks.isEmpty());

    // When running tasks are empty all task should be reported as completed and
    // one of the task for animals datasource should fail and other 2 tasks in
    // the wikipedia datasource should succeed
    Assertions.assertEquals(workerTaskManager.getWorkerCompletedTasks(), ImmutableMap.of(
        "wikipedia", 2L,
        "animals", 1L
    ));
    Assertions.assertEquals(workerTaskManager.getWorkerFailedTasks(), ImmutableMap.of(
            "animals", 1L
    ));
    Assertions.assertEquals(workerTaskManager.getWorkerSuccessfulTasks(), ImmutableMap.of(
            "wikipedia", 2L
    ));
    Assertions.assertEquals(workerTaskManager.getWorkerAssignedTasks().size(), 0L);
  }

  @Test
  public void test_disabledState_persistsAcrossRestart() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    final File baseTaskDir = tempDir;

    workerTaskManager = createWorkerTaskManager(baseTaskDir);
    workerTaskManager.start();
    Assertions.assertTrue(workerTaskManager.isWorkerEnabled());
    workerTaskManager.workerDisabled();
    Assertions.assertFalse(workerTaskManager.isWorkerEnabled());
    Assertions.assertTrue(workerTaskManager.getStateFile().exists());
    workerTaskManager.stop();

    workerTaskManager = createWorkerTaskManager(baseTaskDir);
    workerTaskManager.start();
    Assertions.assertFalse(workerTaskManager.isWorkerEnabled());

    final ChangeRequestsSnapshot<WorkerHistoryItem> history =
        workerTaskManager.getChangesSince(new ChangeRequestHistory.Counter(-1, 0)).get();
    Assertions.assertTrue(((WorkerHistoryItem.Metadata) history.getRequests().get(0)).isDisabled());
  }

  @Test
  public void test_disabledState_reEnablePersistsAcrossRestart() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    final File baseTaskDir = tempDir;

    workerTaskManager = createWorkerTaskManager(baseTaskDir);
    workerTaskManager.start();
    workerTaskManager.workerDisabled();
    workerTaskManager.workerEnabled();
    Assertions.assertTrue(workerTaskManager.isWorkerEnabled());
    workerTaskManager.stop();

    workerTaskManager = createWorkerTaskManager(baseTaskDir);
    workerTaskManager.start();
    Assertions.assertTrue(workerTaskManager.isWorkerEnabled());
  }

  @Test
  public void test_disabledState_defaultsToEnabledWhenNoFile() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    workerTaskManager.start();
    Assertions.assertTrue(workerTaskManager.isWorkerEnabled());
  }

  @Test
  public void test_disabledState_malformedFileToleratedAndStartsEnabled() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    workerTaskManager = createWorkerTaskManager();
    final File stateFile = workerTaskManager.getStateFile();
    FileUtils.mkdirp(stateFile.getParentFile());
    Files.write(stateFile.toPath(), "not valid json".getBytes(StandardCharsets.UTF_8));

    workerTaskManager.start();
    Assertions.assertTrue(workerTaskManager.isWorkerEnabled());
  }

  @Test
  public void test_startAlwaysEnabled_ignoresAndDeletesPersistedDisabledState() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    final File baseTaskDir = tempDir;

    workerTaskManager = createWorkerTaskManager(baseTaskDir);
    workerTaskManager.start();
    workerTaskManager.workerDisabled();
    Assertions.assertFalse(workerTaskManager.isWorkerEnabled());
    Assertions.assertTrue(workerTaskManager.getStateFile().exists());
    workerTaskManager.stop();

    final WorkerConfig workerConfig = new WorkerConfig().cloneBuilder()
        .setStartAlwaysEnabled(true)
        .build();
    workerTaskManager = createWorkerTaskManager(baseTaskDir, workerConfig);
    workerTaskManager.start();
    Assertions.assertTrue(workerTaskManager.isWorkerEnabled());
    Assertions.assertFalse(workerTaskManager.getStateFile().exists());
  }

  @Test
  public void test_startAlwaysEnabled_doesNotCreateStateFileWhenAbsent() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    final WorkerConfig workerConfig = new WorkerConfig().cloneBuilder()
        .setStartAlwaysEnabled(true)
        .build();
    workerTaskManager = createWorkerTaskManager(tempDir, workerConfig);
    workerTaskManager.start();
    Assertions.assertTrue(workerTaskManager.isWorkerEnabled());
    Assertions.assertFalse(workerTaskManager.getStateFile().exists());
  }

  @Test
  public void test_startAlwaysEnabled_runtimeDisableStillPersistsToStateFile() throws Exception
  {
    EasyMock.expect(overlordClient.withRetryPolicy(EasyMock.anyObject())).andReturn(overlordClient).anyTimes();
    EasyMock.replay(overlordClient);

    final WorkerConfig workerConfig = new WorkerConfig().cloneBuilder()
        .setStartAlwaysEnabled(true)
        .build();
    workerTaskManager = createWorkerTaskManager(tempDir, workerConfig);
    workerTaskManager.start();
    workerTaskManager.workerDisabled();
    Assertions.assertFalse(workerTaskManager.isWorkerEnabled());
    Assertions.assertTrue(workerTaskManager.getStateFile().exists());
  }
}
