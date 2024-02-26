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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.discovery.WorkerNodeService;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.IngestionTestBase;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseParallelIndexTaskRunner;
import org.apache.druid.indexing.overlord.autoscaling.NoopProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerTest;
import org.apache.druid.indexing.overlord.hrtr.WorkerHolder;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

public class TaskQueueTest extends IngestionTestBase
{
  private static final Granularity SEGMENT_GRANULARITY = Granularities.DAY;

  /**
   * This test verifies releasing all locks of a task when it is not ready to run yet.
   *
   * This test uses 2 APIs, {@link TaskQueue} APIs and {@link IngestionTestBase} APIs
   * to emulate the scenario of deadlock. The IngestionTestBase provides low-leve APIs
   * which you can manipulate {@link TaskLockbox} manually. These APIs should be used
   * only to emulate a certain deadlock scenario. All normal tasks should use TaskQueue
   * APIs.
   */
  @Test
  public void testManageInternalReleaseLockWhenTaskIsNotReady() throws Exception
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        new SimpleTaskRunner(actionClientFactory),
        actionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.setActive(true);
    // task1 emulates a case when there is a task that was issued before task2 and acquired locks conflicting
    // to task2.
    final TestTask task1 = new TestTask("t1", Intervals.of("2021-01/P1M"));
    // Manually get locks for task1. task2 cannot be ready because of task1.
    prepareTaskForLocking(task1);
    Assert.assertTrue(task1.isReady(actionClientFactory.create(task1)));

    final TestTask task2 = new TestTask("t2", Intervals.of("2021-01-31/P1M"));
    taskQueue.add(task2);
    taskQueue.manageInternal();
    Assert.assertFalse(task2.isDone());
    Assert.assertTrue(getLockbox().findLocksForTask(task2).isEmpty());

    // task3 can run because task2 is still blocked by task1.
    final TestTask task3 = new TestTask("t3", Intervals.of("2021-02-01/P1M"));
    taskQueue.add(task3);
    taskQueue.manageInternal();
    Assert.assertFalse(task2.isDone());
    Assert.assertTrue(task3.isDone());
    Assert.assertTrue(getLockbox().findLocksForTask(task2).isEmpty());

    // Shut down task1 and task3 and release their locks.
    shutdownTask(task1);
    taskQueue.shutdown(task3.getId(), "Emulating shutdown of task3");

    // Now task2 should run.
    taskQueue.manageInternal();
    Assert.assertTrue(task2.isDone());
  }

  @Test
  public void testShutdownReleasesTaskLock() throws Exception
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        new SimpleTaskRunner(actionClientFactory),
        actionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.setActive(true);

    // Create a Task and add it to the TaskQueue
    final TestTask task = new TestTask("t1", Intervals.of("2021-01/P1M"));
    taskQueue.add(task);

    // Acquire a lock for the Task
    getLockbox().lock(
        task,
        new TimeChunkLockRequest(TaskLockType.EXCLUSIVE, task, task.interval, null)
    );
    final List<TaskLock> locksForTask = getLockbox().findLocksForTask(task);
    Assert.assertEquals(1, locksForTask.size());
    Assert.assertEquals(task.interval, locksForTask.get(0).getInterval());

    // Verify that locks are removed on calling shutdown
    taskQueue.shutdown(task.getId(), "Shutdown Task test");
    Assert.assertTrue(getLockbox().findLocksForTask(task).isEmpty());

    Optional<TaskStatus> statusOptional = getTaskStorage().getStatus(task.getId());
    Assert.assertTrue(statusOptional.isPresent());
    Assert.assertEquals(TaskState.FAILED, statusOptional.get().getStatusCode());
    Assert.assertNotNull(statusOptional.get().getErrorMsg());
    Assert.assertEquals("Shutdown Task test", statusOptional.get().getErrorMsg());
  }

  @Test(expected = DruidException.class)
  public void testTaskErrorWhenExceptionIsThrownDueToQueueSize()
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
            new TaskLockConfig(),
            new TaskQueueConfig(1, null, null, null, null),
            new DefaultTaskConfig(),
            getTaskStorage(),
            new SimpleTaskRunner(actionClientFactory),
            actionClientFactory,
            getLockbox(),
            new NoopServiceEmitter()
    );
    taskQueue.setActive(true);

    // Create a Task and add it to the TaskQueue
    final TestTask task1 = new TestTask("t1", Intervals.of("2021-01/P1M"));
    final TestTask task2 = new TestTask("t2", Intervals.of("2021-01/P1M"));
    taskQueue.add(task1);

    // we will get exception here as taskQueue size is 1 druid.indexer.queue.maxSize is already 1
    taskQueue.add(task2);
  }

  @Test
  public void testSetUseLineageBasedSegmentAllocationByDefault() throws EntryExistsException
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        new SimpleTaskRunner(actionClientFactory),
        actionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.setActive(true);
    final Task task = new TestTask("t1", Intervals.of("2021-01-01/P1D"));
    taskQueue.add(task);
    final List<Task> tasks = taskQueue.getTasks();
    Assert.assertEquals(1, tasks.size());
    final Task queuedTask = tasks.get(0);
    Assert.assertTrue(
        queuedTask.getContextValue(SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY)
    );
  }

  @Test
  public void testDefaultTaskContextOverrideDefaultLineageBasedSegmentAllocation() throws EntryExistsException
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig()
        {
          @Override
          public Map<String, Object> getContext()
          {
            return ImmutableMap.of(
                SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
                false
            );
          }
        },
        getTaskStorage(),
        new SimpleTaskRunner(actionClientFactory),
        actionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.setActive(true);
    final Task task = new TestTask("t1", Intervals.of("2021-01-01/P1D"));
    taskQueue.add(task);
    final List<Task> tasks = taskQueue.getTasks();
    Assert.assertEquals(1, tasks.size());
    final Task queuedTask = tasks.get(0);
    Assert.assertFalse(
        queuedTask.getContextValue(SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY)
    );
  }

  @Test
  public void testUserProvidedTaskContextOverrideDefaultLineageBasedSegmentAllocation() throws EntryExistsException
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        new SimpleTaskRunner(actionClientFactory),
        actionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.setActive(true);
    final Task task = new TestTask(
        "t1",
        Intervals.of("2021-01-01/P1D"),
        ImmutableMap.of(
            SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
            false
        )
    );
    taskQueue.add(task);
    final List<Task> tasks = taskQueue.getTasks();
    Assert.assertEquals(1, tasks.size());
    final Task queuedTask = tasks.get(0);
    Assert.assertFalse(
        queuedTask.getContextValue(SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY)
    );
  }

  @Test
  public void testLockConfigTakePrecedenceThanDefaultTaskContext() throws EntryExistsException
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig()
        {
          @Override
          public Map<String, Object> getContext()
          {
            return ImmutableMap.of(
                Tasks.FORCE_TIME_CHUNK_LOCK_KEY,
                false
            );
          }
        },
        getTaskStorage(),
        new SimpleTaskRunner(actionClientFactory),
        actionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.setActive(true);
    final Task task = new TestTask("t1", Intervals.of("2021-01-01/P1D"));
    taskQueue.add(task);
    final List<Task> tasks = taskQueue.getTasks();
    Assert.assertEquals(1, tasks.size());
    final Task queuedTask = tasks.get(0);
    Assert.assertTrue(queuedTask.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY));
  }

  @Test
  public void testUserProvidedContextOverrideLockConfig() throws EntryExistsException
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        new SimpleTaskRunner(actionClientFactory),
        actionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.setActive(true);
    final Task task = new TestTask(
        "t1",
        Intervals.of("2021-01-01/P1D"),
        ImmutableMap.of(
            Tasks.FORCE_TIME_CHUNK_LOCK_KEY,
            false
        )
    );
    taskQueue.add(task);
    final List<Task> tasks = taskQueue.getTasks();
    Assert.assertEquals(1, tasks.size());
    final Task queuedTask = tasks.get(0);
    Assert.assertFalse(queuedTask.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY));
  }

  @Test
  public void testTaskStatusWhenExceptionIsThrownInIsReady() throws EntryExistsException
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        new SimpleTaskRunner(actionClientFactory),
        actionClientFactory,
        getLockbox(),
        new NoopServiceEmitter()
    );
    taskQueue.setActive(true);
    final Task task = new TestTask("t1", Intervals.of("2021-01-01/P1D"))
    {
      @Override
      public boolean isReady(TaskActionClient taskActionClient)
      {
        throw new RuntimeException("isReady failure test");
      }
    };
    taskQueue.add(task);
    taskQueue.manageInternal();

    Optional<TaskStatus> statusOptional = getTaskStorage().getStatus(task.getId());
    Assert.assertTrue(statusOptional.isPresent());
    Assert.assertEquals(TaskState.FAILED, statusOptional.get().getStatusCode());
    Assert.assertNotNull(statusOptional.get().getErrorMsg());
    Assert.assertTrue(
        StringUtils.format("Actual message is: %s", statusOptional.get().getErrorMsg()),
        statusOptional.get().getErrorMsg().startsWith("Failed while waiting for the task to be ready to run")
    );
  }

  @Test
  public void testKilledTasksEmitRuntimeMetricWithHttpRemote() throws EntryExistsException, InterruptedException
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final HttpRemoteTaskRunner taskRunner = createHttpRemoteTaskRunner(ImmutableList.of("t1"));
    final StubServiceEmitter metricsVerifier = new StubServiceEmitter("druid/overlord", "testHost");
    WorkerHolder workerHolder = EasyMock.createMock(WorkerHolder.class);
    EasyMock.expect(workerHolder.getWorker())
            .andReturn(new Worker("http", "worker", "127.0.0.1", 1, "v1", WorkerConfig.DEFAULT_CATEGORY))
            .anyTimes();
    workerHolder.incrementContinuouslyFailedTasksCount();
    EasyMock.expectLastCall();
    workerHolder.setLastCompletedTaskTime(EasyMock.anyObject());
    EasyMock.expect(workerHolder.getContinuouslyFailedTasksCount()).andReturn(1);
    EasyMock.replay(workerHolder);
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        getTaskStorage(),
        taskRunner,
        actionClientFactory,
        getLockbox(),
        metricsVerifier
    );
    taskQueue.setActive(true);
    final Task task = new TestTask(
        "t1",
        Intervals.of("2021-01-01/P1D"),
        ImmutableMap.of(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, false)
    );
    taskQueue.add(task);
    taskQueue.manageInternal();

    // Announce the task and wait for it to start running
    final String taskId = task.getId();
    final TaskLocation taskLocation = TaskLocation.create("worker", 1, 2);
    taskRunner.taskAddedOrUpdated(
        TaskAnnouncement.create(task, TaskStatus.running(taskId), taskLocation),
        workerHolder
    );
    while (taskRunner.getRunnerTaskState(taskId) != RunnerTaskState.RUNNING) {
      Thread.sleep(100);
    }

    // Kill the task, send announcement and wait for TaskQueue to handle update
    taskQueue.shutdown(taskId, "shutdown");
    taskRunner.taskAddedOrUpdated(
        TaskAnnouncement.create(task, TaskStatus.failure(taskId, "shutdown"), taskLocation),
        workerHolder
    );
    taskQueue.manageInternal();
    Thread.sleep(100);

    // Verify that metrics are emitted on receiving announcement
    metricsVerifier.verifyEmitted("task/run/time", 1);
    CoordinatorRunStats stats = taskQueue.getQueueStats();
    Assert.assertEquals(0L, stats.get(Stats.TaskQueue.STATUS_UPDATES_IN_QUEUE));
    Assert.assertEquals(1L, stats.get(Stats.TaskQueue.HANDLED_STATUS_UPDATES));
  }

  @Test
  public void testGetTaskStatus()
  {
    final String newTask = "newTask";
    final String waitingTask = "waitingTask";
    final String pendingTask = "pendingTask";
    final String runningTask = "runningTask";
    final String successfulTask = "successfulTask";
    final String failedTask = "failedTask";

    TaskStorage taskStorage = EasyMock.createMock(TaskStorage.class);
    EasyMock.expect(taskStorage.getStatus(newTask))
            .andReturn(Optional.of(TaskStatus.running(newTask)));
    EasyMock.expect(taskStorage.getStatus(successfulTask))
            .andReturn(Optional.of(TaskStatus.success(successfulTask)));
    EasyMock.expect(taskStorage.getStatus(failedTask))
            .andReturn(Optional.of(TaskStatus.failure(failedTask, failedTask)));
    EasyMock.replay(taskStorage);

    TaskRunner taskRunner = EasyMock.createMock(HttpRemoteTaskRunner.class);
    EasyMock.expect(taskRunner.getRunnerTaskState(newTask))
            .andReturn(null);
    EasyMock.expect(taskRunner.getRunnerTaskState(waitingTask))
            .andReturn(RunnerTaskState.WAITING);
    EasyMock.expect(taskRunner.getRunnerTaskState(pendingTask))
            .andReturn(RunnerTaskState.PENDING);
    EasyMock.expect(taskRunner.getRunnerTaskState(runningTask))
            .andReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(taskRunner.getRunnerTaskState(successfulTask))
            .andReturn(RunnerTaskState.NONE);
    EasyMock.expect(taskRunner.getRunnerTaskState(failedTask))
            .andReturn(RunnerTaskState.NONE);
    EasyMock.expect(taskRunner.getTaskLocation(waitingTask))
            .andReturn(TaskLocation.unknown());
    EasyMock.expect(taskRunner.getTaskLocation(pendingTask))
            .andReturn(TaskLocation.unknown());
    EasyMock.expect(taskRunner.getTaskLocation(runningTask))
            .andReturn(TaskLocation.create("host", 8100, 8100));
    EasyMock.replay(taskRunner);

    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        taskStorage,
        taskRunner,
        createActionClientFactory(),
        getLockbox(),
        new StubServiceEmitter("druid/overlord", "testHost")
    );
    taskQueue.setActive(true);

    Assert.assertEquals(TaskStatus.running(newTask), taskQueue.getTaskStatus(newTask).get());
    Assert.assertEquals(TaskStatus.running(waitingTask), taskQueue.getTaskStatus(waitingTask).get());
    Assert.assertEquals(TaskStatus.running(pendingTask), taskQueue.getTaskStatus(pendingTask).get());
    Assert.assertEquals(TaskStatus.running(runningTask), taskQueue.getTaskStatus(runningTask).get());
    Assert.assertEquals(TaskStatus.success(successfulTask), taskQueue.getTaskStatus(successfulTask).get());
    Assert.assertEquals(TaskStatus.failure(failedTask, failedTask), taskQueue.getTaskStatus(failedTask).get());
  }

  private HttpRemoteTaskRunner createHttpRemoteTaskRunner(List<String> runningTasks)
  {
    HttpRemoteTaskRunnerTest.TestDruidNodeDiscovery druidNodeDiscovery = new HttpRemoteTaskRunnerTest.TestDruidNodeDiscovery();
    DruidNodeDiscoveryProvider druidNodeDiscoveryProvider = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(druidNodeDiscovery);
    EasyMock.replay(druidNodeDiscoveryProvider);
    TaskStorage taskStorageMock = EasyMock.createStrictMock(TaskStorage.class);
    for (String taskId : runningTasks) {
      EasyMock.expect(taskStorageMock.getStatus(taskId)).andReturn(Optional.of(TaskStatus.running(taskId)));
    }
    EasyMock.replay(taskStorageMock);
    HttpRemoteTaskRunner taskRunner = new HttpRemoteTaskRunner(
        TestHelper.makeJsonMapper(),
        new HttpRemoteTaskRunnerConfig()
        {
          @Override
          public int getPendingTasksRunnerNumThreads()
          {
            return 3;
          }
        },
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        EasyMock.createNiceMock(TaskStorage.class),
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null),
        new StubServiceEmitter("druid/overlord", "testHost")
    );

    taskRunner.start();
    taskRunner.registerListener(
        new TaskRunnerListener()
        {
          @Override
          public String getListenerId()
          {
            return "test-listener";
          }

          @Override
          public void locationChanged(String taskId, TaskLocation newLocation)
          {
            // do nothing
          }

          @Override
          public void statusChanged(String taskId, TaskStatus status)
          {
            // do nothing
          }
        },
        Execs.directExecutor()
    );

    return taskRunner;
  }

  private static class TestTask extends AbstractBatchIndexTask
  {
    private final Interval interval;
    private boolean done;

    private TestTask(String id, Interval interval)
    {
      this(id, interval, null);
    }

    private TestTask(String id, Interval interval, Map<String, Object> context)
    {
      super(id, "datasource", context, IngestionMode.NONE);
      this.interval = interval;
    }

    @Override
    public boolean isReady(TaskActionClient taskActionClient) throws Exception
    {
      return tryTimeChunkLock(taskActionClient, ImmutableList.of(interval));
    }

    @Override
    public String setup(TaskToolbox toolbox)
    {
      // do nothing
      return null;
    }

    @Override
    public void cleanUp(TaskToolbox toolbox, TaskStatus taskStatus)
    {
      // do nothing
    }

    @Override
    public TaskStatus runTask(TaskToolbox toolbox)
    {
      done = true;
      return TaskStatus.success(getId());
    }

    @Override
    public boolean requireLockExistingSegments()
    {
      return false;
    }

    @Override
    public List<DataSegment> findSegmentsToLock(TaskActionClient taskActionClient, List<Interval> intervals)
    {
      return null;
    }

    @Override
    public boolean isPerfectRollup()
    {
      return false;
    }

    @Nullable
    @Override
    public Granularity getSegmentGranularity()
    {
      return SEGMENT_GRANULARITY;
    }

    @Override
    public String getType()
    {
      return "test";
    }

    public boolean isDone()
    {
      return done;
    }
  }

  private static class SimpleTaskRunner implements TaskRunner
  {
    private final TaskActionClientFactory actionClientFactory;

    private SimpleTaskRunner(TaskActionClientFactory actionClientFactory)
    {
      this.actionClientFactory = actionClientFactory;
    }

    @Override
    public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
    {
      return null;
    }

    @Override
    public void start()
    {
    }

    @Override
    public void registerListener(TaskRunnerListener listener, Executor executor)
    {
    }

    @Override
    public void unregisterListener(String listenerId)
    {
    }

    @Override
    public ListenableFuture<TaskStatus> run(Task task)
    {
      try {
        final TaskToolbox toolbox = Mockito.mock(TaskToolbox.class);
        Mockito.when(toolbox.getTaskActionClient()).thenReturn(actionClientFactory.create(task));
        return Futures.immediateFuture(task.run(toolbox));
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void shutdown(String taskid, String reason)
    {
    }

    @Override
    public void stop()
    {
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
    {
      return null;
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
    {
      return null;
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
    {
      return Collections.emptyList();
    }

    @Override
    public Optional<ScalingStats> getScalingStats()
    {
      return null;
    }

    @Override
    public Map<String, Long> getTotalTaskSlotCount()
    {
      return ImmutableMap.of(WorkerConfig.DEFAULT_CATEGORY, 0L);
    }

    @Override
    public Map<String, Long> getIdleTaskSlotCount()
    {
      return ImmutableMap.of(WorkerConfig.DEFAULT_CATEGORY, 0L);
    }

    @Override
    public Map<String, Long> getUsedTaskSlotCount()
    {
      return ImmutableMap.of(WorkerConfig.DEFAULT_CATEGORY, 0L);
    }

    @Override
    public Map<String, Long> getLazyTaskSlotCount()
    {
      return ImmutableMap.of(WorkerConfig.DEFAULT_CATEGORY, 0L);
    }

    @Override
    public Map<String, Long> getBlacklistedTaskSlotCount()
    {
      return ImmutableMap.of(WorkerConfig.DEFAULT_CATEGORY, 0L);
    }
  }
}
