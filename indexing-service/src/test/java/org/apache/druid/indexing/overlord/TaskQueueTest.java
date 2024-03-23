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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.common.guava.DSuppliers;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.NoopInputFormat;
import org.apache.druid.data.input.impl.TimestampSpec;
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
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.AbstractBatchIndexTask;
import org.apache.druid.indexing.common.task.IngestionTestBase;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIOConfig;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexIngestionSpec;
import org.apache.druid.indexing.common.task.batch.parallel.ParallelIndexSupervisorTask;
import org.apache.druid.indexing.common.task.batch.parallel.SinglePhaseParallelIndexTaskRunner;
import org.apache.druid.indexing.overlord.autoscaling.NoopProvisioningStrategy;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunner;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerTest;
import org.apache.druid.indexing.overlord.hrtr.WorkerHolder;
import org.apache.druid.indexing.overlord.setup.DefaultWorkerBehaviorConfig;
import org.apache.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.DerbyMetadataStorageActionHandlerFactory;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.timeline.DataSegment;
import org.easymock.EasyMock;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

public class TaskQueueTest extends IngestionTestBase
{
  private static final Granularity SEGMENT_GRANULARITY = Granularities.DAY;

  private TaskActionClientFactory actionClientFactory;
  private TaskQueue taskQueue;
  private StubServiceEmitter serviceEmitter;
  private Map<String, Object> defaultTaskContext;

  @Override
  public void setUpIngestionTestBase() throws IOException
  {
    super.setUpIngestionTestBase();
    serviceEmitter = new StubServiceEmitter();
    actionClientFactory = createActionClientFactory();
    defaultTaskContext = new HashMap<>();

    taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(3, null, null, null, null),
        new DefaultTaskConfig()
        {
          @Override
          public Map<String, Object> getContext()
          {
            return defaultTaskContext;
          }
        },
        getTaskStorage(),
        new SimpleTaskRunner(),
        actionClientFactory,
        getLockbox(),
        serviceEmitter,
        getObjectMapper()
    );
    taskQueue.setActive();
  }

  @Test
  public void testManageInternalReleaseLockWhenTaskIsNotReady() throws Exception
  {
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

  @Test
  public void testAddThrowsExceptionWhenQueueIsFull()
  {
    // Fill up the queue
    for (int i = 0; i < 3; ++i) {
      taskQueue.add(new TestTask("t_" + i, Intervals.of("2021-01/P1M")));
    }

    // Verify that adding another task throws an exception
    Assert.assertThrows(
        DruidException.class,
        () -> taskQueue.add(new TestTask("tx", Intervals.of("2021-01/P1M")))
    );
  }

  @Test
  public void testAddedTaskUsesLineageBasedSegmentAllocationByDefault()
  {
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
  public void testDefaultTaskContextOverrideDefaultLineageBasedSegmentAllocation()
  {
    defaultTaskContext.put(
        SinglePhaseParallelIndexTaskRunner.CTX_USE_LINEAGE_BASED_SEGMENT_ALLOCATION_KEY,
        false
    );
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
  public void testUserProvidedTaskContextOverrideDefaultLineageBasedSegmentAllocation()
  {
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
  public void testLockConfigTakePrecedenceThanDefaultTaskContext()
  {
    defaultTaskContext.put(Tasks.FORCE_TIME_CHUNK_LOCK_KEY, false);
    final Task task = new TestTask("t1", Intervals.of("2021-01-01/P1D"));
    taskQueue.add(task);
    final List<Task> tasks = taskQueue.getTasks();
    Assert.assertEquals(1, tasks.size());
    final Task queuedTask = tasks.get(0);
    Assert.assertTrue(queuedTask.getContextValue(Tasks.FORCE_TIME_CHUNK_LOCK_KEY));
  }

  @Test
  public void testUserProvidedContextOverrideLockConfig()
  {
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
  public void testExceptionInIsReadyFailsTask()
  {
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
        statusOptional.get().getErrorMsg().startsWith("Failed while waiting for the task to be ready to run")
    );
  }

  @Test
  public void testKilledTasksEmitRuntimeMetricWithHttpRemote() throws InterruptedException
  {
    final HttpRemoteTaskRunner taskRunner = createHttpRemoteTaskRunner();
    taskRunner.start();

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
        serviceEmitter,
        getObjectMapper()
    );
    taskQueue.setActive();
    final Task task = new TestTask("t1", Intervals.of("2021-01-01/P1D"));
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
    serviceEmitter.verifyEmitted("task/run/time", 1);
    CoordinatorRunStats stats = taskQueue.getQueueStats();
    Assert.assertEquals(0L, stats.get(Stats.TaskQueue.STATUS_UPDATES_IN_QUEUE));
    Assert.assertEquals(1L, stats.get(Stats.TaskQueue.HANDLED_STATUS_UPDATES));
  }

  @Test
  public void testGetTaskStatus()
  {
    final TaskRunner taskRunner = EasyMock.createMock(TaskRunner.class);
    final TaskStorage taskStorage = EasyMock.createMock(TaskStorage.class);

    final String newTask = "newTask";
    EasyMock.expect(taskRunner.getRunnerTaskState(newTask))
            .andReturn(null);
    EasyMock.expect(taskStorage.getStatus(newTask))
            .andReturn(Optional.of(TaskStatus.running(newTask)));

    final String waitingTask = "waitingTask";
    EasyMock.expect(taskRunner.getRunnerTaskState(waitingTask))
            .andReturn(RunnerTaskState.WAITING);
    EasyMock.expect(taskRunner.getTaskLocation(waitingTask))
            .andReturn(TaskLocation.unknown());

    final String pendingTask = "pendingTask";
    EasyMock.expect(taskRunner.getRunnerTaskState(pendingTask))
            .andReturn(RunnerTaskState.PENDING);
    EasyMock.expect(taskRunner.getTaskLocation(pendingTask))
            .andReturn(TaskLocation.unknown());

    final String runningTask = "runningTask";
    EasyMock.expect(taskRunner.getRunnerTaskState(runningTask))
            .andReturn(RunnerTaskState.RUNNING);
    EasyMock.expect(taskRunner.getTaskLocation(runningTask))
            .andReturn(TaskLocation.create("host", 8100, 8100));

    final String successfulTask = "successfulTask";
    EasyMock.expect(taskRunner.getRunnerTaskState(successfulTask))
            .andReturn(RunnerTaskState.NONE);
    EasyMock.expect(taskStorage.getStatus(successfulTask))
            .andReturn(Optional.of(TaskStatus.success(successfulTask)));

    final String failedTask = "failedTask";
    EasyMock.expect(taskRunner.getRunnerTaskState(failedTask))
            .andReturn(RunnerTaskState.NONE);
    EasyMock.expect(taskStorage.getStatus(failedTask))
            .andReturn(Optional.of(TaskStatus.failure(failedTask, failedTask)));

    EasyMock.replay(taskRunner, taskStorage);

    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        taskStorage,
        taskRunner,
        actionClientFactory,
        getLockbox(),
        serviceEmitter,
        getObjectMapper()
    );
    taskQueue.setActive();

    Assert.assertEquals(TaskStatus.running(newTask), taskQueue.getTaskStatus(newTask).get());
    Assert.assertEquals(TaskStatus.running(waitingTask), taskQueue.getTaskStatus(waitingTask).get());
    Assert.assertEquals(TaskStatus.running(pendingTask), taskQueue.getTaskStatus(pendingTask).get());
    Assert.assertEquals(TaskStatus.running(runningTask), taskQueue.getTaskStatus(runningTask).get());
    Assert.assertEquals(TaskStatus.success(successfulTask), taskQueue.getTaskStatus(successfulTask).get());
    Assert.assertEquals(TaskStatus.failure(failedTask, failedTask), taskQueue.getTaskStatus(failedTask).get());
  }

  @Test
  public void testGetActiveTaskRedactsPassword() throws JsonProcessingException
  {
    final String password = "AbCd_1234";
    final ObjectMapper mapper = getObjectMapper();

    final HttpInputSourceConfig httpInputSourceConfig = new HttpInputSourceConfig(Collections.singleton("http"));
    mapper.setInjectableValues(new InjectableValues.Std()
                                   .addValue(HttpInputSourceConfig.class, httpInputSourceConfig)
                                   .addValue(ObjectMapper.class, new DefaultObjectMapper())
    );

    final SQLMetadataConnector derbyConnector = derbyConnectorRule.getConnector();
    final TaskStorage taskStorage = new MetadataTaskStorage(
        derbyConnector,
        new TaskStorageConfig(null),
        new DerbyMetadataStorageActionHandlerFactory(
            derbyConnector,
            derbyConnectorRule.metadataTablesConfigSupplier().get(),
            mapper
        )
    );

    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null, null),
        new DefaultTaskConfig(),
        taskStorage,
        EasyMock.createMock(HttpRemoteTaskRunner.class),
        createActionClientFactory(),
        new TaskLockbox(taskStorage, new TestIndexerMetadataStorageCoordinator()),
        new StubServiceEmitter("druid/overlord", "testHost"),
        mapper
    );

    final DataSchema dataSchema = new DataSchema(
        "DS",
        new TimestampSpec(null, null, null),
        new DimensionsSpec(null),
        null,
        new UniformGranularitySpec(Granularities.YEAR, Granularities.DAY, null),
        null
    );
    final ParallelIndexIOConfig ioConfig = new ParallelIndexIOConfig(
        null,
        new HttpInputSource(Collections.singletonList(URI.create("http://host.org")),
                            "user",
                            new DefaultPasswordProvider(password),
                            null,
                            httpInputSourceConfig),
        new NoopInputFormat(),
        null,
        null
    );
    final ParallelIndexSupervisorTask taskWithPassword = new ParallelIndexSupervisorTask(
        "taskWithPassword",
        "taskWithPassword",
        null,
        new ParallelIndexIngestionSpec(
            dataSchema,
            ioConfig,
            null
        ),
        null,
        null,
        false
    );
    Assert.assertTrue(mapper.writeValueAsString(taskWithPassword).contains(password));

    taskQueue.start();
    taskQueue.add(taskWithPassword);

    final Optional<Task> taskInStorage = taskStorage.getTask(taskWithPassword.getId());
    Assert.assertTrue(taskInStorage.isPresent());
    final String taskInStorageAsString = mapper.writeValueAsString(taskInStorage.get());
    Assert.assertFalse(taskInStorageAsString.contains(password));

    final Optional<Task> taskInQueue = taskQueue.getActiveTask(taskWithPassword.getId());
    Assert.assertTrue(taskInQueue.isPresent());
    final String taskInQueueAsString = mapper.writeValueAsString(taskInQueue.get());
    Assert.assertFalse(taskInQueueAsString.contains(password));

    Assert.assertEquals(taskInStorageAsString, taskInQueueAsString);
  }

  private HttpRemoteTaskRunner createHttpRemoteTaskRunner()
  {
    final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider
        = EasyMock.createMock(DruidNodeDiscoveryProvider.class);
    EasyMock.expect(druidNodeDiscoveryProvider.getForService(WorkerNodeService.DISCOVERY_SERVICE_KEY))
            .andReturn(new HttpRemoteTaskRunnerTest.TestDruidNodeDiscovery());
    EasyMock.replay(druidNodeDiscoveryProvider);

    return new HttpRemoteTaskRunner(
        getObjectMapper(),
        new HttpRemoteTaskRunnerConfig(),
        EasyMock.createNiceMock(HttpClient.class),
        DSuppliers.of(new AtomicReference<>(DefaultWorkerBehaviorConfig.defaultConfig())),
        new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        EasyMock.createNiceMock(TaskStorage.class),
        EasyMock.createNiceMock(CuratorFramework.class),
        new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null),
        serviceEmitter
    );
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

  private class SimpleTaskRunner extends SingleTaskBackgroundRunner
  {
    SimpleTaskRunner()
    {
      super(
          EasyMock.createMock(TaskToolboxFactory.class),
          null,
          serviceEmitter,
          new DruidNode("overlord", "localhost", false, 8091, null, true, false),
          null
      );
    }

    @Override
    public ListenableFuture<TaskStatus> run(Task task)
    {
      try {
        return Futures.immediateFuture(task.run(null));
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
