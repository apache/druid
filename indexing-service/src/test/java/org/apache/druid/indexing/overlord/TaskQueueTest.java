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
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.metadata.EntryExistsException;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.timeline.DataSegment;
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
        new TaskQueueConfig(null, null, null, null),
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
        new TaskQueueConfig(null, null, null, null),
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

  @Test
  public void testSetUseLineageBasedSegmentAllocationByDefault() throws EntryExistsException
  {
    final TaskActionClientFactory actionClientFactory = createActionClientFactory();
    final TaskQueue taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, null, null, null),
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
        new TaskQueueConfig(null, null, null, null),
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
        new TaskQueueConfig(null, null, null, null),
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
        new TaskQueueConfig(null, null, null, null),
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
        new TaskQueueConfig(null, null, null, null),
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
        new TaskQueueConfig(null, null, null, null),
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
