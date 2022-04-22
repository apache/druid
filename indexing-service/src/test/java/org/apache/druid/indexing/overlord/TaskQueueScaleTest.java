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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.indexer.RunnerTaskState;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.actions.TaskAction;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.ScheduledExecutors;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.IndexerSQLMetadataStorageCoordinator;
import org.apache.druid.metadata.TaskLookup;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.joda.time.Duration;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Tests that {@link TaskQueue} is able to handle large numbers of concurrently-running tasks.
 */
public class TaskQueueScaleTest
{
  private static final String DATASOURCE = "ds";

  private final int numTasks = 1000;

  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private TaskQueue taskQueue;
  private TaskStorage taskStorage;
  private TestTaskRunner taskRunner;
  private Closer closer;

  @Before
  public void setUp()
  {
    EmittingLogger.registerEmitter(new NoopServiceEmitter());

    closer = Closer.create();

    // Be as realistic as possible; use actual classes for storage rather than mocks.
    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(Period.hours(1)));
    taskRunner = new TestTaskRunner();
    closer.register(taskRunner::stop);
    final ObjectMapper jsonMapper = TestHelper.makeJsonMapper();

    final IndexerSQLMetadataStorageCoordinator storageCoordinator = new IndexerSQLMetadataStorageCoordinator(
        jsonMapper,
        derbyConnectorRule.metadataTablesConfigSupplier().get(),
        derbyConnectorRule.getConnector()
    );

    final TaskActionClientFactory unsupportedTaskActionFactory =
        task -> new TaskActionClient()
        {
          @Override
          public <RetType> RetType submit(TaskAction<RetType> taskAction)
          {
            throw new UnsupportedOperationException();
          }
        };

    taskQueue = new TaskQueue(
        new TaskLockConfig(),
        new TaskQueueConfig(null, Period.millis(1), null, null),
        new DefaultTaskConfig(),
        taskStorage,
        taskRunner,
        unsupportedTaskActionFactory, // Not used for anything serious
        new TaskLockbox(taskStorage, storageCoordinator),
        new NoopServiceEmitter()
    );

    taskQueue.start();
    closer.register(taskQueue::stop);
  }

  @After
  public void tearDown() throws Exception
  {
    closer.close();
  }

  @Test(timeout = 60_000L) // more than enough time if the task queue is efficient
  public void doMassLaunchAndExit() throws Exception
  {
    Assert.assertEquals("no tasks should be running", 0, taskRunner.getKnownTasks().size());
    Assert.assertEquals("no tasks should be known", 0, taskQueue.getTasks().size());
    Assert.assertEquals("no tasks should be running", 0, taskQueue.getRunningTaskCount().size());

    // Add all tasks.
    for (int i = 0; i < numTasks; i++) {
      final TestTask testTask = new TestTask(i, 2000L /* runtime millis */);
      taskQueue.add(testTask);
    }

    // in theory we can get a race here, since we fetch the counts at separate times
    Assert.assertEquals("all tasks should be known", numTasks, taskQueue.getTasks().size());
    long runningTasks = taskQueue.getRunningTaskCount().values().stream().mapToLong(Long::longValue).sum();
    long pendingTasks = taskQueue.getPendingTaskCount().values().stream().mapToLong(Long::longValue).sum();
    long waitingTasks = taskQueue.getWaitingTaskCount().values().stream().mapToLong(Long::longValue).sum();
    Assert.assertEquals("all tasks should be known", numTasks, (runningTasks + pendingTasks + waitingTasks));

    // Wait for all tasks to finish.
    final TaskLookup.CompleteTaskLookup completeTaskLookup =
        TaskLookup.CompleteTaskLookup.of(numTasks, Duration.standardHours(1));

    while (taskStorage.getTaskInfos(completeTaskLookup, DATASOURCE).size() < numTasks) {
      Thread.sleep(100);
    }

    Thread.sleep(100);

    Assert.assertEquals("no tasks should be active", 0, taskStorage.getActiveTasks().size());
    runningTasks = taskQueue.getRunningTaskCount().values().stream().mapToLong(Long::longValue).sum();
    pendingTasks = taskQueue.getPendingTaskCount().values().stream().mapToLong(Long::longValue).sum();
    waitingTasks = taskQueue.getWaitingTaskCount().values().stream().mapToLong(Long::longValue).sum();
    Assert.assertEquals("no tasks should be running", 0, runningTasks);
    Assert.assertEquals("no tasks should be pending", 0, pendingTasks);
    Assert.assertEquals("no tasks should be waiting", 0, waitingTasks);
  }

  @Test(timeout = 60_000L) // more than enough time if the task queue is efficient
  public void doMassLaunchAndShutdown() throws Exception
  {
    Assert.assertEquals("no tasks should be running", 0, taskRunner.getKnownTasks().size());

    // Add all tasks.
    final List<String> taskIds = new ArrayList<>();
    for (int i = 0; i < numTasks; i++) {
      final TestTask testTask = new TestTask(
          i,
          Duration.standardHours(1).getMillis() /* very long runtime millis, so we can do a shutdown */
      );
      taskQueue.add(testTask);
      taskIds.add(testTask.getId());
    }

    // wait for all tasks to progress to running state
    while (taskStorage.getActiveTasks().size() < numTasks) {
      Thread.sleep(100);
    }
    Assert.assertEquals("all tasks should be running", numTasks, taskStorage.getActiveTasks().size());

    // Shut down all tasks.
    for (final String taskId : taskIds) {
      taskQueue.shutdown(taskId, "test shutdown");
    }

    // Wait for all tasks to finish.
    while (!taskStorage.getActiveTasks().isEmpty()) {
      Thread.sleep(100);
    }

    Assert.assertEquals("no tasks should be running", 0, taskStorage.getActiveTasks().size());

    int completed = taskStorage.getTaskInfos(
        TaskLookup.CompleteTaskLookup.of(numTasks, Duration.standardHours(1)),
        DATASOURCE
    ).size();
    Assert.assertEquals("all tasks should have completed", numTasks, completed);
  }

  private static class TestTask extends NoopTask
  {
    private final int number;
    private final long runtime;

    public TestTask(int number, long runtime)
    {
      super(null, null, DATASOURCE, 0, 0, null, null, Collections.emptyMap());
      this.number = number;
      this.runtime = runtime;
    }

    public int getNumber()
    {
      return number;
    }

    public long getRuntimeMillis()
    {
      return runtime;
    }
  }

  private static class TestTaskRunner implements TaskRunner
  {
    private static final Logger log = new Logger(TestTaskRunner.class);
    private static final Duration T_PENDING_TO_RUNNING = Duration.standardSeconds(2);
    private static final Duration T_SHUTDOWN_ACK = Duration.millis(8);
    private static final Duration T_SHUTDOWN_COMPLETE = Duration.standardSeconds(2);

    @GuardedBy("knownTasks")
    private final Map<String, TestTaskRunnerWorkItem> knownTasks = new HashMap<>();

    private final ScheduledExecutorService exec = ScheduledExecutors.fixed(8, "TaskQueueScaleTest-%s");

    @Override
    public void start()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<TaskStatus> run(Task task)
    {
      // Production task runners generally do not take a long time to execute "run", but may take a long time to
      // go from "running" to "pending".
      synchronized (knownTasks) {
        final TestTaskRunnerWorkItem item = knownTasks.computeIfAbsent(task.getId(), TestTaskRunnerWorkItem::new);
        exec.schedule(
            () -> {
              try {
                synchronized (knownTasks) {
                  final TestTaskRunnerWorkItem item2 = knownTasks.get(task.getId());
                  if (item2.getState() == RunnerTaskState.PENDING) {
                    knownTasks.put(task.getId(), item2.withState(RunnerTaskState.RUNNING));
                  }
                }

                exec.schedule(
                    () -> {
                      try {
                        final TestTaskRunnerWorkItem item2;
                        synchronized (knownTasks) {
                          item2 = knownTasks.get(task.getId());
                          knownTasks.put(task.getId(), item2.withState(RunnerTaskState.NONE));
                        }
                        if (item2 != null) {
                          item2.setResult(TaskStatus.success(task.getId()));
                        }
                      }
                      catch (Throwable e) {
                        log.error(e, "Error in scheduled executor");
                      }
                    },
                    ((TestTask) task).getRuntimeMillis(),
                    TimeUnit.MILLISECONDS
                );
              }
              catch (Throwable e) {
                log.error(e, "Error in scheduled executor");
              }
            },
            T_PENDING_TO_RUNNING.getMillis(),
            TimeUnit.MILLISECONDS
        );

        return item.getResult();
      }
    }

    @Override
    public void shutdown(String taskid, String reason)
    {
      // Production task runners take a long time to execute "shutdown" if the task is currently running.
      synchronized (knownTasks) {
        if (!knownTasks.containsKey(taskid)) {
          return;
        }
      }

      threadSleep(T_SHUTDOWN_ACK);

      final TestTaskRunnerWorkItem existingTask;
      synchronized (knownTasks) {
        existingTask = knownTasks.get(taskid);
      }
      if (!existingTask.getResult().isDone()) {
        exec.schedule(() -> {
          existingTask.setResult(TaskStatus.failure("taskId", "stopped"));
          synchronized (knownTasks) {
            knownTasks.remove(taskid);
          }
        }, T_SHUTDOWN_COMPLETE.getMillis(), TimeUnit.MILLISECONDS);
      }
    }

    static void threadSleep(Duration duration)
    {
      try {
        Thread.sleep(duration.getMillis());
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }

    @Override
    public void registerListener(TaskRunnerListener listener, Executor executor)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unregisterListener(String listenerId)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
    {
      // Do nothing, and return null. (TaskQueue doesn't use the return value.)
      return null;
    }

    @Override
    public void stop()
    {
      exec.shutdownNow();
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getRunningTasks()
    {
      synchronized (knownTasks) {
        return knownTasks.values()
                         .stream()
                         .filter(item -> item.getState() == RunnerTaskState.RUNNING)
                         .collect(Collectors.toList());
      }
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
    {
      synchronized (knownTasks) {
        return knownTasks.values()
                         .stream()
                         .filter(item -> item.getState() == RunnerTaskState.PENDING)
                         .collect(Collectors.toList());
      }
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
    {
      synchronized (knownTasks) {
        return ImmutableList.copyOf(knownTasks.values());
      }
    }

    @Override
    public Optional<ScalingStats> getScalingStats()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getTotalTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getIdleTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getUsedTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getLazyTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getBlacklistedTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }
  }

  private static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final RunnerTaskState state;

    public TestTaskRunnerWorkItem(final String taskId)
    {
      this(taskId, SettableFuture.create(), RunnerTaskState.PENDING);
    }

    private TestTaskRunnerWorkItem(
        final String taskId,
        final ListenableFuture<TaskStatus> result,
        final RunnerTaskState state
    )
    {
      super(taskId, result);
      this.state = state;
    }

    public RunnerTaskState getState()
    {
      return state;
    }

    @Override
    public TaskLocation getLocation()
    {
      return TaskLocation.unknown();
    }

    @Nullable
    @Override
    public String getTaskType()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getDataSource()
    {
      throw new UnsupportedOperationException();
    }

    public void setResult(final TaskStatus result)
    {
      ((SettableFuture<TaskStatus>) getResult()).set(result);

      // possibly a parallel shutdown request was issued during the
      // shutdown time; ignore it
    }

    public TestTaskRunnerWorkItem withState(final RunnerTaskState newState)
    {
      return new TestTaskRunnerWorkItem(getTaskId(), getResult(), newState);
    }
  }
}

