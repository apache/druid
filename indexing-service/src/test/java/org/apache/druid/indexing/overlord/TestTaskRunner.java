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
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.concurrent.TaskThreadPriority;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Runs multiple tasks in a JVM thread using an ExecutorService. This task runner is supposed to be used only for unit
 * tests.
 */
public class TestTaskRunner implements TaskRunner, QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(TestTaskRunner.class);

  private final ConcurrentHashMap<Integer, ListeningExecutorService> exec = new ConcurrentHashMap<>();
  private final Set<TestTaskRunnerWorkItem> runningItems = new ConcurrentSkipListSet<>();
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  private final TaskToolboxFactory toolboxFactory;
  private final TaskConfig taskConfig;
  private final TaskLocation taskLocation;

  private volatile boolean stopping = false;

  public TestTaskRunner(
      TaskToolboxFactory toolboxFactory,
      TaskConfig taskConfig,
      TaskLocation taskLocation
  )
  {
    this.toolboxFactory = Preconditions.checkNotNull(toolboxFactory, "toolboxFactory");
    this.taskConfig = taskConfig;
    this.taskLocation = taskLocation;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return ImmutableList.of();
  }

  @Override
  public void registerListener(TaskRunnerListener listener, Executor executor)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listener.getListenerId())) {
        throw new ISE("Listener [%s] already registered", listener.getListenerId());
      }
    }

    final Pair<TaskRunnerListener, Executor> listenerPair = Pair.of(listener, executor);

    // Location never changes for an existing task, so it's ok to add the listener first and then issue bootstrap
    // callbacks without any special synchronization.

    listeners.add(listenerPair);
    log.info("Registered listener [%s]", listener.getListenerId());
    for (TestTaskRunnerWorkItem item : runningItems) {
      TaskRunnerUtils.notifyLocationChanged(ImmutableList.of(listenerPair), item.getTaskId(), item.getLocation());
    }
  }

  @Override
  public void unregisterListener(String listenerId)
  {
    for (Pair<TaskRunnerListener, Executor> pair : listeners) {
      if (pair.lhs.getListenerId().equals(listenerId)) {
        listeners.remove(pair);
        log.info("Unregistered listener [%s]", listenerId);
        return;
      }
    }
  }

  private static ListeningExecutorService buildExecutorService(int priority)
  {
    return MoreExecutors.listeningDecorator(
        Execs.singleThreaded(
            "test-task-runner-%d-priority-" + priority,
            TaskThreadPriority.getThreadPriorityFromTaskPriority(priority)
        )
    );
  }

  @Override
  public void stop()
  {
    stopping = true;

    for (Map.Entry<Integer, ListeningExecutorService> entry : exec.entrySet()) {
      try {
        entry.getValue().shutdown();
      }
      catch (SecurityException ex) {
        throw new RuntimeException("I can't control my own threads!", ex);
      }
    }

    for (TestTaskRunnerWorkItem item : runningItems) {
      final Task task = item.getTask();
      final long start = System.currentTimeMillis();

      if (taskConfig.isRestoreTasksOnRestart() && task.canRestore()) {
        // Attempt graceful shutdown.
        log.info("Starting graceful shutdown of task[%s].", task.getId());

        try {
          task.stopGracefully(taskConfig);
          final TaskStatus taskStatus = item.getResult().get(
              new Interval(DateTimes.utc(start), taskConfig.getGracefulShutdownTimeout()).toDurationMillis(),
              TimeUnit.MILLISECONDS
          );

          // Ignore status, it doesn't matter for graceful shutdowns.
          log.info(
              "Graceful shutdown of task[%s] finished in %,dms.",
              task.getId(),
              System.currentTimeMillis() - start
          );

          TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), taskStatus);
        }
        catch (Exception e) {
          TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), TaskStatus.failure(task.getId()));
          throw new RE(e, "Graceful shutdown of task[%s] aborted with exception", task.getId());
        }
      } else {
        TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), TaskStatus.failure(task.getId()));
      }
    }

    // Ok, now interrupt everything.
    for (Map.Entry<Integer, ListeningExecutorService> entry : exec.entrySet()) {
      try {
        entry.getValue().shutdownNow();
      }
      catch (SecurityException ex) {
        throw new RuntimeException("I can't control my own threads!", ex);
      }
    }
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    final TaskToolbox toolbox = toolboxFactory.build(task);
    final Object taskPriorityObj = task.getContextValue(TaskThreadPriority.CONTEXT_KEY);
    int taskPriority = 0;
    try {
      taskPriority = taskPriorityObj == null ? 0 : Numbers.parseInt(taskPriorityObj);
    }
    catch (NumberFormatException e) {
      log.error(e, "Error parsing task priority [%s] for task [%s]", taskPriorityObj, task.getId());
    }
    final int finalTaskPriority = taskPriority;
    final ListenableFuture<TaskStatus> statusFuture = exec
        .computeIfAbsent(taskPriority, k -> buildExecutorService(finalTaskPriority))
        .submit(new TestTaskRunnerCallable(task, toolbox));
    final TestTaskRunnerWorkItem taskRunnerWorkItem = new TestTaskRunnerWorkItem(task, statusFuture);
    runningItems.add(taskRunnerWorkItem);
    Futures.addCallback(
        statusFuture,
        new FutureCallback<TaskStatus>()
        {
          @Override
          public void onSuccess(TaskStatus result)
          {
            runningItems.remove(taskRunnerWorkItem);
          }

          @Override
          public void onFailure(Throwable t)
          {
            runningItems.remove(taskRunnerWorkItem);
          }
        }
    );

    return statusFuture;
  }

  @Override
  public void shutdown(final String taskid, String reason)
  {
    for (final TaskRunnerWorkItem runningItem : runningItems) {
      if (runningItem.getTaskId().equals(taskid)) {
        runningItem.getResult().cancel(true);
      }
    }
  }

  @Override
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    return ImmutableList.copyOf(runningItems);
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    return ImmutableList.of();
  }

  @Override
  public Collection<TaskRunnerWorkItem> getKnownTasks()
  {
    return ImmutableList.copyOf(runningItems);
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public void start()
  {
    // No state startup required
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    throw new UnsupportedOperationException();
  }

  private static class TestTaskRunnerWorkItem extends TaskRunnerWorkItem
      implements Comparable<TestTaskRunnerWorkItem>
  {
    private final Task task;

    private TestTaskRunnerWorkItem(Task task, ListenableFuture<TaskStatus> result)
    {
      super(task.getId(), result);
      this.task = task;
    }

    public Task getTask()
    {
      return task;
    }

    @Override
    public TaskLocation getLocation()
    {
      return TaskLocation.create("testHost", 10000, 10000);
    }

    @Override
    public String getTaskType()
    {
      return task.getType();
    }

    @Override
    public String getDataSource()
    {
      return task.getDataSource();
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }

      if (o instanceof TestTaskRunnerWorkItem) {
        final TestTaskRunnerWorkItem that = (TestTaskRunnerWorkItem) o;
        return task.getId().equals(that.task.getId());
      }

      return false;
    }

    @Override
    public int hashCode()
    {
      return task.getId().hashCode();
    }

    @Override
    public int compareTo(TestTaskRunnerWorkItem o)
    {
      return task.getId().compareTo(o.task.getId());
    }
  }

  private class TestTaskRunnerCallable implements Callable<TaskStatus>
  {
    private final Task task;
    private final TaskToolbox toolbox;

    public TestTaskRunnerCallable(Task task, TaskToolbox toolbox)
    {
      this.task = task;
      this.toolbox = toolbox;
    }

    @Override
    public TaskStatus call()
    {
      final long startTime = System.currentTimeMillis();

      TaskStatus status;

      try {
        log.info("Running task: %s", task.getId());
        TaskRunnerUtils.notifyLocationChanged(
            listeners,
            task.getId(),
            taskLocation
        );
        TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), TaskStatus.running(task.getId()));
        status = task.run(toolbox);
      }
      catch (InterruptedException e) {
        // Don't reset the interrupt flag of the thread, as we do want to continue to the end of this callable.
        if (stopping) {
          // Tasks may interrupt their own run threads to stop themselves gracefully; don't be too scary about this.
          log.debug(e, "Interrupted while running task[%s] during graceful shutdown.", task);
        } else {
          // Not stopping, this is definitely unexpected.
          log.warn(e, "Interrupted while running task[%s]", task);
        }

        status = TaskStatus.failure(task.getId());
      }
      catch (Exception e) {
        log.error(e, "Exception while running task[%s]", task);
        status = TaskStatus.failure(task.getId());
      }
      catch (Throwable t) {
        throw new RE(t, "Uncaught Throwable while running task[%s]", task);
      }

      status = status.withDuration(System.currentTimeMillis() - startTime);
      TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), status);
      return status;
    }
  }
}
