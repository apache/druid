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
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.concurrent.TaskThreadPriority;
import org.apache.druid.guice.annotations.Self;
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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.NoopQueryRunner;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.SetAndVerifyContextQueryRunner;
import org.apache.druid.server.initialization.ServerConfig;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Runs a single task in a JVM thread using an ExecutorService.
 */
public class SingleTaskBackgroundRunner implements TaskRunner, QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(SingleTaskBackgroundRunner.class);

  private final TaskToolboxFactory toolboxFactory;
  private final TaskConfig taskConfig;
  private final ServiceEmitter emitter;
  private final TaskLocation location;
  private final ServerConfig serverConfig;

  // Currently any listeners are registered in peons, but they might be used in the future.
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();

  private volatile ListeningExecutorService executorService;
  private volatile SingleTaskBackgroundRunnerWorkItem runningItem;
  private volatile boolean stopping;

  @Inject
  public SingleTaskBackgroundRunner(
      TaskToolboxFactory toolboxFactory,
      TaskConfig taskConfig,
      ServiceEmitter emitter,
      @Self DruidNode node,
      ServerConfig serverConfig
  )
  {
    this.toolboxFactory = Preconditions.checkNotNull(toolboxFactory, "toolboxFactory");
    this.taskConfig = taskConfig;
    this.emitter = Preconditions.checkNotNull(emitter, "emitter");
    this.location = TaskLocation.create(node.getHost(), node.getPlaintextPort(), node.getTlsPort());
    this.serverConfig = serverConfig;
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return Collections.emptyList();
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
    if (runningItem != null) {
      TaskRunnerUtils.notifyLocationChanged(
          ImmutableList.of(listenerPair),
          runningItem.getTaskId(),
          runningItem.getLocation()
      );
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
            "task-runner-%d-priority-" + priority,
            TaskThreadPriority.getThreadPriorityFromTaskPriority(priority)
        )
    );
  }

  @Override
  @LifecycleStart
  public void start()
  {
    // No state startup required
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    stopping = true;

    if (executorService != null) {
      try {
        executorService.shutdown();
      }
      catch (SecurityException ex) {
        log.wtf(ex, "I can't control my own threads!");
      }
    }

    if (runningItem != null) {
      final Task task = runningItem.getTask();
      final long start = System.currentTimeMillis();
      final long elapsed;
      boolean error = false;

      // stopGracefully for resource cleaning
      log.info("Starting graceful shutdown of task[%s].", task.getId());
      task.stopGracefully(taskConfig);

      if (taskConfig.isRestoreTasksOnRestart() && task.canRestore()) {
        try {
          final TaskStatus taskStatus = runningItem.getResult().get(
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
          log.makeAlert(e, "Graceful task shutdown failed: %s", task.getDataSource())
             .addData("taskId", task.getId())
             .addData("dataSource", task.getDataSource())
             .emit();
          log.warn(e, "Graceful shutdown of task[%s] aborted with exception.", task.getId());
          error = true;
          TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), TaskStatus.failure(task.getId()));
        }
      } else {
        TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), TaskStatus.failure(task.getId()));
      }

      elapsed = System.currentTimeMillis() - start;

      final ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent
          .builder()
          .setDimension("task", task.getId())
          .setDimension("dataSource", task.getDataSource())
          .setDimension("graceful", "true") // for backward compatibility
          .setDimension("error", String.valueOf(error));

      emitter.emit(metricBuilder.build("task/interrupt/count", 1L));
      emitter.emit(metricBuilder.build("task/interrupt/elapsed", elapsed));
    }

    // Ok, now interrupt everything.
    if (executorService != null) {
      try {
        executorService.shutdownNow();
      }
      catch (SecurityException ex) {
        log.wtf(ex, "I can't control my own threads!");
      }
    }
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    if (runningItem == null) {
      final TaskToolbox toolbox = toolboxFactory.build(task);
      final Object taskPriorityObj = task.getContextValue(TaskThreadPriority.CONTEXT_KEY);
      int taskPriority = 0;
      try {
        taskPriority = taskPriorityObj == null ? 0 : Numbers.parseInt(taskPriorityObj);
      }
      catch (NumberFormatException e) {
        log.error(e, "Error parsing task priority [%s] for task [%s]", taskPriorityObj, task.getId());
      }
      // Ensure an executor for that priority exists
      executorService = buildExecutorService(taskPriority);
      final ListenableFuture<TaskStatus> statusFuture = executorService.submit(
          new SingleTaskBackgroundRunnerCallable(task, location, toolbox)
      );
      runningItem = new SingleTaskBackgroundRunnerWorkItem(
          task,
          location,
          statusFuture
      );

      return statusFuture;
    } else {
      throw new ISE("Already running task[%s]", runningItem.getTask().getId());
    }
  }

  /**
   * There might be a race between {@link #run(Task)} and this method, but it shouldn't happen in real applications
   * because this method is called only in unit tests. See TaskLifecycleTest.
   *
   * @param taskid task ID to clean up resources for
   */
  @Override
  public void shutdown(final String taskid, String reason)
  {
    log.info("Shutdown [%s] because: [%s]", taskid, reason);
    if (runningItem != null && runningItem.getTask().getId().equals(taskid)) {
      runningItem.getResult().cancel(true);
    }
  }

  @Override
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    return runningItem == null ? Collections.emptyList() : Collections.singletonList(runningItem);
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    return Collections.emptyList();
  }

  @Override
  public Collection<TaskRunnerWorkItem> getKnownTasks()
  {
    return runningItem == null ? Collections.emptyList() : Collections.singletonList(runningItem);
  }

  @Override
  public TaskLocation getTaskLocation(String taskId)
  {
    return location;
  }

  @Override
  public Optional<ScalingStats> getScalingStats()
  {
    return Optional.absent();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return getQueryRunnerImpl(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return getQueryRunnerImpl(query);
  }

  private <T> QueryRunner<T> getQueryRunnerImpl(Query<T> query)
  {
    QueryRunner<T> queryRunner = null;

    if (runningItem != null) {
      final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());
      final Task task = runningItem.getTask();

      if (analysis.getBaseTableDataSource().isPresent()
          && task.getDataSource().equals(analysis.getBaseTableDataSource().get().getName())) {
        final QueryRunner<T> taskQueryRunner = task.getQueryRunner(query);

        if (taskQueryRunner != null) {
          queryRunner = taskQueryRunner;
        }
      }
    }

    return new SetAndVerifyContextQueryRunner<>(
        serverConfig,
        queryRunner == null ? new NoopQueryRunner<>() : queryRunner
    );
  }

  private static class SingleTaskBackgroundRunnerWorkItem extends TaskRunnerWorkItem
  {
    private final Task task;
    private final TaskLocation location;

    private SingleTaskBackgroundRunnerWorkItem(
        Task task,
        TaskLocation location,
        ListenableFuture<TaskStatus> result
    )
    {
      super(task.getId(), result);
      this.task = task;
      this.location = location;
    }

    public Task getTask()
    {
      return task;
    }

    @Override
    public TaskLocation getLocation()
    {
      return location;
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

  }

  private class SingleTaskBackgroundRunnerCallable implements Callable<TaskStatus>
  {
    private final Task task;
    private final TaskLocation location;
    private final TaskToolbox toolbox;

    SingleTaskBackgroundRunnerCallable(Task task, TaskLocation location, TaskToolbox toolbox)
    {
      this.task = task;
      this.location = location;
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
            location
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

        status = TaskStatus.failure(task.getId(), e.toString());
      }
      catch (Exception e) {
        log.error(e, "Exception while running task[%s]", task);
        status = TaskStatus.failure(task.getId(), e.toString());
      }
      catch (Throwable t) {
        log.error(t, "Uncaught Throwable while running task[%s]", task);
        throw t;
      }

      status = status.withDuration(System.currentTimeMillis() - startTime);
      TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), status);
      return status;
    }
  }
}
