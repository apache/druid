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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.concurrent.Execs;
import io.druid.concurrent.TaskThreadPriority;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import io.druid.server.DruidNode;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * Runs tasks in a JVM thread using an ExecutorService.
 */
public class ThreadPoolTaskRunner implements TaskRunner, QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(ThreadPoolTaskRunner.class);

  private final TaskToolboxFactory toolboxFactory;
  private final TaskConfig taskConfig;
  private final ConcurrentMap<Integer, ListeningExecutorService> exec = new ConcurrentHashMap<>();
  private final Set<ThreadPoolTaskRunnerWorkItem> runningItems = new ConcurrentSkipListSet<>(
      ThreadPoolTaskRunnerWorkItem.COMPARATOR
  );
  private final CopyOnWriteArrayList<Pair<TaskRunnerListener, Executor>> listeners = new CopyOnWriteArrayList<>();
  private final ServiceEmitter emitter;
  private final TaskLocation location;

  private volatile boolean stopping = false;

  @Inject
  public ThreadPoolTaskRunner(
      TaskToolboxFactory toolboxFactory,
      TaskConfig taskConfig,
      ServiceEmitter emitter,
      @Self DruidNode node
  )
  {
    this.toolboxFactory = Preconditions.checkNotNull(toolboxFactory, "toolboxFactory");
    this.taskConfig = taskConfig;
    this.emitter = Preconditions.checkNotNull(emitter, "emitter");
    this.location = TaskLocation.create(node.getHost(), node.getPort());
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
    for (ThreadPoolTaskRunnerWorkItem item : runningItems) {
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
            "task-runner-%d-priority-" + priority,
            TaskThreadPriority.getThreadPriorityFromTaskPriority(priority)
        )
    );
  }

  @Override
  @LifecycleStop
  public void stop()
  {
    stopping = true;

    for (Map.Entry<Integer, ListeningExecutorService> entry : exec.entrySet()) {
      try {
        entry.getValue().shutdown();
      }
      catch (SecurityException ex) {
        log.wtf(ex, "I can't control my own threads!");
      }
    }

    for (ThreadPoolTaskRunnerWorkItem item : runningItems) {
      final Task task = item.getTask();
      final long start = System.currentTimeMillis();
      final boolean graceful;
      final long elapsed;
      boolean error = false;

      if (taskConfig.isRestoreTasksOnRestart() && task.canRestore()) {
        // Attempt graceful shutdown.
        graceful = true;
        log.info("Starting graceful shutdown of task[%s].", task.getId());

        try {
          task.stopGracefully();
          final TaskStatus taskStatus = item.getResult().get(
              new Interval(new DateTime(start), taskConfig.getGracefulShutdownTimeout()).toDurationMillis(),
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
        graceful = false;
        TaskRunnerUtils.notifyStatusChanged(listeners, task.getId(), TaskStatus.failure(task.getId()));
      }

      elapsed = System.currentTimeMillis() - start;

      final ServiceMetricEvent.Builder metricBuilder = ServiceMetricEvent
          .builder()
          .setDimension("task", task.getId())
          .setDimension("dataSource", task.getDataSource())
          .setDimension("graceful", String.valueOf(graceful))
          .setDimension("error", String.valueOf(error));

      emitter.emit(metricBuilder.build("task/interrupt/count", 1L));
      emitter.emit(metricBuilder.build("task/interrupt/elapsed", elapsed));
    }

    // Ok, now interrupt everything.
    for (Map.Entry<Integer, ListeningExecutorService> entry : exec.entrySet()) {
      try {
        entry.getValue().shutdownNow();
      }
      catch (SecurityException ex) {
        log.wtf(ex, "I can't control my own threads!");
      }
    }
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    final TaskToolbox toolbox = toolboxFactory.build(task);
    final Object taskPriorityObj = task.getContextValue(TaskThreadPriority.CONTEXT_KEY);
    int taskPriority = 0;
    if (taskPriorityObj != null) {
      if (taskPriorityObj instanceof Number) {
        taskPriority = ((Number) taskPriorityObj).intValue();
      } else if (taskPriorityObj instanceof String) {
        try {
          taskPriority = Integer.parseInt(taskPriorityObj.toString());
        }
        catch (NumberFormatException e) {
          log.error(e, "Error parsing task priority [%s] for task [%s]", taskPriorityObj, task.getId());
        }
      }
    }
    // Ensure an executor for that priority exists
    if (!exec.containsKey(taskPriority)) {
      final ListeningExecutorService executorService = buildExecutorService(taskPriority);
      if (exec.putIfAbsent(taskPriority, executorService) != null) {
        // favor prior service
        executorService.shutdownNow();
      }
    }
    final ListenableFuture<TaskStatus> statusFuture = exec.get(taskPriority)
                                                          .submit(new ThreadPoolTaskRunnerCallable(
                                                              task,
                                                              location,
                                                              toolbox
                                                          ));
    final ThreadPoolTaskRunnerWorkItem taskRunnerWorkItem = new ThreadPoolTaskRunnerWorkItem(
        task,
        location,
        statusFuture
    );
    runningItems.add(taskRunnerWorkItem);
    Futures.addCallback(
        statusFuture, new FutureCallback<TaskStatus>()
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
  public void shutdown(final String taskid)
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
    return ImmutableList.<TaskRunnerWorkItem>copyOf(runningItems);
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    return ImmutableList.of();
  }

  @Override
  public Collection<TaskRunnerWorkItem> getKnownTasks()
  {
    return ImmutableList.<TaskRunnerWorkItem>copyOf(runningItems);
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
    final String queryDataSource = Iterables.getOnlyElement(query.getDataSource().getNames());

    for (final ThreadPoolTaskRunnerWorkItem taskRunnerWorkItem : ImmutableList.copyOf(runningItems)) {
      final Task task = taskRunnerWorkItem.getTask();
      if (task.getDataSource().equals(queryDataSource)) {
        final QueryRunner<T> taskQueryRunner = task.getQueryRunner(query);

        if (taskQueryRunner != null) {
          if (queryRunner == null) {
            queryRunner = taskQueryRunner;
          } else {
            log.makeAlert("Found too many query runners for datasource")
               .addData("dataSource", queryDataSource)
               .emit();
          }
        }
      }
    }

    return queryRunner == null ? new NoopQueryRunner<T>() : queryRunner;
  }

  private static class ThreadPoolTaskRunnerWorkItem extends TaskRunnerWorkItem
  {
    private static final Comparator<ThreadPoolTaskRunnerWorkItem> COMPARATOR = new Comparator<ThreadPoolTaskRunnerWorkItem>()
    {
      @Override
      public int compare(
          ThreadPoolTaskRunnerWorkItem lhs,
          ThreadPoolTaskRunnerWorkItem rhs
      )
      {
        return lhs.getTaskId().compareTo(rhs.getTaskId());
      }
    };

    private final Task task;
    private final TaskLocation location;

    private ThreadPoolTaskRunnerWorkItem(
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
  }

  private class ThreadPoolTaskRunnerCallable implements Callable<TaskStatus>
  {
    private final Task task;
    private final TaskLocation location;
    private final TaskToolbox toolbox;

    public ThreadPoolTaskRunnerCallable(Task task, TaskLocation location, TaskToolbox toolbox)
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

        status = TaskStatus.failure(task.getId());
      }
      catch (Exception e) {
        log.error(e, "Exception while running task[%s]", task);
        status = TaskStatus.failure(task.getId());
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
