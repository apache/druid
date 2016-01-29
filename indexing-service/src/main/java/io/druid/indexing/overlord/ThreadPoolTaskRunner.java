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
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.Pair;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.concurrent.Execs;
import io.druid.concurrent.TaskThreadPriority;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
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
  private final Set<ThreadPoolTaskRunnerWorkItem> runningItems = new ConcurrentSkipListSet<>();
  private final ServiceEmitter emitter;

  @Inject
  public ThreadPoolTaskRunner(
      TaskToolboxFactory toolboxFactory,
      TaskConfig taskConfig,
      ServiceEmitter emitter
  )
  {
    this.toolboxFactory = Preconditions.checkNotNull(toolboxFactory, "toolboxFactory");
    this.taskConfig = taskConfig;
    this.emitter = Preconditions.checkNotNull(emitter, "emitter");
  }

  @Override
  public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
  {
    return ImmutableList.of();
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

  @LifecycleStop
  public void stop()
  {
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
          log.info(
              "Graceful shutdown of task[%s] finished in %,dms with status[%s].",
              task.getId(),
              System.currentTimeMillis() - start,
              taskStatus.getStatusCode()
          );
        }
        catch (Exception e) {
          log.makeAlert(e, "Graceful task shutdown failed: %s", task.getDataSource())
             .addData("taskId", task.getId())
             .addData("dataSource", task.getDataSource())
             .emit();
          log.warn(e, "Graceful shutdown of task[%s] aborted with exception.", task.getId());
          error = true;
        }
      } else {
        graceful = false;
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
    if(taskPriorityObj != null){
      if(taskPriorityObj instanceof Number) {
        taskPriority = ((Number) taskPriorityObj).intValue();
      } else if(taskPriorityObj instanceof String) {
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
                                                          .submit(new ThreadPoolTaskRunnerCallable(task, toolbox));
    final ThreadPoolTaskRunnerWorkItem taskRunnerWorkItem = new ThreadPoolTaskRunnerWorkItem(task, statusFuture);
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
    private final Task task;

    private ThreadPoolTaskRunnerWorkItem(
        Task task,
        ListenableFuture<TaskStatus> result
    )
    {
      super(task.getId(), result);
      this.task = task;
    }

    public Task getTask()
    {
      return task;
    }
  }

  private static class ThreadPoolTaskRunnerCallable implements Callable<TaskStatus>
  {
    private final Task task;
    private final TaskToolbox toolbox;

    public ThreadPoolTaskRunnerCallable(Task task, TaskToolbox toolbox)
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
        status = task.run(toolbox);
      }
      catch (InterruptedException e) {
        log.error(e, "Interrupted while running task[%s]", task);
        throw Throwables.propagate(e);
      }
      catch (Exception e) {
        log.error(e, "Exception while running task[%s]", task);
        status = TaskStatus.failure(task.getId());
      }
      catch (Throwable t) {
        log.error(t, "Uncaught Throwable while running task[%s]", task);
        throw Throwables.propagate(t);
      }

      try {
        return status.withDuration(System.currentTimeMillis() - startTime);
      }
      catch (Exception e) {
        log.error(e, "Uncaught Exception during callback for task[%s]", task);
        throw Throwables.propagate(e);
      }
    }
  }
}
