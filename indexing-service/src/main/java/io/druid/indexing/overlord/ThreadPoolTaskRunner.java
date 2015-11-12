/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.overlord;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.task.Task;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.SegmentDescriptor;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;

import java.io.File;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * Runs tasks in a JVM thread using an ExecutorService.
 */
public class ThreadPoolTaskRunner implements TaskRunner, QuerySegmentWalker
{
  private final TaskToolboxFactory toolboxFactory;
  private final ListeningExecutorService exec;
  private final Set<ThreadPoolTaskRunnerWorkItem> runningItems = new ConcurrentSkipListSet<>();
  private final QueryRunnerFactoryConglomerate conglomerate;
  private static final EmittingLogger log = new EmittingLogger(ThreadPoolTaskRunner.class);

  @Inject
  public ThreadPoolTaskRunner(
      TaskToolboxFactory toolboxFactory,
      QueryRunnerFactoryConglomerate conglomerate
  )
  {
    this.toolboxFactory = Preconditions.checkNotNull(toolboxFactory, "toolboxFactory");
    this.exec = MoreExecutors.listeningDecorator(Execs.singleThreaded("task-runner-%d"));
    this.conglomerate = conglomerate;
  }

  @LifecycleStop
  public void stop()
  {
    exec.shutdownNow();
  }

  @Override
  public ListenableFuture<TaskStatus> run(final Task task)
  {
    final TaskToolbox toolbox = toolboxFactory.build(task);
    final ListenableFuture<TaskStatus> statusFuture = exec.submit(new ThreadPoolTaskRunnerCallable(task, toolbox));
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
  public Collection<ZkWorker> getWorkers()
  {
    return Lists.newArrayList();
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
      final File taskDir = toolbox.getTaskWorkDir();

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
        if (taskDir.exists()) {
          log.info("Removing task directory: %s", taskDir);
          FileUtils.deleteDirectory(taskDir);
        }
      }
      catch (Exception e) {
        log.makeAlert(e, "Failed to delete task directory")
           .addData("taskDir", taskDir.toString())
           .addData("task", task.getId())
           .emit();
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
