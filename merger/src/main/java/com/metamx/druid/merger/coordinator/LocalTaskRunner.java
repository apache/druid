/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.merger.coordinator;

import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.TaskToolboxFactory;
import com.metamx.druid.merger.common.task.Task;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;

import java.io.File;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Runs tasks in a JVM thread using an ExecutorService.
 */
public class LocalTaskRunner implements TaskRunner
{
  private final TaskToolboxFactory toolboxFactory;
  private final ListeningExecutorService exec;

  private final Set<TaskRunnerWorkItem> runningItems = new ConcurrentSkipListSet<TaskRunnerWorkItem>();

  private static final Logger log = new Logger(LocalTaskRunner.class);

  public LocalTaskRunner(
      TaskToolboxFactory toolboxFactory,
      ExecutorService exec
  )
  {
    this.toolboxFactory = toolboxFactory;
    this.exec = MoreExecutors.listeningDecorator(exec);
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
    return exec.submit(new LocalTaskRunnerCallable(task, toolbox));
  }

  @Override
  public Collection<TaskRunnerWorkItem> getRunningTasks()
  {
    return runningItems;
  }

  @Override
  public Collection<TaskRunnerWorkItem> getPendingTasks()
  {
    if (exec instanceof ThreadPoolExecutor) {
      ThreadPoolExecutor tpe = (ThreadPoolExecutor) exec;

      return Lists.newArrayList(
          FunctionalIterable.create(tpe.getQueue())
                            .keep(
                                new Function<Runnable, TaskRunnerWorkItem>()
                                {
                                  @Override
                                  public TaskRunnerWorkItem apply(Runnable input)
                                  {
                                    if (input instanceof LocalTaskRunnerCallable) {
                                      return ((LocalTaskRunnerCallable) input).getTaskRunnerWorkItem();
                                    }
                                    return null;
                                  }
                                }
                            )
      );
    }

    return Lists.newArrayList();
  }

  @Override
  public Collection<ZkWorker> getWorkers()
  {
    return Lists.newArrayList();
  }

  private static class LocalTaskRunnerCallable implements Callable<TaskStatus>
  {
    private final Task task;
    private final TaskToolbox toolbox;

    private final DateTime createdTime;

    public LocalTaskRunnerCallable(Task task, TaskToolbox toolbox)
    {
      this.task = task;
      this.toolbox = toolbox;

      this.createdTime = new DateTime();
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
        final File taskDir = toolbox.getTaskDir();

        if (taskDir.exists()) {
          log.info("Removing task directory: %s", taskDir);
          FileUtils.deleteDirectory(taskDir);
        }
      }
      catch (Exception e) {
        log.error(e, "Failed to delete task directory: %s", task.getId());
      }

      try {
        return status.withDuration(System.currentTimeMillis() - startTime);
      } catch(Exception e) {
        log.error(e, "Uncaught Exception during callback for task[%s]", task);
        throw Throwables.propagate(e);
      }
    }

    public TaskRunnerWorkItem getTaskRunnerWorkItem()
    {
      return new TaskRunnerWorkItem(
          task,
          null,
          null,
          createdTime
      );
    }
  }
}
