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
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.RetryPolicy;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.TaskToolboxFactory;
import com.metamx.druid.merger.common.task.Task;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.mortbay.thread.ThreadPool;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * Runs tasks in a JVM thread using an ExecutorService.
 */
public class LocalTaskRunner implements TaskRunner
{
  private final TaskToolboxFactory toolboxFactory;
  private final ExecutorService exec;

  private final Set<TaskRunnerWorkItem> runningItems = new ConcurrentSkipListSet<TaskRunnerWorkItem>();

  private static final Logger log = new Logger(LocalTaskRunner.class);

  public LocalTaskRunner(
      TaskToolboxFactory toolboxFactory,
      ExecutorService exec
  )
  {
    this.toolboxFactory = toolboxFactory;
    this.exec = exec;
  }

  @LifecycleStop
  public void stop()
  {
    exec.shutdownNow();
  }

  @Override
  public void run(final Task task, final TaskCallback callback)
  {
    final TaskToolbox toolbox = toolboxFactory.build(task);

    exec.submit(new LocalTaskRunnerRunnable(task, toolbox, callback));
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
                                    if (input instanceof LocalTaskRunnerRunnable) {
                                      return ((LocalTaskRunnerRunnable) input).getTaskRunnerWorkItem();
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

  private static class LocalTaskRunnerRunnable implements Runnable
  {
    private final Task task;
    private final TaskToolbox toolbox;
    private final TaskCallback callback;

    private final DateTime createdTime;

    public LocalTaskRunnerRunnable(Task task, TaskToolbox toolbox, TaskCallback callback)
    {
      this.task = task;
      this.toolbox = toolbox;
      this.callback = callback;

      this.createdTime = new DateTime();
    }

    @Override
    public void run()
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
        callback.notify(status.withDuration(System.currentTimeMillis() - startTime));
      } catch(Exception e) {
        log.error(e, "Uncaught Exception during callback for task[%s]", task);
        throw Throwables.propagate(e);
      }
    }

    public TaskRunnerWorkItem getTaskRunnerWorkItem()
    {
      return new TaskRunnerWorkItem(
          task,
          callback,
          null,
          createdTime
      );
    }
  }

  @Override
  public void shutdown(String taskId)
  {
  }
}
