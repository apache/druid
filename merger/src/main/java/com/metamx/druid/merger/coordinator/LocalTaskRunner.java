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

import java.io.File;
import java.util.concurrent.ExecutorService;

import org.apache.commons.io.FileUtils;

import com.google.common.base.Throwables;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.task.Task;

/**
 * Runs tasks in a JVM thread using an ExecutorService.
 */
public class LocalTaskRunner implements TaskRunner
{
  private final TaskToolbox toolbox;
  private final ExecutorService exec;

  private static final Logger log = new Logger(TaskQueue.class);

  public LocalTaskRunner(
      TaskToolbox toolbox,
      ExecutorService exec
  )
  {
    this.toolbox = toolbox;
    this.exec = exec;
  }

  @LifecycleStop
  public void stop()
  {
    exec.shutdownNow();
  }

  @Override
  public void run(final Task task, final TaskContext context, final TaskCallback callback)
  {
    exec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            final long startTime = System.currentTimeMillis();
            final File taskDir = toolbox.getConfig().getTaskDir(task);

            TaskStatus status;

            try {
              status = task.run(context, toolbox);
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
              log.error(e, "Failed to delete task directory[%s]", taskDir.toString());
            }

            try {
              callback.notify(status.withDuration(System.currentTimeMillis() - startTime));
            } catch(Throwable t) {
              log.error(t, "Uncaught Throwable during callback for task[%s]", task);
              throw Throwables.propagate(t);
            }
          }
        }
    );
  }
}
