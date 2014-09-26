/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.repackaged.com.google.common.base.Preconditions;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskRunner;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;

/**
 * Encapsulates the lifecycle of a task executor. Loads one task, runs it, writes its status, and all the while
 * monitors its parent process.
 */
public class ExecutorLifecycle
{
  private static final EmittingLogger log = new EmittingLogger(ExecutorLifecycle.class);

  private final ExecutorLifecycleConfig config;
  private final TaskActionClientFactory taskActionClientFactory;
  private final TaskRunner taskRunner;
  private final ObjectMapper jsonMapper;

  private final ExecutorService parentMonitorExec = Execs.singleThreaded("parent-monitor-%d");

  private volatile ListenableFuture<TaskStatus> statusFuture = null;

  @Inject
  public ExecutorLifecycle(
      ExecutorLifecycleConfig config,
      TaskActionClientFactory taskActionClientFactory,
      TaskRunner taskRunner,
      ObjectMapper jsonMapper
  )
  {
    this.config = config;
    this.taskActionClientFactory = taskActionClientFactory;
    this.taskRunner = taskRunner;
    this.jsonMapper = jsonMapper;
  }

  @LifecycleStart
  public void start()
  {
    final File taskFile = Preconditions.checkNotNull(config.getTaskFile(), "taskFile");
    final File statusFile = Preconditions.checkNotNull(config.getStatusFile(), "statusFile");
    final InputStream parentStream = Preconditions.checkNotNull(config.getParentStream(), "parentStream");

    final Task task;

    try {
      task = jsonMapper.readValue(taskFile, Task.class);

      log.info(
          "Running with task: %s",
          jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(task)
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    // Spawn monitor thread to keep a watch on parent's stdin
    // If stdin reaches eof, the parent is gone, and we should shut down
    parentMonitorExec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              while (parentStream.read() != -1) {
                // Toss the byte
              }
            }
            catch (Exception e) {
              log.error(e, "Failed to read from stdin");
            }

            // Kind of gross, but best way to kill the JVM as far as I know
            log.info("Triggering JVM shutdown.");
            System.exit(2);
          }
        }
    );

    // Won't hurt in remote mode, and is required for setting up locks in local mode:
    try {
      if (!task.isReady(taskActionClientFactory.create(task))) {
        throw new ISE("Task is not ready to run yet!", task.getId());
      }
    } catch (Exception e) {
      throw new ISE(e, "Failed to run isReady", task.getId());
    }

    statusFuture = Futures.transform(
        taskRunner.run(task),
        new Function<TaskStatus, TaskStatus>()
        {
          @Override
          public TaskStatus apply(TaskStatus taskStatus)
          {
            try {
              log.info(
                  "Task completed with status: %s",
                  jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(taskStatus)
              );

              final File statusFileParent = statusFile.getParentFile();
              if (statusFileParent != null) {
                statusFileParent.mkdirs();
              }
              jsonMapper.writeValue(statusFile, taskStatus);

              return taskStatus;
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
  }

  public void join()
  {
    try {
      statusFuture.get();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @LifecycleStop
  public void stop()
  {
    parentMonitorExec.shutdown();
  }
}
