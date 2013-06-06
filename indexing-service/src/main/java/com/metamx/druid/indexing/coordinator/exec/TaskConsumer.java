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

package com.metamx.druid.indexing.coordinator.exec;

import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.actions.TaskActionClientFactory;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.coordinator.TaskQueue;
import com.metamx.druid.indexing.coordinator.TaskRunner;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

public class TaskConsumer implements Runnable
{
  private final TaskQueue queue;
  private final TaskRunner runner;
  private final TaskActionClientFactory taskActionClientFactory;
  private final ServiceEmitter emitter;
  private final Thread thready;

  private volatile boolean shutdown = false;

  private static final EmittingLogger log = new EmittingLogger(TaskConsumer.class);

  public TaskConsumer(
      TaskQueue queue,
      TaskRunner runner,
      TaskActionClientFactory taskActionClientFactory,
      ServiceEmitter emitter
  )
  {
    this.queue = queue;
    this.runner = runner;
    this.taskActionClientFactory = taskActionClientFactory;
    this.emitter = emitter;
    this.thready = new Thread(this);
  }

  @LifecycleStart
  public void start()
  {
    thready.start();
  }

  @LifecycleStop
  public void stop()
  {
    shutdown = true;
    thready.interrupt();
  }

  @Override
  public void run()
  {

    try {
      while (!Thread.currentThread().isInterrupted()) {

        final Task task;

        try {
          task = queue.take();
        }
        catch (InterruptedException e) {
          log.info("Interrupted while waiting for new work");
          Thread.currentThread().interrupt();
          break;
        }

        try {
          handoff(task);
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to hand off task")
             .addData("task", task.getId())
             .addData("type", task.getType())
             .addData("dataSource", task.getDataSource())
             .addData("interval", task.getImplicitLockInterval())
             .emit();

          // Retry would be nice, but only after we have a way to throttle and limit them. Just fail for now.
          if (!shutdown) {
            queue.notify(task, TaskStatus.failure(task.getId()));
          }
        }
      }
    }
    catch (Exception e) {
      // exit thread
      log.error(e, "Uncaught exception while consuming tasks");
      throw Throwables.propagate(e);
    }
  }

  private void handoff(final Task task) throws Exception
  {
    final ServiceMetricEvent.Builder metricBuilder = new ServiceMetricEvent.Builder()
        .setUser2(task.getDataSource())
        .setUser4(task.getType())
        .setUser5(task.getImplicitLockInterval().toString());

    // Run preflight checks
    TaskStatus preflightStatus;
    try {
      preflightStatus = task.preflight(taskActionClientFactory.create(task));
      log.info("Preflight done for task: %s", task.getId());
    }
    catch (Exception e) {
      preflightStatus = TaskStatus.failure(task.getId());
      log.error(e, "Exception thrown during preflight for task: %s", task.getId());
    }

    if (!preflightStatus.isRunnable()) {
      log.info("Task finished during preflight: %s", task.getId());
      queue.notify(task, preflightStatus);
      return;
    }

    // Hand off work to TaskRunner, with a callback
    final ListenableFuture<TaskStatus> status = runner.run(task);

    Futures.addCallback(
        status, new FutureCallback<TaskStatus>()
    {
      @Override
      public void onSuccess(final TaskStatus status)
      {
        log.info("Received %s status for task: %s", status.getStatusCode(), task);
        handleStatus(status);
      }

      @Override
      public void onFailure(Throwable t)
      {
        log.makeAlert(t, "Failed to run task")
           .addData("task", task.getId())
           .addData("type", task.getType())
           .addData("dataSource", task.getDataSource())
           .addData("interval", task.getImplicitLockInterval())
           .emit();

        handleStatus(TaskStatus.failure(task.getId()));
      }

      private void handleStatus(TaskStatus status)
      {
        try {
          // If we're not supposed to be running anymore, don't do anything. Somewhat racey if the flag gets set after
          // we check and before we commit the database transaction, but better than nothing.
          if (shutdown) {
            log.info("Abandoning task due to shutdown: %s", task.getId());
            return;
          }

          queue.notify(task, status);

          // Emit event and log, if the task is done
          if (status.isComplete()) {
            metricBuilder.setUser3(status.getStatusCode().toString());
            emitter.emit(metricBuilder.build("indexer/time/run/millis", status.getDuration()));

            log.info(
                "Task %s: %s (%d run duration)",
                status.getStatusCode(),
                task,
                status.getDuration()
            );
          }
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to handle task status")
             .addData("task", task.getId())
             .addData("statusCode", status.getStatusCode())
             .emit();
        }
      }
    }
    );
  }
}
