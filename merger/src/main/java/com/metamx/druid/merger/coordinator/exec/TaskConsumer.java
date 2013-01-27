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

package com.metamx.druid.merger.coordinator.exec;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.MergerDBCoordinator;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.druid.merger.coordinator.TaskRunner;
import com.metamx.druid.merger.coordinator.VersionedTaskWrapper;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.Set;

public class TaskConsumer implements Runnable
{
  private final TaskQueue queue;
  private final TaskRunner runner;
  private final MergerDBCoordinator mergerDBCoordinator;
  private final ServiceEmitter emitter;
  private final Thread thready;

  private volatile boolean shutdown = false;

  private static final EmittingLogger log = new EmittingLogger(TaskConsumer.class);

  public TaskConsumer(
      TaskQueue queue,
      TaskRunner runner,
      MergerDBCoordinator mergerDBCoordinator,
      ServiceEmitter emitter
  )
  {
    this.queue = queue;
    this.runner = runner;
    this.mergerDBCoordinator = mergerDBCoordinator;
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
        final String version;

        try {
          final VersionedTaskWrapper taskWrapper = queue.take();
          task = taskWrapper.getTask();
          version = taskWrapper.getVersion();
        }
        catch (InterruptedException e) {
          log.info(e, "Interrupted while waiting for new work");
          throw e;
        }

        try {
          handoff(task, version);
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to hand off task")
             .addData("task", task.getId())
             .addData("type", task.getType().toString())
             .addData("dataSource", task.getDataSource())
             .addData("interval", task.getInterval())
             .emit();

          // Retry would be nice, but only after we have a way to throttle and limit them.  Just fail for now.
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

  private void handoff(final Task task, final String version) throws Exception
  {
    final TaskContext context = new TaskContext(
        version,
        ImmutableSet.copyOf(
            mergerDBCoordinator.getUsedSegmentsForInterval(
                task.getDataSource(),
                task.getInterval()
            )
        ),
        ImmutableSet.copyOf(
            mergerDBCoordinator.getUnusedSegmentsForInterval(
                task.getDataSource(),
                task.getInterval()
            )
        )
    );
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
        .setUser2(task.getDataSource())
        .setUser4(task.getType().toString())
        .setUser5(task.getInterval().toString());

    // Run preflight checks
    TaskStatus preflightStatus;
    try {
      preflightStatus = task.preflight(context);
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

    // Hand off work to TaskRunner
    // TODO -- Should something in the TaskCallback enforce that each returned status is logically after the previous?
    // TODO -- Probably yes. But make sure it works in the face of RTR retries.
    runner.run(
        task, context, new TaskCallback()
    {
      @Override
      public void notify(final TaskStatus statusFromRunner)
      {
        try {
          log.info("Received %s status for task: %s", statusFromRunner.getStatusCode(), task);

          // If we're not supposed to be running anymore, don't do anything. Somewhat racey if the flag gets set after
          // we check and before we commit the database transaction, but better than nothing.
          if (shutdown) {
            log.info("Abandoning task due to shutdown: %s", task.getId());
            return;
          }

          queue.notify(
              task, statusFromRunner, new Runnable()
          {
            @Override
            public void run()
            {
              try {
                if (statusFromRunner.getSegments().size() > 0) {
                  // TODO -- Publish in transaction
                  publishSegments(task, context, statusFromRunner.getSegments());
                }

                if (statusFromRunner.getSegmentsNuked().size() > 0) {
                  deleteSegments(task, context, statusFromRunner.getSegmentsNuked());
                }
              }
              catch (Exception e) {
                log.error(e, "Exception while publishing segments for task: %s", task);
                throw Throwables.propagate(e);
              }
            }
          }
          );

          // Emit event and log, if the task is done
          if (statusFromRunner.isComplete()) {
            int segmentBytes = 0;
            for (DataSegment segment : statusFromRunner.getSegments()) {
              segmentBytes += segment.getSize();
            }

            builder.setUser3(statusFromRunner.getStatusCode().toString());

            emitter.emit(builder.build("indexer/time/run/millis", statusFromRunner.getDuration()));
            emitter.emit(builder.build("indexer/segment/count", statusFromRunner.getSegments().size()));
            emitter.emit(builder.build("indexer/segment/bytes", segmentBytes));

            if (statusFromRunner.isFailure()) {
              log.makeAlert("Failed to index")
                 .addData("task", task.getId())
                 .addData("type", task.getType().toString())
                 .addData("dataSource", task.getDataSource())
                 .addData("interval", task.getInterval())
                 .emit();
            }

            log.info(
                "Task %s: %s (%d segments) (%d run duration)",
                statusFromRunner.getStatusCode(),
                task,
                statusFromRunner.getSegments().size(),
                statusFromRunner.getDuration()
            );
          }
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to handle task callback")
             .addData("task", task.getId())
             .addData("statusCode", statusFromRunner.getStatusCode())
             .emit();
        }
      }
    }
    );
  }

  private void deleteSegments(Task task, TaskContext context, Set<DataSegment> segments) throws Exception
  {
    for (DataSegment segment : segments) {
      verifyDataSourceAndInterval(task, context, segment);

      // Verify version (must be less than our context version)
      if (segment.getVersion().compareTo(context.getVersion()) >= 0) {
        throw new IllegalStateException(
            String.format(
                "Segment-to-nuke for task[%s] has invalid version: %s",
                task.getId(),
                segment.getIdentifier()
            )
        );
      }

      log.info("Deleting segment[%s] for task[%s]", segment.getIdentifier(), task.getId());
      mergerDBCoordinator.deleteSegment(segment);
    }
  }

  private void publishSegments(Task task, TaskContext context, Set<DataSegment> segments) throws Exception
  {
    for (DataSegment segment : segments) {
      verifyDataSourceAndInterval(task, context, segment);

      // Verify version (must be equal to our context version)
      if (!context.getVersion().equals(segment.getVersion())) {
        throw new IllegalStateException(
            String.format(
                "Segment for task[%s] has invalid version: %s",
                task.getId(),
                segment.getIdentifier()
            )
        );
      }

      log.info("Publishing segment[%s] for task[%s]", segment.getIdentifier(), task.getId());
      mergerDBCoordinator.announceHistoricalSegment(segment);
    }
  }

  private void verifyDataSourceAndInterval(Task task, TaskContext context, DataSegment segment)
  {
    if (!task.getDataSource().equals(segment.getDataSource())) {
      throw new IllegalStateException(
          String.format(
              "Segment for task[%s] has invalid dataSource: %s",
              task.getId(),
              segment.getIdentifier()
          )
      );
    }

    if (!task.getInterval().contains(segment.getInterval())) {
      throw new IllegalStateException(
          String.format(
              "Segment for task[%s] has invalid interval: %s",
              task.getId(),
              segment.getIdentifier()
          )
      );
    }
  }
}
