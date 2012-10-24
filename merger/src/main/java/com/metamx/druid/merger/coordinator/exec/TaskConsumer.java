package com.metamx.druid.merger.coordinator.exec;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.MergerDBCoordinator;
import com.metamx.druid.merger.coordinator.TaskCallback;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.merger.coordinator.TaskQueue;
import com.metamx.druid.merger.coordinator.TaskRunner;
import com.metamx.druid.merger.coordinator.VersionedTaskWrapper;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.AlertEvent;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.util.concurrent.ExecutorService;

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
          throw Throwables.propagate(e);
        }

        try {
          handoff(task, version);
        }
        catch (Exception e) {
          log.makeAlert(e, "Failed to hand off task")
             .addData("task", task.getId())
             .addData("type", task.getType().toString().toLowerCase())
             .addData("dataSource", task.getDataSource())
             .addData("interval", task.getInterval())
             .emit();

          // TODO - Retry would be nice, but only after we have a way to throttle and limit them
          queue.done(task, TaskStatus.failure(task.getId()));
        }
      }
    }
    catch (Throwable t) {
      // exit thread
      log.error(t, "Uncaught Throwable while consuming tasks");
      throw Throwables.propagate(t);
    }
  }

  private void handoff(final Task task, final String version) throws Exception
  {
    final TaskContext context = new TaskContext(
        version,
        ImmutableSet.copyOf(
            mergerDBCoordinator.getSegmentsForInterval(
                task.getDataSource(),
                task.getInterval()
            )
        )
    );
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder()
        .setUser2(task.getDataSource())
        .setUser4(task.getType().toString().toLowerCase())
        .setUser5(task.getInterval().toString());

    // Run preflight checks
    TaskStatus preflightStatus;
    try {
      preflightStatus = task.preflight(context);
      log.info("Preflight done for task: %s", task.getId());
    } catch(Exception e) {
      preflightStatus = TaskStatus.failure(task.getId());
      log.error(e, "Exception thrown during preflight for task: %s", task.getId());
    }

    if (!preflightStatus.isRunnable()) {
      log.info("Task finished during preflight: %s", task.getId());
      queue.done(task, preflightStatus);
      return;
    }

    // Hand off work to TaskRunner
    runner.run(
        task, context, new TaskCallback()
    {
      @Override
      public void notify(final TaskStatus statusFromRunner)
      {

        // task is done
        log.info("TaskRunner finished task: %s", task);

        // we might need to change this due to exceptions
        TaskStatus status = statusFromRunner;

        // If we're not supposed to be running anymore, don't do anything. Somewhat racey if the flag gets set after
        // we check and before we commit the database transaction, but better than nothing.
        if(shutdown) {
          log.info("Abandoning task due to shutdown: %s", task.getId());
          return;
        }

        // Publish returned segments
        // FIXME: Publish in transaction
        try {
          for (DataSegment segment : status.getSegments()) {
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
        catch (Exception e) {
          log.error(e, "Exception while publishing segments for task: %s", task);
          status = TaskStatus.failure(task.getId()).withDuration(status.getDuration());
        }

        try {
          queue.done(task, status);
        }
        catch (Exception e) {
          log.error(e, "Exception while marking task done: %s", task);
          throw Throwables.propagate(e);
        }

        // emit event and log
        int bytes = 0;
        for (DataSegment segment : status.getSegments()) {
          bytes += segment.getSize();
        }

        builder.setUser3(status.getStatusCode().toString().toLowerCase());

        emitter.emit(builder.build("indexer/time/run/millis", status.getDuration()));
        emitter.emit(builder.build("indexer/segment/count", status.getSegments().size()));
        emitter.emit(builder.build("indexer/segment/bytes", bytes));

        if (status.isFailure()) {
          emitter.emit(
              new AlertEvent.Builder().build(
                  String.format("Failed to index: %s", task.getDataSource()),
                  ImmutableMap.<String, Object>builder()
                              .put("task", task.getId())
                              .put("type", task.getType().toString().toLowerCase())
                              .put("dataSource", task.getDataSource())
                              .put("interval", task.getInterval())
                              .build()
              )
          );
        }

        log.info(
            "Task %s: %s (%d segments) (%d run duration)",
            status.getStatusCode(),
            task,
            status.getSegments().size(),
            status.getDuration()
        );

      }
    }
    );
  }
}
