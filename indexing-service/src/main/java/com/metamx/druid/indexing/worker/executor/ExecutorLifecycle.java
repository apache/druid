package com.metamx.druid.indexing.worker.executor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.coordinator.TaskRunner;
import com.metamx.emitter.EmittingLogger;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Encapsulates the lifecycle of a task executor. Loads one task, runs it, writes its status, and all the while
 * monitors its parent process.
 */
public class ExecutorLifecycle
{
  private static final EmittingLogger log = new EmittingLogger(ExecutorLifecycle.class);

  private final File taskFile;
  private final File statusFile;
  private final TaskRunner taskRunner;
  private final InputStream parentStream;
  private final ObjectMapper jsonMapper;

  private final ExecutorService parentMonitorExec = Executors.newFixedThreadPool(
      1,
      new ThreadFactoryBuilder().setNameFormat("parent-monitor-%d").setDaemon(true).build()
  );

  private volatile ListenableFuture<TaskStatus> statusFuture = null;

  public ExecutorLifecycle(
      File taskFile,
      File statusFile,
      TaskRunner taskRunner,
      InputStream parentStream,
      ObjectMapper jsonMapper
  )
  {
    this.taskFile = taskFile;
    this.statusFile = statusFile;
    this.taskRunner = taskRunner;
    this.parentStream = parentStream;
    this.jsonMapper = jsonMapper;
  }

  @LifecycleStart
  public void start()
  {
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
    // If a message comes over stdin, we want to handle it
    // If stdin reaches eof, the parent is gone, and we should shut down
    parentMonitorExec.submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              final BufferedReader parentReader = new BufferedReader(new InputStreamReader(parentStream));
              String messageString;
              while ((messageString = parentReader.readLine()) != null) {
                final Map<String, Object> message = jsonMapper
                    .readValue(
                        messageString,
                        new TypeReference<Map<String, Object>>()
                        {
                        }
                    );

                if (message == null) {
                  break;
                } else if (message.get("shutdown") != null && message.get("shutdown").equals("now")) {
                  log.info("Shutting down!");
                  task.shutdown();
                } else {
                  throw new ISE("Unrecognized message from parent: %s", message);
                }
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

    statusFuture = Futures.transform(
        taskRunner.run(task), new Function<TaskStatus, TaskStatus>()
    {
      @Override
      public TaskStatus apply(TaskStatus taskStatus)
      {
        try {
          log.info(
              "Task completed with status: %s",
              jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(taskStatus)
          );

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
