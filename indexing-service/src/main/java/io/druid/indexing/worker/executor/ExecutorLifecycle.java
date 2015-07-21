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

package io.druid.indexing.worker.executor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSink;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.server.DruidNode;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.CharBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.file.StandardOpenOption;
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
  private final DruidNode druidNode;

  private volatile ListenableFuture<TaskStatus> statusFuture = null;

  @Inject
  public ExecutorLifecycle(
      ExecutorLifecycleConfig config,
      TaskActionClientFactory taskActionClientFactory,
      TaskRunner taskRunner,
      ObjectMapper jsonMapper,
      @Self DruidNode druidNode
  )
  {
    this.config = config;
    this.taskActionClientFactory = taskActionClientFactory;
    this.taskRunner = taskRunner;
    this.jsonMapper = jsonMapper;
    this.druidNode = druidNode;
  }

  @LifecycleStart
  public void start()
  {
    final File taskFile = Preconditions.checkNotNull(config.getTaskFile(), "taskFile");
    final File statusFile = Preconditions.checkNotNull(config.getStatusFile(), "statusFile");
    final File portFile = Preconditions.checkNotNull(config.getPortFile(), "portFile");

    try(final FileChannel portFileChannel = FileChannel.open(portFile.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
      final String portStr = String.format("%d", druidNode.getPort());
      final FileLock lock = portFileChannel.lock();
      try {
        CharsetEncoder encoder = Charsets.UTF_8.newEncoder();
        portFileChannel.write(encoder.encode(CharBuffer.wrap(portStr)));
      }finally{
        lock.release();
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

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
    final File portFile = Preconditions.checkNotNull(config.getPortFile(), "portFile");
    if(!portFile.delete())
    {
      log.warn("Unable to delete task port file at [%s]", portFile.toString());
    } else {
      log.info("Deleted port file at [%s]", portFile.toString());
    }
  }
}
