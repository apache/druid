/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.task.concurrent;

import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.task.AbstractTask;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test task that can be given a series of commands to execute in its {@link #runTask} method.
 */
public class CommandQueueTask extends AbstractTask
{
  private static final Logger log = new Logger(CommandQueueTask.class);

  private final Object queueNotification = new Object();
  private final BlockingQueue<Command<?>> commandQueue = new LinkedBlockingQueue<>();

  private final AtomicBoolean finishRequested = new AtomicBoolean(false);
  private final AtomicInteger numCommandsExecuted = new AtomicInteger(0);

  private final CompletableFuture<TaskStatus> finalTaskStatus = new CompletableFuture<>();

  public CommandQueueTask(String datasource, String groupId)
  {
    super(
        StringUtils.format("test_%s_%s", datasource, UUID.randomUUID().toString()),
        groupId,
        null,
        datasource,
        null
    );
  }

  /**
   * Marks the run of this task as finished so that no new commands are accepted.
   * This methods waits for all the commands submitted so far to finish execution
   * and returns the final TaskStatus.
   */
  public TaskStatus finishRunAndGetStatus()
  {
    synchronized (finishRequested) {
      finishRequested.set(true);
    }
    synchronized (queueNotification) {
      queueNotification.notifyAll();
    }

    try {
      return finalTaskStatus.get(10, TimeUnit.SECONDS);
    }
    catch (Exception e) {
      throw new ISE(e, "Error waiting for task[%s] to finish", getId());
    }
  }

  /**
   * Submits the given runnable for execution on the task thread. This method
   * returns immediately and does not wait for the execution to finish.
   */
  public void submit(Runnable runnable)
  {
    // Add a command with a dummy return value
    Command<?> command = new Command<>(
        () -> {
          runnable.run();
          return 1;
        }
    );
    addToQueue(command);
  }

  /**
   * Executes the given callable on the task thread. This method waits until the
   * execution has finished and returns the computed value.
   */
  public <V> V execute(Callable<V> callable)
  {
    Command<V> command = new Command<>(callable);
    addToQueue(command);
    return waitForCommandToFinish(command);
  }

  private <V> void addToQueue(Command<V> command)
  {
    synchronized (finishRequested) {
      if (finishRequested.get()) {
        throw new ISE("Task[%s] cannot accept any more commands as it is already shutting down.", getId());
      } else {
        boolean added = commandQueue.offer(command);
        if (!added) {
          throw new ISE("Could not add command to task[%s].", getId());
        }
      }
    }

    synchronized (queueNotification) {
      queueNotification.notifyAll();
    }
  }

  private <V> V waitForCommandToFinish(Command<V> command)
  {
    try {
      return command.value.get(10, TimeUnit.SECONDS);
    }
    catch (Exception e) {
      throw new ISE(e, "Error waiting for command on task[%s] to finish", getId());
    }
  }

  @Override
  public TaskStatus runTask(TaskToolbox taskToolbox)
  {
    TaskStatus status;
    try {
      while (true) {
        synchronized (finishRequested) {
          if (finishRequested.get() && commandQueue.isEmpty()) {
            break;
          }
        }

        Command<?> command = commandQueue.poll();
        if (command == null) {
          synchronized (queueNotification) {
            queueNotification.wait(10_000);
          }
        } else {
          log.info("Running command[%d] for task[%s]", numCommandsExecuted.get(), getId());
          command.execute();
          numCommandsExecuted.incrementAndGet();
        }
      }
      status = TaskStatus.success(getId());
    }
    catch (Exception e) {
      log.error(e, "Error while running command[%d] for task[%s]", numCommandsExecuted.get(), getId());
      status = TaskStatus.failure(getId(), e.getMessage());
    }

    finalTaskStatus.complete(status);
    return status;
  }

  @Override
  public String getType()
  {
    return "test_command_executing";
  }

  @Override
  public boolean isReady(TaskActionClient taskActionClient)
  {
    return true;
  }

  @Override
  public void stopGracefully(TaskConfig taskConfig)
  {

  }

  private static class Command<V>
  {
    final Callable<V> callable;
    final CompletableFuture<V> value = new CompletableFuture<>();

    Command(Callable<V> callable)
    {
      this.callable = callable;
    }

    void execute() throws Exception
    {
      try {
        V result = callable.call();
        value.complete(result);
      }
      catch (Exception e) {
        value.completeExceptionally(e);
        throw e;
      }
    }
  }
}
