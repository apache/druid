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

package org.apache.druid.server.coordinator.simulate;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * An executor that keeps submitted tasks in a queue until they are explicitly
 * invoked by calling one of these methods:
 * <ul>
 *   <li>{@link #finishNextPendingTask()}</li>
 *   <li>{@link #finishNextPendingTasks(int)}</li>
 *   <li>{@link #finishAllPendingTasks()}</li>
 * </ul>
 */
public class BlockingExecutorService implements ScheduledExecutorService
{
  private static final Logger log = new Logger(BlockingExecutorService.class);

  private final String nameFormat;
  private final boolean ignoreScheduledTasks;
  private final Queue<Task<?>> taskQueue = new ConcurrentLinkedQueue<>();

  public BlockingExecutorService(String nameFormat, boolean ignoreScheduledTasks)
  {
    this.nameFormat = nameFormat;
    this.ignoreScheduledTasks = ignoreScheduledTasks;
  }

  public boolean hasPendingTasks()
  {
    return !taskQueue.isEmpty();
  }

  /**
   * Executes the next pending task on the calling thread itself.
   */
  public int finishNextPendingTask()
  {
    log.debug("[%s] Executing next pending task", nameFormat);
    Task<?> task = taskQueue.poll();
    if (task != null) {
      task.executeNow();
      return 1;
    } else {
      return 0;
    }
  }

  /**
   * Executes the next {@code numTasksToExecute} pending tasks on the calling
   * thread itself.
   */
  public int finishNextPendingTasks(int numTasksToExecute)
  {
    log.debug("[%s] Executing %d pending tasks", nameFormat, numTasksToExecute);
    int executedTaskCount = 0;
    for (; executedTaskCount < numTasksToExecute; ++executedTaskCount) {
      Task<?> task = taskQueue.poll();
      if (task == null) {
        break;
      } else {
        task.executeNow();
      }
    }
    return executedTaskCount;
  }

  /**
   * Executes all the remaining pending tasks on the calling thread itself.
   * <p>
   * Note: This method can keep running forever if another thread keeps submitting
   * new tasks to the executor.
   */
  public int finishAllPendingTasks()
  {
    log.debug("[%s] Executing all pending tasks", nameFormat);
    Task<?> task;
    int executedTaskCount = 0;
    while ((task = taskQueue.poll()) != null) {
      task.executeNow();
      ++executedTaskCount;
    }

    return executedTaskCount;
  }

  // Task submission operations
  @Override
  public <T> Future<T> submit(Callable<T> task)
  {
    return addTaskToQueue(task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result)
  {
    return addTaskToQueue(() -> {
      task.run();
      return result;
    });
  }

  @Override
  public Future<?> submit(Runnable task)
  {
    return addTaskToQueue(() -> {
      task.run();
      return null;
    });
  }

  @Override
  public void execute(Runnable command)
  {
    submit(command);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit)
  {
    if (ignoreScheduledTasks) {
      log.debug("[%s] Ignoring scheduled task", nameFormat);
      return new WrappingScheduledFuture<>(CompletableFuture.completedFuture(null));
    }

    // Ignore the delay and just queue the task
    return new WrappingScheduledFuture<>(submit(command));
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit)
  {
    if (ignoreScheduledTasks) {
      log.debug("[%s] Ignoring scheduled task", nameFormat);
      return new WrappingScheduledFuture<>(CompletableFuture.completedFuture(null));
    }

    // Ignore the delay and just queue the task
    return new WrappingScheduledFuture<>(submit(callable));
  }

  private <T> Future<T> addTaskToQueue(Callable<T> callable)
  {
    Task<T> task = new Task<>(callable);
    taskQueue.add(task);
    return task.future;
  }

  // Termination operations
  @Override
  public void shutdown()
  {

  }

  @Override
  public List<Runnable> shutdownNow()
  {
    return null;
  }

  @Override
  public boolean isShutdown()
  {
    return false;
  }

  @Override
  public boolean isTerminated()
  {
    return false;
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    return false;
  }

  // Unsupported operations
  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks,
      long timeout,
      TimeUnit unit
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command,
      long initialDelay,
      long period,
      TimeUnit unit
  )
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command,
      long initialDelay,
      long delay,
      TimeUnit unit
  )
  {
    throw new UnsupportedOperationException();
  }

  /**
   * Task that can be invoked to complete the corresponding future.
   */
  private static class Task<T>
  {
    private final Callable<T> callable;
    private final CompletableFuture<T> future = new CompletableFuture<>();

    private Task(Callable<T> callable)
    {
      this.callable = callable;
    }

    private void executeNow()
    {
      try {
        T result = callable.call();
        future.complete(result);
      }
      catch (Exception e) {
        throw new ISE("Error while executing task", e);
      }
    }
  }

  /**
   * Wraps a Future into a ScheduledFuture.
   */
  private static class WrappingScheduledFuture<V> implements ScheduledFuture<V>
  {
    private final Future<V> future;

    private WrappingScheduledFuture(Future<V> future)
    {
      this.future = future;
    }

    @Override
    public long getDelay(TimeUnit unit)
    {
      return 0;
    }

    @Override
    public int compareTo(Delayed o)
    {
      return 0;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning)
    {
      return future.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled()
    {
      return future.isCancelled();
    }

    @Override
    public boolean isDone()
    {
      return future.isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException
    {
      return future.get();
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException
    {
      return future.get(timeout, unit);
    }
  }

}
