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

package org.apache.druid.java.util.common.concurrent;

import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * A decorator around a {@link ThreadPoolExecutor} that emits metrics about the wait time for tasks in the queue
 * and other relevant metrics about the state of the executor.
 */
public class WaitTimeMonitoringExecutorService implements ExecutorService
{
  private final ThreadPoolExecutor executor;
  private final ServiceEmitter emitter;
  private final String metricPrefix;

  public WaitTimeMonitoringExecutorService(ThreadPoolExecutor executor, ServiceEmitter emitter, String metricPrefix)
  {
    this.executor = executor;
    this.emitter = emitter;
    this.metricPrefix = metricPrefix;
  }

  @Override
  public void shutdown()
  {
    executor.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow()
  {
    return executor.shutdownNow();
  }

  @Override
  public boolean isShutdown()
  {
    return executor.isShutdown();
  }

  @Override
  public boolean isTerminated()
  {
    return executor.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    return executor.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task)
  {
    final long startTime = System.currentTimeMillis();
    return executor.submit(() -> {
          final long queueDuration = System.currentTimeMillis() - startTime;
          emitMetrics(queueDuration);
          return task.call();
        }
    );
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result)
  {
    final long startTime = System.currentTimeMillis();
    return executor.submit(() -> {
          final long queueDuration = System.currentTimeMillis() - startTime;
          emitMetrics(queueDuration);
          task.run();
        }, result
    );
  }

  @Override
  public Future<?> submit(Runnable task)
  {
    final long startTime = System.currentTimeMillis();
    return executor.submit(() -> {
          final long queueDuration = System.currentTimeMillis() - startTime;
          emitMetrics(queueDuration);
          task.run();
        }
    );
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException
  {
    return executor.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException
  {
    return executor.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException
  {
    return executor.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException
  {
    return executor.invokeAny(tasks, timeout, unit);
  }

  @Override
  public void execute(Runnable command)
  {
    executor.execute(command);
  }

  /**
   * Emits various metrics about the executor service's state.
   *
   * @param taskQueuedDuration the time a task spent in the queue before execution.
   */
  private void emitMetrics(long taskQueuedDuration)
  {
    final ServiceMetricEvent.Builder builder = new ServiceMetricEvent.Builder();
    emitter.emit(builder.setMetric(metricPrefix + "/taskQueuedDuration", taskQueuedDuration));
    emitter.emit(builder.setMetric(metricPrefix + "/queuedTasks", executor.getQueue().size()));
    emitter.emit(builder.setMetric(metricPrefix + "/taskCount", executor.getTaskCount()));
    emitter.emit(builder.setMetric(metricPrefix + "/activeThreads", executor.getActiveCount()));
    emitter.emit(builder.setMetric(metricPrefix + "/completedTasks", executor.getCompletedTaskCount()));
  }
}
