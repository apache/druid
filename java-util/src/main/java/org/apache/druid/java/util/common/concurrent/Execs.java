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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class Execs
{
  /**
   * Returns an ExecutorService which is terminated and shutdown from the beginning and not able to accept any tasks.
   */
  public static ExecutorService dummy()
  {
    return DummyExecutorService.INSTANCE;
  }

  public static ExecutorService singleThreaded(@NotNull String nameFormat)
  {
    return singleThreaded(nameFormat, null);
  }

  public static ExecutorService singleThreaded(@NotNull String nameFormat, @Nullable Integer priority)
  {
    return Executors.newSingleThreadExecutor(makeThreadFactory(nameFormat, priority));
  }

  public static ExecutorService multiThreaded(int threads, @NotNull String nameFormat)
  {
    return multiThreaded(threads, nameFormat, null);
  }

  public static ExecutorService multiThreaded(int threads, @NotNull String nameFormat, @Nullable Integer priority)
  {
    return Executors.newFixedThreadPool(threads, makeThreadFactory(nameFormat, priority));
  }

  public static ScheduledExecutorService scheduledSingleThreaded(@NotNull String nameFormat)
  {
    return scheduledSingleThreaded(nameFormat, null);
  }

  public static ScheduledExecutorService scheduledSingleThreaded(@NotNull String nameFormat, @Nullable Integer priority)
  {
    return Executors.newSingleThreadScheduledExecutor(makeThreadFactory(nameFormat, priority));
  }

  public static ThreadFactory makeThreadFactory(@NotNull String nameFormat)
  {
    return makeThreadFactory(nameFormat, null);
  }

  public static ThreadFactory makeThreadFactory(@NotNull String nameFormat, @Nullable Integer priority)
  {
    final ThreadFactoryBuilder builder = new ThreadFactoryBuilder()
        .setDaemon(true)
        .setNameFormat(nameFormat);
    if (priority != null) {
      builder.setPriority(priority);
    }

    return builder.build();
  }

  public static Thread makeThread(String name, Runnable runnable, boolean isDaemon)
  {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(name), "name null/empty");
    Preconditions.checkNotNull(runnable, "null runnable");

    Thread t = new Thread(runnable);
    t.setName(name);
    t.setDaemon(isDaemon);
    return t;
  }

  /**
   * @param nameFormat nameformat for threadFactory
   * @param capacity   maximum capacity after which the executorService will block on accepting new tasks
   *
   * @return ExecutorService which blocks accepting new tasks when the capacity reached
   */
  public static ExecutorService newBlockingSingleThreaded(final String nameFormat, final int capacity)
  {
    return newBlockingSingleThreaded(nameFormat, capacity, null);
  }

  public static ExecutorService newBlockingSingleThreaded(
      final String nameFormat,
      final int capacity,
      final Integer priority
  )
  {
    final BlockingQueue<Runnable> queue;
    if (capacity > 0) {
      queue = new ArrayBlockingQueue<>(capacity);
    } else {
      queue = new SynchronousQueue<>();
    }
    return new ThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, queue, makeThreadFactory(nameFormat, priority),
        new RejectedExecutionHandler()
        {
          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
          {
            try {
              executor.getQueue().put(r);
            }
            catch (InterruptedException e) {
              throw new RejectedExecutionException("Got Interrupted while adding to the Queue", e);
            }
          }
        }
    );
  }

  private static final AtomicLong fjpWorkerThreadCount = new AtomicLong(0L);

  public static ForkJoinWorkerThread makeWorkerThread(String name, ForkJoinPool pool)
  {
    final ForkJoinWorkerThread t = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
    t.setDaemon(true);
    final long threadNumber = fjpWorkerThreadCount.incrementAndGet();
    t.setName(StringUtils.nonStrictFormat(name, threadNumber));
    return t;
  }

  private static final int DUMMY_THREAD_NUMBER = 17;

  /**
   * Fail fast if the format can't take a single argument integer for a thread counter.
   *
   * Note that LACK of any argument in the format string still renders a valid name
   *
   * @param format The name format to check
   *
   * @throws java.util.IllegalFormatException if the format passed in does is not able to take a single thread parameter
   */
  public static void checkThreadNameFormat(String format)
  {
    StringUtils.format(format, DUMMY_THREAD_NUMBER);
  }

  /**
   * Get the result for the future (without timeout), but do so in a way safe for running in a ForkJoinPool
   *
   * @param future The future to block on completion
   * @param <T>    The type of the return value
   *
   * @return The result of the future if successfully completed, or one of the exceptions if not
   *
   * @throws InterruptedException If the call to future.get() was interrupted
   * @throws ExecutionException   If the future completed with an exception
   */
  public static <T> T futureManagedBlockGet(final Future<? extends T> future)
      throws InterruptedException, ExecutionException
  {
    ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker()
    {
      @Override
      public boolean block() throws InterruptedException
      {
        try {
          future.get();
        }
        catch (ExecutionException e) {
          // Ignore, will be caught when get is called below
        }
        return true;
      }

      @Override
      public boolean isReleasable()
      {
        return future.isDone();
      }
    });
    return future.get();
  }

  /**
   * Attempt to get the result of the future before the deadline, but do so in a way safe to run in a ForkJoinPool.
   * The deadline is best effort. It is possible the future completes, but the deadline is exceeded before the result
   * can be returned. In such a scenario a TimeoutException will be thrown.
   *
   * The caller is responsible for handling the state of the Future in the case of an exception being thrown.
   * Specifically, if an InterruptedException or a TimeoutException is thrown, there is no attempt in this method
   * to change the behavior of the future. The caller should handle the potentially still active future as they see fit.
   *
   * @param future   The future to await completion
   * @param deadline Best effort deadline for the completion of the future.
   * @param <T>      The future's yielded type
   *
   * @return The yield of the future or else a thrown exception
   *
   * @throws InterruptedException If the call to future.get is interrupted
   * @throws TimeoutException     If the deadline is exceeded
   * @throws ExecutionException   If the future completed with an exception
   */
  public static <T> T futureManagedBlockGet(final Future<? extends T> future, final DateTime deadline)
      throws InterruptedException, TimeoutException, ExecutionException
  {
    ForkJoinPool.managedBlock(new ForkJoinPool.ManagedBlocker()
    {
      @Override
      public boolean block() throws InterruptedException
      {
        try {
          future.get(JodaUtils.timeoutForDeadline(deadline), TimeUnit.MILLISECONDS);
        }
        catch (ExecutionException | TimeoutException e) {
          // Will get caught later
        }
        return true;
      }

      @Override
      public boolean isReleasable()
      {
        return future.isDone() || deadline.isBefore(DateTimes.nowUtc());
      }
    });
    return future.get(JodaUtils.timeoutForDeadline(deadline), TimeUnit.MILLISECONDS);
  }
}
