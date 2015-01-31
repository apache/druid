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

package io.druid.concurrent;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 */
public class Execs
{
  public static ExecutorService singleThreaded(String nameFormat)
  {
    return Executors.newSingleThreadExecutor(makeThreadFactory(nameFormat));
  }

  public static ExecutorService multiThreaded(int threads, String nameFormat)
  {
    return Executors.newFixedThreadPool(threads, makeThreadFactory(nameFormat));
  }

  public static ScheduledExecutorService scheduledSingleThreaded(String nameFormat)
  {
    return Executors.newSingleThreadScheduledExecutor(makeThreadFactory(nameFormat));
  }

  public static ThreadFactory makeThreadFactory(String nameFormat)
  {
    return new ThreadFactoryBuilder().setDaemon(true).setNameFormat(nameFormat).build();
  }

  /**
   * @param nameFormat nameformat for threadFactory
   * @param capacity   maximum capacity after which the executorService will block on accepting new tasks
   *
   * @return ExecutorService which blocks accepting new tasks when the capacity reached
   */
  public static ExecutorService newBlockingSingleThreaded(final String nameFormat, final int capacity)
  {
    final BlockingQueue<Runnable> queue;
    if (capacity > 0) {
      queue = new ArrayBlockingQueue<>(capacity);
    } else {
      queue = new SynchronousQueue<>();
    }
    return new ThreadPoolExecutor(
        1, 1, 0L, TimeUnit.MILLISECONDS, queue, makeThreadFactory(nameFormat),
        new RejectedExecutionHandler()
        {
          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor)
          {
            try {
              executor.getQueue().put(r);
            }
            catch (InterruptedException e) {
              throw new RejectedExecutionException("Got Interrupted while adding to the Queue");
            }
          }
        }
    );
  }
}
