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

import com.google.common.util.concurrent.AbstractListeningExecutorService;
import com.google.errorprone.annotations.concurrent.GuardedBy;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

// Copy of Guava's Apache 2.0 licensed https://github.com/google/guava/blob/a5cafa67da64a12444037bd4f4c30c39a0c184aa/guava/src/com/google/common/util/concurrent/MoreExecutors.java#L240-L339

/**
 * Creates an executor service that runs each task in the thread that invokes {@code
 * execute/submit}, as in {@link java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy} This applies both to individually submitted
 * tasks and to collections of tasks submitted via {@code invokeAll} or {@code invokeAny}. In the
 * latter case, tasks will run serially on the calling thread. Tasks are run to completion before
 * a {@code Future} is returned to the caller (unless the executor has been shutdown).
 *
 * <p>Although all tasks are immediately executed in the thread that submitted the task, this
 * {@code ExecutorService} imposes a small locking overhead on each task submission in order to
 * implement shutdown and termination behavior.
 *
 * <p>The implementation deviates from the {@code ExecutorService} specification with regards to
 * the {@code shutdownNow} method. First, "best-effort" with regards to canceling running tasks is
 * implemented as "no-effort". No interrupts or other attempts are made to stop threads executing
 * tasks. Second, the returned list will always be empty, as any submitted task is considered to
 * have started execution. This applies also to tasks given to {@code invokeAll} or {@code
 * invokeAny} which are pending serial execution, even the subset of the tasks that have not yet
 * started execution. It is unclear from the {@code ExecutorService} specification if these should
 * be included, and it's much easier to implement the interpretation that they not be. Finally, a
 * call to {@code shutdown} or {@code shutdownNow} may result in concurrent calls to {@code
 * invokeAll/invokeAny} throwing RejectedExecutionException, although a subset of the tasks may
 * already have been executed.
 */
public class DirectExecutorService extends AbstractListeningExecutorService
{

  /**
   * Lock used whenever accessing the state variables (runningTasks, shutdown) of the executor
   */
  private final Object lock = new Object();

  /*
   * Conceptually, these two variables describe the executor being in
   * one of three states:
   *   - Active: shutdown == false
   *   - Shutdown: runningTasks > 0 and shutdown == true
   *   - Terminated: runningTasks == 0 and shutdown == true
   */
  @GuardedBy("lock")
  private int runningTasks = 0;

  @GuardedBy("lock")
  private boolean shutdown = false;

  @Override
  public void execute(Runnable command)
  {
    startTask();
    try {
      command.run();
    }
    finally {
      endTask();
    }
  }

  @Override
  public boolean isShutdown()
  {
    synchronized (lock) {
      return shutdown;
    }
  }

  @Override
  public void shutdown()
  {
    synchronized (lock) {
      shutdown = true;
      if (runningTasks == 0) {
        lock.notifyAll();
      }
    }
  }

  // See newDirectExecutorService javadoc for unusual behavior of this method.
  @Override
  public List<Runnable> shutdownNow()
  {
    shutdown();
    return Collections.emptyList();
  }

  @Override
  public boolean isTerminated()
  {
    synchronized (lock) {
      return shutdown && runningTasks == 0;
    }
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException
  {
    long nanos = unit.toNanos(timeout);
    synchronized (lock) {
      while (true) {
        if (shutdown && runningTasks == 0) {
          return true;
        } else if (nanos <= 0) {
          return false;
        } else {
          long now = System.nanoTime();
          TimeUnit.NANOSECONDS.timedWait(lock, nanos);
          nanos -= System.nanoTime() - now; // subtract the actual time we waited
        }
      }
    }
  }

  /**
   * Checks if the executor has been shut down and increments the running task count.
   *
   * @throws RejectedExecutionException if the executor has been previously shutdown
   */
  private void startTask()
  {
    synchronized (lock) {
      if (shutdown) {
        throw new RejectedExecutionException("Executor already shutdown");
      }
      runningTasks++;
    }
  }

  /**
   * Decrements the running task count.
   */
  private void endTask()
  {
    synchronized (lock) {
      int numRunning = --runningTasks;
      if (numRunning == 0) {
        lock.notifyAll();
      }
    }
  }
}
