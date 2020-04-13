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

package org.apache.druid.testing;

import org.junit.runners.model.MultipleFailureException;
import org.junit.runners.model.Statement;
import org.junit.runners.model.TestTimedOutException;

import javax.annotation.Nullable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class is based on {@link org.junit.internal.runners.statements.FailOnTimeout}, additionally deadlocked
 * threads are detected.
 */
final class DeadlockDetectingFailOnTimeout extends Statement
{
  private final Statement originalStatement;
  private final TimeUnit timeUnit;
  private final long timeout;

  DeadlockDetectingFailOnTimeout(long timeout, TimeUnit timeoutUnit, Statement statement)
  {
    originalStatement = statement;
    this.timeout = timeout;
    timeUnit = timeoutUnit;
  }

  @Override
  public void evaluate() throws Throwable
  {
    CallableStatement callable = new CallableStatement();
    FutureTask<Throwable> task = new FutureTask<>(callable);
    ThreadGroup threadGroup = new ThreadGroup("FailOnTimeoutGroup");
    Thread thread = new Thread(threadGroup, task, "Time-limited test");
    try {
      thread.setDaemon(true);
      thread.start();
      callable.awaitStarted();
      Throwable throwable = getResult(task, thread);
      if (throwable != null) {
        throw throwable;
      }
    }
    finally {
      try {
        thread.join(1);
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      try {
        threadGroup.destroy();
      }
      catch (IllegalThreadStateException e) {
        // If a thread from the group is still alive, the ThreadGroup cannot be destroyed.
        // Swallow the exception to keep the same behavior prior to this change.
      }
    }
  }

  /**
   * Wait for the test task, returning the exception thrown by the test if the
   * test failed, an exception indicating a timeout if the test timed out, or
   * {@code null} if the test passed.
   */
  private Throwable getResult(FutureTask<Throwable> task, Thread thread)
  {
    try {
      if (timeout > 0) {
        return task.get(timeout, timeUnit);
      } else {
        return task.get();
      }
    }
    catch (InterruptedException e) {
      return e; // caller will re-throw; no need to call Thread.interrupt()
    }
    catch (ExecutionException e) {
      // test failed; have caller re-throw the exception thrown by the test
      return e.getCause();
    }
    catch (TimeoutException e) {
      return createTimeoutException(thread);
    }
  }

  private Exception createTimeoutException(Thread thread)
  {
    StackTraceElement[] stackTrace = thread.getStackTrace();
    Exception currThreadException = new TestTimedOutException(timeout, timeUnit);
    if (stackTrace != null) {
      currThreadException.setStackTrace(stackTrace);
      thread.interrupt();
    }
    Exception stuckThreadException = getStuckThreadException(thread);
    Exception deadlockException = getDeadlockedThreadsException();
    if (stuckThreadException != null || deadlockException != null) {
      List<Throwable> exceptions = Stream
          .of(currThreadException, stuckThreadException, deadlockException)
          .filter(Objects::nonNull)
          .collect(Collectors.toList());
      return new MultipleFailureException(exceptions);
    } else {
      return currThreadException;
    }
  }

  /**
   * Retrieves the stack trace for a given thread.
   *
   * @param thread The thread whose stack is to be retrieved.
   *
   * @return The stack trace; returns a zero-length array if the thread has
   * terminated or the stack cannot be retrieved for some other reason.
   */
  private StackTraceElement[] getStackTrace(Thread thread)
  {
    try {
      return thread.getStackTrace();
    }
    catch (SecurityException e) {
      return new StackTraceElement[0];
    }
  }

  @Nullable
  private Exception getStuckThreadException(Thread mainThread)
  {
    final Thread stuckThread = getStuckThread(mainThread);
    if (stuckThread == null) {
      return null;
    }
    Exception stuckThreadException = new Exception("Appears to be stuck in thread " + stuckThread.getName());
    stuckThreadException.setStackTrace(getStackTrace(stuckThread));
    return stuckThreadException;
  }

  /**
   * Determines whether the test appears to be stuck in some thread other than
   * the "main thread" (the one created to run the test).  This feature is experimental.
   * Behavior may change after the 4.12 release in response to feedback.
   *
   * @param mainThread The main thread created by {@code evaluate()}
   *
   * @return The thread which appears to be causing the problem, if different from
   * {@code mainThread}, or {@code null} if the main thread appears to be the
   * problem or if the thread cannot be determined.  The return value is never equal
   * to {@code mainThread}.
   */
  @SuppressWarnings("SSBasedInspection") // Prohibit check on Thread.getState()
  private Thread getStuckThread(Thread mainThread)
  {
    List<Thread> threadsInGroup = getThreadsInGroup(mainThread.getThreadGroup());
    if (threadsInGroup.isEmpty()) {
      return null;
    }

    // Now that we have all the threads in the test's thread group: Assume that
    // any thread we're "stuck" in is RUNNABLE.  Look for all RUNNABLE threads.
    // If just one, we return that (unless it equals threadMain).  If there's more
    // than one, pick the one that's using the most CPU time, if this feature is
    // supported.
    Thread stuckThread = null;
    long maxCpuTime = 0;
    for (Thread thread : threadsInGroup) {
      if (thread.getState() == Thread.State.RUNNABLE) {
        long threadCpuTime = cpuTime(thread);
        if (stuckThread == null || threadCpuTime > maxCpuTime) {
          stuckThread = thread;
          maxCpuTime = threadCpuTime;
        }
      }
    }
    return (stuckThread == mainThread) ? null : stuckThread;
  }

  /**
   * Returns all active threads belonging to a thread group.
   *
   * @param group The thread group.
   *
   * @return The active threads in the thread group.  The result should be a
   * complete list of the active threads at some point in time.  Returns an empty list
   * if this cannot be determined, e.g. because new threads are being created at an
   * extremely fast rate.
   */
  private List<Thread> getThreadsInGroup(ThreadGroup group)
  {
    final int activeThreadCount = group.activeCount(); // this is just an estimate
    int threadArraySize = Math.max(activeThreadCount * 2, 100);
    for (int loopCount = 0; loopCount < 5; loopCount++) {
      Thread[] threads = new Thread[threadArraySize];
      int enumCount = group.enumerate(threads);
      if (enumCount < threadArraySize) {
        return Arrays.asList(threads).subList(0, enumCount);
      }
      // if there are too many threads to fit into the array, enumerate's result
      // is >= the array's length; therefore we can't trust that it returned all
      // the threads.  Try again.
      threadArraySize += 100;
    }
    // threads are proliferating too fast for us.  Bail before we get into
    // trouble.
    return Collections.emptyList();
  }

  /**
   * Returns the CPU time used by a thread, if possible.
   *
   * @param thr The thread to query.
   *
   * @return The CPU time used by {@code thr}, or 0 if it cannot be determined.
   */
  private long cpuTime(Thread thr)
  {
    ThreadMXBean mxBean = ManagementFactory.getThreadMXBean();
    if (mxBean.isThreadCpuTimeSupported()) {
      try {
        return mxBean.getThreadCpuTime(thr.getId());
      }
      catch (UnsupportedOperationException ignore) {
        // fall through
      }
    }
    return 0;
  }

  @Nullable
  private Exception getDeadlockedThreadsException()
  {
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final long[] deadlockedThreadIds = threadMXBean.findDeadlockedThreads();
    if (deadlockedThreadIds == null) {
      return null;
    }
    Exception deadlockException = new Exception("Deadlocked threads:");
    for (long deadlockedThreadId : deadlockedThreadIds) {
      ThreadInfo threadInfo = threadMXBean.getThreadInfo(deadlockedThreadId);
      Exception threadException = new Exception(threadInfo.getThreadName() + " at " + threadInfo.getLockName());
      threadException.setStackTrace(threadInfo.getStackTrace());
      deadlockException.addSuppressed(threadException);
    }
    return deadlockException;
  }

  private class CallableStatement implements Callable<Throwable>
  {
    private final CountDownLatch startLatch = new CountDownLatch(1);

    @Override
    public Throwable call() throws Exception
    {
      try {
        startLatch.countDown();
        originalStatement.evaluate();
      }
      catch (Exception e) {
        throw e;
      }
      catch (Throwable e) {
        return e;
      }
      return null;
    }

    public void awaitStarted() throws InterruptedException
    {
      startLatch.await();
    }
  }
}
