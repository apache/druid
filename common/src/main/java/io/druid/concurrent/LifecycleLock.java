/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

/**
 * A synchronization tool for lifecycled objects (see {@link io.druid.java.util.common.lifecycle.Lifecycle}, that need
 * happens-before between start() and other methods and/or to check that the object was successfully started in other
 * methods.
 *
 * Guarantees in terms of JMM: happens-before between {@link #exitStart()} and {@link #awaitStarted()},
 * exitStart() and {@link #canStop()}, if it returns {@code true}.
 *
 * Example:
 * class ExampleLifecycledClass
 * {
 *   final LifecycleLock lifecycleLock = new LifecycleLock();
 *
 *   void start()
 *   {
 *     if (!lifecycleLock.canStart()) {
 *       .. return or throw exception
 *     }
 *     try {
 *       .. do start
 *       lifecycleLock.started();
 *     }
 *     finally {
 *       lifecycleLock.exitStart();
 *     }
 *   }
 *
 *   void otherMethod()
 *   {
 *     Preconditions.checkState(lifecycleLock.awaitStarted());
 *     // all actions done in start() are visible here
 *     .. do stuff
 *   }
 *
 *   void stop()
 *   {
 *     if (!lifecycleLock.canStop()) {
 *       .. return or throw exception
 *     }
 *     // all actions done in start() are visible here
 *     .. do stop
 *   }
 * }
 */
public final class LifecycleLock
{
  private static class Sync extends AbstractQueuedSynchronizer
  {
    private static final int NOT_STARTED = 0;
    private static final int STARTING = 1;
    private static final int STARTED = 2;
    private static final int START_EXITED_SUCCESSFUL = 3;
    private static final int START_EXITED_FAIL = 4;
    private static final int STOPPING = 5;
    private static final int STOPPED = 6;

    boolean canStart()
    {
      return compareAndSetState(NOT_STARTED, STARTING);
    }

    void started()
    {
      if (!compareAndSetState(STARTING, STARTED)) {
        throw new IllegalMonitorStateException("Called started() not in the context of start()");
      }
    }

    void exitStart()
    {
      // see tryReleaseShared()
      releaseShared(1);
    }

    @Override
    protected boolean tryReleaseShared(int ignore)
    {
      while (true) {
        int state = getState();
        if (state == STARTING) {
          if (compareAndSetState(STARTING, START_EXITED_FAIL)) {
            return true;
          }
        } else if (state == STARTED) {
          if (compareAndSetState(STARTED, START_EXITED_SUCCESSFUL)) {
            return true;
          }
        } else {
          throw new IllegalMonitorStateException("exitStart() called not in the end of the start() method");
        }
      }
    }

    boolean awaitStarted()
    {
      try {
        // see tryAcquireShared()
        acquireSharedInterruptibly(1);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return getState() == START_EXITED_SUCCESSFUL;
    }

    boolean awaitStarted(long timeNanos)
    {
      try {
        // see tryAcquireShared()
        if (!tryAcquireSharedNanos(1, timeNanos)) {
          return false;
        }
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
      return getState() == START_EXITED_SUCCESSFUL;
    }

    @Override
    protected int tryAcquireShared(int ignore)
    {
      return getState() > STARTED ? 1 : -1;
    }

    boolean canStop()
    {
      while (true) {
        int state = getState();
        if (state == START_EXITED_FAIL || state == STOPPING) {
          return false;
        } else if (state == START_EXITED_SUCCESSFUL) {
          if (compareAndSetState(START_EXITED_SUCCESSFUL, STOPPING)) {
            return true;
          }
        } else {
          throw new IllegalMonitorStateException("Called canStop() before start()");
        }
      }
    }

    void exitStop()
    {
      if (!compareAndSetState(STOPPING, STOPPED)) {
        throw new IllegalMonitorStateException("Called exitStop() not in the context of stop()");
      }
    }

    void reset()
    {
      if (!compareAndSetState(STOPPED, NOT_STARTED)) {
        throw new IllegalMonitorStateException("Not called exitStop() before reset()");
      }
    }
  }

  private final Sync sync = new Sync();

  /**
   * Start latch, only one canStart() call in any thread on this LifecycleLock object could return true, if {@link
   * #reset()} is not called in between.
   */
  public boolean canStart()
  {
    return sync.canStart();
  }

  /**
   * Announce the start was successful.
   *
   * @throws IllegalMonitorStateException if {@link #canStart()} is not yet called or if {@link #exitStart()} is already
   * called on this LifecycleLock
   */
  public void started()
  {
    sync.started();
  }

  /**
   * Must be called before exit from start() on the lifecycled object, usually in a finally block.
   *
   * @throws IllegalMonitorStateException if {@link #canStart()} is not yet called on this LifecycleLock
   */
  public void exitStart()
  {
    sync.exitStart();
  }

  /**
   * Awaits until {@link #exitStart()} is called, if needed, and returns {@code true} if {@link #started()} was called
   * before that. Returns {@code false} if {@link #started()} is not called before {@link #exitStart()}, or if {@link
   * #canStop()} is already called on this LifecycleLock.
   */
  public boolean awaitStarted()
  {
    return sync.awaitStarted();
  }

  /**
   * Awaits until {@link #exitStart()} is called for at most the specified timeout, and returns {@code true} if {@link
   * #started()} was called before that. Returns {@code false} if {@code started()} wasn't called before {@code
   * exitStart()}, or if {@code exitStart()} isn't called on this LifecycleLock until the specified timeout expires, or
   * if {@link #canStop()} is already called on this LifecycleLock.
   */
  public boolean awaitStarted(long timeout, TimeUnit unit)
  {
    return sync.awaitStarted(unit.toNanos(timeout));
  }

  /**
   * Stop latch, only one canStop() call in any thread on this LifecycleLock object could return {@code true}. If
   * {@link #started()} wasn't called on this LifecycleLock object, always returns {@code false}.
   *
   * @throws IllegalMonitorStateException if {@link #exitStart()} are not yet called on this LifecycleLock
   */
  public boolean canStop()
  {
    return sync.canStop();
  }

  /**
   * If this LifecycleLock is used in a restartable object, which uses {@link #reset()}, exitStop() must be called
   * before exit from stop() on this object, usually in a finally block.
   *
   * @throws IllegalMonitorStateException if {@link #canStop()} is not yet called on this LifecycleLock
   */
  public void exitStop()
  {
    sync.exitStop();
  }

  /**
   * Resets the LifecycleLock after {@link #exitStop()}, so that {@link #canStart()} could be called again.
   *
   * @throws IllegalMonitorStateException if {@link #exitStop()} is not yet called on this LifecycleLock
   */
  public void reset()
  {
    sync.reset();
  }
}
