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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A synchronization tool for lifecycled objects (see {@link io.druid.java.util.common.lifecycle.Lifecycle}, that need
 * happens-before between start() and other methods and/or to check that the object was successfully started in other
 * methods.
 *
 * Guarantees in terms of JMM: happens-before between {@link #release()} and {@link #isStarted()},
 * release() and {@link #canStop()}.
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
 *       lifecycleLock.release();
 *     }
 *   }
 *
 *   void otherMethod()
 *   {
 *     Preconditions.checkState(lifecycleLock.isStarted());
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
  private enum State { NOT_STARTED, STARTING, STARTED, STOPPED }

  private final AtomicReference<State> state = new AtomicReference<>(State.NOT_STARTED);
  private final CountDownLatch canStop = new CountDownLatch(1);

  /**
   * Start latch, only one canStart() call in any thread on this LifecycleLock object could return true.
   */
  public boolean canStart()
  {
    return state.compareAndSet(State.NOT_STARTED, State.STARTING);
  }

  /**
   * Announce the start was successful.
   */
  public void started()
  {
    if (!state.compareAndSet(State.STARTING, State.STARTED)) {
      throw new IllegalStateException("Called started() not in the context of start()");
    }
  }

  /**
   * Must be called before exit from start() on the lifecycled object, usually in a finally block
   */
  public void release()
  {
    canStop.countDown();
  }

  private void awaitStarted()
  {
    try {
      canStop.await();
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Awaits until {@link #release()} is called, if needed, and returns {@code true} if {@link #started()} was called
   * before that.
   */
  public boolean isStarted()
  {
    awaitStarted();
    return state.get() == State.STARTED;
  }

  /**
   * Stop latch, only one canStop() call in any thread on this LifecycleLock object could return {@code true}. Awaits
   * until {@link #release()} is called, if needed.
   */
  public boolean canStop()
  {
    awaitStarted();
    return state.compareAndSet(State.STARTED, State.STOPPED);
  }
}
