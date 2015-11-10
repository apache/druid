/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.common.config;

import org.apache.logging.log4j.core.util.Cancellable;
import org.apache.logging.log4j.core.util.ShutdownCallbackRegistry;

import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class Log4jShutdown implements ShutdownCallbackRegistry, org.apache.logging.log4j.core.LifeCycle
{
  private final AtomicReference<State> state = new AtomicReference<>(State.INITIALIZED);
  private final Queue<Runnable> shutdownCallbacks = new ConcurrentLinkedQueue<>();
  private final AtomicBoolean callbacksRun = new AtomicBoolean(false);

  @Override
  public Cancellable addShutdownCallback(final Runnable callback)
  {
    if (callback == null) {
      throw new NullPointerException("callback");
    }
    if (!isStarted()) {
      throw new IllegalStateException("Not started");
    }
    final Cancellable cancellable = new Cancellable()
    {
      private volatile boolean cancelled = false;
      private final AtomicBoolean ran = new AtomicBoolean(false);

      @Override
      public void cancel()
      {
        cancelled = true;
      }

      @Override
      public void run()
      {
        if (!cancelled) {
          if (ran.compareAndSet(false, true)) {
            callback.run();
          }
        }
      }
    };
    shutdownCallbacks.add(cancellable);
    if (!isStarted()) {
      // We are shutting down in the middle of registering... Make sure the callback fires
      callback.run();
      throw new IllegalStateException("Shutting down while adding shutdown hook. Callback fired just in case");
    }
    return cancellable;
  }

  @Override
  public State getState()
  {
    return state.get();
  }

  @Override
  public void initialize()
  {
    // NOOP, state is always at least INITIALIZED
  }

  @Override
  public void start()
  {
    if (!state.compareAndSet(State.INITIALIZED, State.STARTED)) { // Skip STARTING
      throw new IllegalStateException(String.format("Expected state [%s] found [%s]", State.INITIALIZED, state.get()));
    }
  }

  @Override
  public void stop()
  {
    if (callbacksRun.get()) {
      return;
    }
    if (!state.compareAndSet(State.STARTED, State.STOPPED)) {
      throw new IllegalStateException(String.format("Expected state [%s] found [%s]", State.STARTED, state.get()));
    }
  }

  public void runCallbacks()
  {
    if (!callbacksRun.compareAndSet(false, true)) {
      // Already run, skip
      return;
    }
    stop();
    RuntimeException e = null;
    for (Runnable callback = shutdownCallbacks.poll(); callback != null; callback = shutdownCallbacks.poll()) {
      try {
        callback.run();
      }
      catch (RuntimeException ex) {
        if (e == null) {
          e = new RuntimeException("Error running callback");
        }
        e.addSuppressed(ex);
      }
    }
    if (e != null) {
      throw e;
    }
  }

  @Override
  public boolean isStarted()
  {
    return State.STARTED.equals(getState());
  }

  @Override
  public boolean isStopped()
  {
    return State.STOPPED.equals(getState());
  }
}
