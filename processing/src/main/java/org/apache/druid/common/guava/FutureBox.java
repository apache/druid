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

package org.apache.druid.common.guava;

import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.concurrent.Execs;

import java.io.Closeable;
import java.util.Set;

/**
 * Box for tracking pending futures. Allows cancellation of all pending futures.
 */
public class FutureBox implements Closeable
{
  /**
   * Currently-outstanding futures. These are tracked so they can be canceled in {@link #close()}.
   */
  private final Set<ListenableFuture<?>> pendingFutures = Sets.newConcurrentHashSet();

  private volatile boolean stopped;

  /**
   * Returns the number of currently-pending futures.
   */
  public int pendingCount()
  {
    return pendingFutures.size();
  }

  /**
   * Adds a future to the box.
   * If {@link #close()} had previously been called, the future is immediately canceled.
   */
  public <R> ListenableFuture<R> register(final ListenableFuture<R> future)
  {
    pendingFutures.add(future);
    future.addListener(() -> pendingFutures.remove(future), Execs.directExecutor());

    // If "stop" was called while we were creating this future, cancel it prior to returning it.
    if (stopped) {
      future.cancel(false);
    }

    return future;
  }

  /**
   * Closes the box, canceling all currently-pending futures.
   */
  @Override
  public void close()
  {
    stopped = true;
    for (ListenableFuture<?> future : pendingFutures) {
      future.cancel(false); // Ignore return value
    }
  }
}
