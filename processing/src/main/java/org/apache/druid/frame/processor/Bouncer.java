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

package org.apache.druid.frame.processor;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Limiter for access to some resource.
 *
 * Used by {@link FrameProcessorExecutor#runAllFully} to limit the number of outstanding processors.
 */
public class Bouncer
{
  private final int maxCount;

  private final Object lock = new Object();

  @GuardedBy("lock")
  private int currentCount = 0;

  @GuardedBy("lock")
  private final Queue<SettableFuture<Ticket>> waiters = new ArrayDeque<>();

  public Bouncer(final int maxCount)
  {
    this.maxCount = maxCount;

    if (maxCount <= 0) {
      throw new ISE("maxConcurrentWorkers must be greater than zero");
    }
  }

  public static Bouncer unlimited()
  {
    return new Bouncer(Integer.MAX_VALUE);
  }

  public int getMaxCount()
  {
    return maxCount;
  }

  public ListenableFuture<Ticket> ticket()
  {
    synchronized (lock) {
      if (currentCount < maxCount) {
        currentCount++;
        //noinspection UnstableApiUsage
        return Futures.immediateFuture(new Ticket());
      } else {
        final SettableFuture<Ticket> future = SettableFuture.create();
        waiters.add(future);
        return future;
      }
    }
  }

  @VisibleForTesting
  int getCurrentCount()
  {
    synchronized (lock) {
      return currentCount;
    }
  }

  public class Ticket
  {
    private final AtomicBoolean givenBack = new AtomicBoolean();

    public void giveBack()
    {
      if (!givenBack.compareAndSet(false, true)) {
        return;
      }

      // Loop to find a new home for this ticket that isn't canceled.

      while (true) {
        final SettableFuture<Ticket> nextFuture;

        synchronized (lock) {
          nextFuture = waiters.poll();

          if (nextFuture == null) {
            // Nobody was waiting.
            currentCount--;
            return;
          }
        }

        if (nextFuture.set(new Ticket())) {
          return;
        }
      }
    }
  }
}
