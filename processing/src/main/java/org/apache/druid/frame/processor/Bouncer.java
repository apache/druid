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
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
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
  @Nullable
  private final Bouncer parentBouncer;
  private final int maxCount;

  private final Object lock = new Object();

  @GuardedBy("lock")
  private int currentCount = 0;

  @GuardedBy("lock")
  private final Queue<SettableFuture<Ticket>> waiters = new ArrayDeque<>();

  /**
   * Create a Bouncer that issues a limited number of tickets.
   */
  public Bouncer(final int maxCount)
  {
    this.parentBouncer = null;
    this.maxCount = maxCount;

    if (maxCount <= 0) {
      throw new ISE("maxConcurrentWorkers must be greater than zero");
    }
  }

  /**
   * Create a Bouncer that issues a limited number of tickets from a parent.
   */
  public Bouncer(final int maxCount, @Nullable final Bouncer parentBouncer)
  {
    this.parentBouncer = parentBouncer;
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
    return parentBouncer == null ? maxCount : Math.min(parentBouncer.getMaxCount(), maxCount);
  }

  public ListenableFuture<Ticket> ticket()
  {
    // Acquire parent ticket first, if there's a parent.
    if (parentBouncer != null) {
      return FutureUtils.transformAsync(parentBouncer.ticket(), this::ticketInternal);
    } else {
      return ticketInternal(null);
    }
  }

  /**
   * Acquire a ticket from this Bouncer. Precondition: if there is a parentBouncer, only call this method when
   * holding a parent ticket.
   */
  private ListenableFuture<Ticket> ticketInternal(@Nullable final Ticket parentTicket)
  {
    synchronized (lock) {
      if (currentCount < maxCount) {
        currentCount++;
        return Futures.immediateFuture(new Ticket(parentTicket));
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
    @Nullable
    private final Ticket parentTicket;
    private final AtomicBoolean givenBack = new AtomicBoolean();

    public Ticket(@Nullable Ticket parentTicket)
    {
      this.parentTicket = parentTicket;
    }

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
            break;
          }
        }

        if (nextFuture.set(new Ticket(parentTicket))) {
          return;
        }
      }

      // Dispose of the parent ticket, if we have one.
      if (parentTicket != null) {
        parentTicket.giveBack();
      }
    }
  }
}
