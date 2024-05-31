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
 * <p>
 * The class performs the work of a "bouncer", limiting the number of threads that can concurrently access the guarded
 * critical sections by the bouncer. The bouncer is initialized with the max number of threads that can enter the
 * gaurded section(s) at a time.
 * <p>
 * The entering thread must ask for a {@link #ticket()} from the bouncer. The bouncer provides a future that resolves
 * with the ticket when it becomes available:
 * a. If the bouncer already has tickets to spare, the future resolves immediately
 * b. If the bouncer doesn't have a ticket, the thread enters a queue (of threads) waiting for a ticket. When one of the
 * threads holding a ticket give it back ({@link Ticket#giveBack()}), the bouncer hands out that ticket to the waiting
 * threads in a first-cum-first-serve fashion (and removes that thread from the queue). The future resolves when the thread
 * requesting the ticket is first in the queue, and the bouncer gets a ticket back from the previous holders.
 * <p>
 * This class is designed to be used by {@link FrameProcessorExecutor#runAllFully} to limit the number of outstanding processors.
 * The callback's to the ticket's future handed out by the bouncer execute within the bouncer's lock, which can deteriorate the
 * performance if the callback is computationally intensive. This class's design must be assessed before using it for
 * any other purpose.
 */
public class Bouncer
{
  /**
   * Maximum number of tickets the bouncer can hand out at a time
   */
  private final int maxCount;

  /**
   * Lock for synchronizing bouncer's methods
   */
  private final Object lock = new Object();

  /**
   * Number of tickets handed out by the bouncer at a given time
   */
  @GuardedBy("lock")
  private int currentCount = 0;

  /**
   * Pending futures handed out to the threads requesting a ticket.
   */
  @GuardedBy("lock")
  private final Queue<SettableFuture<Ticket>> waiters = new ArrayDeque<>();

  public Bouncer(final int maxCount)
  {
    this.maxCount = maxCount;

    if (maxCount <= 0) {
      throw new ISE("maxConcurrentWorkers must be greater than zero");
    }
  }

  /**
   * Returns a bouncer with unlimited tickets to spare
   */
  public static Bouncer unlimited()
  {
    return new Bouncer(Integer.MAX_VALUE);
  }

  /**
   * Maximum number of tickets the bouncer can hand out at a time
   */
  public int getMaxCount()
  {
    return maxCount;
  }

  /**
   * Request a ticket from the bouncer. Returns a future that resolves when the bouncer has a ticket to spare and the
   * requester is the first in the queue of the pending futures.
   */
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

  /**
   * Handed out by the bouncer once the thread can enter the guarded section
   */
  public class Ticket
  {
    private final AtomicBoolean givenBack = new AtomicBoolean();

    /**
     * Gives back the ticket to the bouncer. Usually called once the calling thread completes the actions in the
     * critical section
     */
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

          if (nextFuture.set(new Ticket())) {
            return;
          }
        }
      }
    }
  }
}
