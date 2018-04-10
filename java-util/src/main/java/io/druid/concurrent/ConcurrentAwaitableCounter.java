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
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedLongSynchronizer;

/**
 * This synchronization object allows to {@link #increment} a counter without blocking, potentially from multiple
 * threads (although in some use cases there is just one incrementer thread), and block in other thread(s), awaiting
 * when the count reaches the provided value: see {@link #awaitCount}, or the specified number of events since the
 * call: see {@link #awaitNextIncrements}.
 *
 * This counter wraps around {@link Long#MAX_VALUE} and starts from 0 again, so "next" count should be generally
 * obtained by calling {@link #nextCount nextCount(currentCount)} rather than {@code currentCount + 1}.
 *
 * Memory consistency effects: actions in threads prior to calling {@link #increment} while the count was less than the
 * awaited value happen-before actions following count awaiting methods such as {@link #awaitCount}.
 */
public final class ConcurrentAwaitableCounter
{
  private static final long MAX_COUNT = Long.MAX_VALUE;

  /**
   * This method should be called to obtain the next total increment count to be passed to {@link #awaitCount} methods,
   * instead of just adding 1 to the previous count, because the count must wrap around {@link Long#MAX_VALUE} and start
   * from 0 again.
   */
  public static long nextCount(long prevCount)
  {
    return (prevCount + 1) & MAX_COUNT;
  }

  private static class Sync extends AbstractQueuedLongSynchronizer
  {
    @Override
    protected long tryAcquireShared(long countWhenWaitStarted)
    {
      long currentCount = getState();
      return compareCounts(currentCount, countWhenWaitStarted) > 0 ? 1 : -1;
    }

    @Override
    protected boolean tryReleaseShared(long increment)
    {
      long count;
      long nextCount;
      do {
        count = getState();
        nextCount = (count + increment) & MAX_COUNT;
      } while (!compareAndSetState(count, nextCount));
      return true;
    }

    long getCount()
    {
      return getState();
    }
  }

  private final Sync sync = new Sync();

  /**
   * Increment the count. This method could be safely called from concurrent threads.
   */
  public void increment()
  {
    sync.releaseShared(1);
  }

  /**
   * Await until the {@link #increment} is called on this counter object the specified number of times from the creation
   * of this counter object.
   */
  public void awaitCount(long totalCount) throws InterruptedException
  {
    checkTotalCount(totalCount);
    long currentCount = sync.getCount();
    while (compareCounts(totalCount, currentCount) > 0) {
      sync.acquireSharedInterruptibly(currentCount);
      currentCount = sync.getCount();
    }
  }

  private static void checkTotalCount(long totalCount)
  {
    if (totalCount < 0) {
      throw new AssertionError(
          "Total count must always be >= 0, even in the face of overflow. "
          + "The next count should always be obtained by calling ConcurrentAwaitableCounter.nextCount(prevCount), "
          + "not just +1"
      );
    }
  }

  /**
   * Await until the {@link #increment} is called on this counter object the specified number of times from the creation
   * of this counter object, for not longer than the specified period of time. If by this time the target increment
   * count is not reached, {@link TimeoutException} is thrown.
   */
  public void awaitCount(long totalCount, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
  {
    checkTotalCount(totalCount);
    long nanos = unit.toNanos(timeout);
    long currentCount = sync.getCount();
    while (compareCounts(totalCount, currentCount) > 0) {
      if (!sync.tryAcquireSharedNanos(currentCount, nanos)) {
        throw new TimeoutException();
      }
      currentCount = sync.getCount();
    }
  }

  private static int compareCounts(long count1, long count2)
  {
    long diff = (count1 - count2) & MAX_COUNT;
    if (diff == 0) {
      return 0;
    }
    return diff < MAX_COUNT / 2 ? 1 : -1;
  }

  /**
   * Somewhat loosely defined wait for "next N increments", because the starting point is not defined from the Java
   * Memory Model perspective.
   */
  public void awaitNextIncrements(long nextIncrements) throws InterruptedException
  {
    if (nextIncrements <= 0) {
      throw new IllegalArgumentException("nextIncrements is not positive: " + nextIncrements);
    }
    if (nextIncrements > MAX_COUNT / 4) {
      throw new UnsupportedOperationException("Couldn't wait for so many increments: " + nextIncrements);
    }
    awaitCount((sync.getCount() + nextIncrements) & MAX_COUNT);
  }

  /**
   * The difference between this method and {@link #awaitCount(long, long, TimeUnit)} with argument 1 is that {@code
   * awaitFirstIncrement()} returns boolean designating whether the count was await (while waiting for no longer than
   * for the specified period of time), while {@code awaitCount()} throws {@link TimeoutException} if the count was not
   * awaited.
   */
  public boolean awaitFirstIncrement(long timeout, TimeUnit unit) throws InterruptedException
  {
    return sync.tryAcquireSharedNanos(0, unit.toNanos(timeout));
  }
}
