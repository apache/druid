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

public final class ConcurrentAwaitableCounter
{
  private static final long MAX_COUNT = Long.MAX_VALUE;

  public static long nextCount(long prevCount)
  {
    return (prevCount + 1) & MAX_COUNT;
  }

  private static class Sync extends AbstractQueuedLongSynchronizer
  {
    @Override
    protected long tryAcquireShared(long awaitedCount)
    {
      long currentCount = getState();
      return compareCounts(currentCount, awaitedCount) > 0 ? 1 : -1;
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

  public void increment()
  {
    sync.releaseShared(1);
  }

  public void awaitCount(long totalCount) throws InterruptedException
  {
    totalCount &= MAX_COUNT;
    long currentCount;
    currentCount = sync.getCount();
    while (compareCounts(totalCount, currentCount) > 0) {
      sync.acquireSharedInterruptibly(currentCount);
      currentCount = sync.getCount();
    }
  }

  public void awaitCount(long totalCount, long timeout, TimeUnit unit) throws InterruptedException, TimeoutException
  {
    long nanos = unit.toNanos(timeout);
    totalCount &= MAX_COUNT;
    long currentCount;
    currentCount = sync.getCount();
    while (compareCounts(totalCount, currentCount) > 0) {
      if (!sync.tryAcquireNanos(currentCount, nanos)) {
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

  public void awaitNextIncrements(long nextIncrements) throws InterruptedException
  {
    if (nextIncrements <= 0) {
      throw new IllegalArgumentException("nextIncrements is not positive: " + nextIncrements);
    }
    if (nextIncrements > MAX_COUNT / 4) {
      throw new UnsupportedOperationException("Couldn't wait for so many increments: " + nextIncrements);
    }
    awaitCount(sync.getCount() + nextIncrements);
  }

  public boolean awaitFirstIncrement(long timeout, TimeUnit unit) throws InterruptedException
  {
    return sync.tryAcquireSharedNanos(0, unit.toNanos(timeout));
  }
}
