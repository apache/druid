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

package org.apache.druid.collections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * Pool that pre-generates objects up to a limit, then permits possibly-blocking "take" operations.
 */
public class DefaultBlockingPool<T> implements BlockingPool<T>
{
  private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  @VisibleForTesting
  final ArrayDeque<T> objects;

  private final ReentrantLock lock;
  private final Condition notEnough;
  private final int maxSize;
  private final ResourceGroupScheduler resourceGroupScheduler;

  public DefaultBlockingPool(
      Supplier<T> generator,
      int limit
  )
  {
    this(new SemaphoreResourceGroupScheduler(limit, ImmutableMap.of(), true), generator);
  }

  public DefaultBlockingPool(
      ResourceGroupScheduler resourceGroupScheduler,
      Supplier<T> generator
  )
  {
    int limit = resourceGroupScheduler.getTotalCapacity();
    this.resourceGroupScheduler = resourceGroupScheduler;
    this.objects = new ArrayDeque<>(limit);
    this.maxSize = limit;

    for (int i = 0; i < limit; i++) {
      objects.add(generator.get());
    }

    this.lock = new ReentrantLock();
    this.notEnough = lock.newCondition();
  }

  @Override
  public int maxSize()
  {
    return maxSize;
  }

  @VisibleForTesting
  public int getPoolSize()
  {
    return objects.size();
  }

  @Nullable
  private ReferenceCountingResourceHolder<T> wrapObject(String group, T theObject)
  {
    return theObject == null ? null : new ReferenceCountingResourceHolder<>(
        theObject,
        () -> offer(group, theObject)
    );
  }

  @Override
  public List<ReferenceCountingResourceHolder<T>> takeBatch(final int elementNum, final long timeoutMs)
  {
    return takeBatch(null, elementNum, timeoutMs);
  }

  @Override
  public List<ReferenceCountingResourceHolder<T>> takeBatch(String group, int elementNum, long timeoutMs)
  {
    Preconditions.checkArgument(timeoutMs >= 0, "timeoutMs must be a non-negative value, but was [%s]", timeoutMs);
    checkInitialized();
    try {
      final List<T> objects = timeoutMs > 0
                              ? pollObjects(group, elementNum, timeoutMs)
                              : pollObjects(group, elementNum);
      return objects.stream().map(obj -> wrapObject(group, obj)).collect(Collectors.toList());
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public List<ReferenceCountingResourceHolder<T>> takeBatch(final int elementNum)
  {
    return takeBatch(null, elementNum);
  }

  @Override
  public List<ReferenceCountingResourceHolder<T>> takeBatch(String group, int elementNum)
  {
    checkInitialized();
    try {
      return takeObjects(group, elementNum).stream().map(obj -> wrapObject(group, obj)).collect(Collectors.toList());
    }
    catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  private List<T> pollObjects(@Nullable String group, int elementNum) throws InterruptedException
  {
    final List<T> list = new ArrayList<>(elementNum);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      if (isGroupResourceUnavaiable(group, elementNum)) {
        return Collections.emptyList();
      } else {
        if (!resourceGroupScheduler.tryAcquire(group, elementNum)) {
          return Collections.emptyList();
        }
        for (int i = 0; i < elementNum; i++) {
          list.add(objects.pop());
        }
        return list;
      }
    }
    finally {
      lock.unlock();
    }
  }

  private boolean isGroupResourceUnavaiable(String group, int elementNum)
  {
    if (objects.size() < elementNum) {
      return true;
    }
    return resourceGroupScheduler.getGroupAvailableCapacity(group) < elementNum;
  }

  private List<T> pollObjects(@Nullable String group, int elementNum, long timeoutMs)
      throws InterruptedException
  {
    long nanos = TIME_UNIT.toNanos(timeoutMs);
    final List<T> list = new ArrayList<>(elementNum);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (isGroupResourceUnavaiable(group, elementNum)) {
        if (nanos <= 0) {
          return Collections.emptyList();
        }
        nanos = notEnough.awaitNanos(nanos);
      }
      if (!resourceGroupScheduler.tryAcquire(group, elementNum, nanos, TimeUnit.NANOSECONDS)) {
        return Collections.emptyList();
      }
      for (int i = 0; i < elementNum; i++) {
        list.add(objects.pop());
      }
      return list;
    }
    finally {
      lock.unlock();
    }
  }

  private List<T> takeObjects(@Nullable String group, int elementNum) throws InterruptedException
  {
    final List<T> list = new ArrayList<>(elementNum);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (isGroupResourceUnavaiable(group, elementNum)) {
        notEnough.await();
      }
      resourceGroupScheduler.accquire(group, elementNum);
      for (int i = 0; i < elementNum; i++) {
        list.add(objects.pop());
      }
      return list;
    }
    finally {
      lock.unlock();
    }
  }

  private void checkInitialized()
  {
    Preconditions.checkState(maxSize > 0, "Pool was initialized with limit = 0, there are no objects to take.");
  }

  private void offer(String group, T theObject)
  {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (objects.size() < maxSize) {
        objects.push(theObject);
        resourceGroupScheduler.release(group);
        notEnough.signalAll();
      } else {
        resourceGroupScheduler.release(group);
        throw new ISE("Cannot exceed pre-configured maximum size");
      }
    }
    finally {
      lock.unlock();
    }
  }
}
