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

package io.druid.collections;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import io.druid.java.util.common.ISE;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Pool that pre-generates objects up to a limit, then permits possibly-blocking "take" operations.
 */
public class DefaultBlockingPool<T> implements BlockingPool<T>
{
  private static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;

  private final ArrayDeque<T> objects;
  private final ReentrantLock lock;
  private final Condition notEnough;
  private final int maxSize;

  public DefaultBlockingPool(
      Supplier<T> generator,
      int limit
  )
  {
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

  @Override
  public ReferenceCountingResourceHolder<T> take(final long timeoutMs)
  {
    Preconditions.checkArgument(timeoutMs >= 0, "timeoutMs must be a non-negative value, but was [%s]", timeoutMs);
    checkInitialized();
    try {
      return wrapObject(timeoutMs > 0 ? pollObject(timeoutMs) : pollObject());
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ReferenceCountingResourceHolder<T> take()
  {
    checkInitialized();
    try {
      return wrapObject(takeObject());
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  private ReferenceCountingResourceHolder<T> wrapObject(T theObject)
  {
    return theObject == null ? null : new ReferenceCountingResourceHolder<>(
        theObject,
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            offer(theObject);
          }
        }
    );
  }

  private T pollObject()
  {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      return objects.isEmpty() ? null : objects.pop();
    }
    finally {
      lock.unlock();
    }
  }

  private T pollObject(long timeoutMs) throws InterruptedException
  {
    long nanos = TIME_UNIT.toNanos(timeoutMs);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (objects.isEmpty()) {
        if (nanos <= 0) {
          return null;
        }
        nanos = notEnough.awaitNanos(nanos);
      }
      return objects.pop();
    }
    finally {
      lock.unlock();
    }
  }

  private T takeObject() throws InterruptedException
  {
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (objects.isEmpty()) {
        notEnough.await();
      }
      return objects.pop();
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public ReferenceCountingResourceHolder<List<T>> takeBatch(final int elementNum, final long timeoutMs)
  {
    Preconditions.checkArgument(timeoutMs >= 0, "timeoutMs must be a non-negative value, but was [%s]", timeoutMs);
    checkInitialized();
    try {
      return wrapObjects(timeoutMs > 0 ? pollObjects(elementNum, timeoutMs) : pollObjects(elementNum));
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public ReferenceCountingResourceHolder<List<T>> takeBatch(final int elementNum)
  {
    checkInitialized();
    try {
      return wrapObjects(takeObjects(elementNum));
    }
    catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }
  }

  private ReferenceCountingResourceHolder<List<T>> wrapObjects(List<T> theObjects)
  {
    return theObjects == null ? null : new ReferenceCountingResourceHolder<>(
        theObjects,
        new Closeable()
        {
          @Override
          public void close() throws IOException
          {
            offerBatch(theObjects);
          }
        }
    );
  }

  private List<T> pollObjects(int elementNum) throws InterruptedException
  {
    final List<T> list = Lists.newArrayListWithCapacity(elementNum);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      if (objects.size() < elementNum) {
        return null;
      } else {
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

  private List<T> pollObjects(int elementNum, long timeoutMs) throws InterruptedException
  {
    long nanos = TIME_UNIT.toNanos(timeoutMs);
    final List<T> list = Lists.newArrayListWithCapacity(elementNum);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (objects.size() < elementNum) {
        if (nanos <= 0) {
          return null;
        }
        nanos = notEnough.awaitNanos(nanos);
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

  private List<T> takeObjects(int elementNum) throws InterruptedException
  {
    final List<T> list = Lists.newArrayListWithCapacity(elementNum);
    final ReentrantLock lock = this.lock;
    lock.lockInterruptibly();
    try {
      while (objects.size() < elementNum) {
        notEnough.await();
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

  private void checkInitialized()
  {
    Preconditions.checkState(maxSize > 0, "Pool was initialized with limit = 0, there are no objects to take.");
  }

  private void offer(T theObject)
  {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (objects.size() < maxSize) {
        objects.push(theObject);
        notEnough.signal();
      } else {
        throw new ISE("Cannot exceed pre-configured maximum size");
      }
    }
    finally {
      lock.unlock();
    }
  }

  private void offerBatch(List<T> offers)
  {
    final ReentrantLock lock = this.lock;
    lock.lock();
    try {
      if (objects.size() + offers.size() <= maxSize) {
        for (T offer : offers) {
          objects.push(offer);
        }
        notEnough.signal();
      } else {
        throw new ISE("Cannot exceed pre-configured maximum size");
      }
    }
    finally {
      lock.unlock();
    }
  }
}
