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

package io.druid.client.cache;

import io.druid.java.util.common.IAE;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract implementation of a BlockingQueue bounded by the size of elements,
 * works similar to LinkedBlockingQueue except that capacity is bounded by the size in bytes of the elements in the queue.
 */
public abstract class BytesBoundedLinkedQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>
{
  private final Queue<E> delegate;
  private final AtomicLong currentSize = new AtomicLong(0);
  private final Lock putLock = new ReentrantLock();
  private final Condition notFull = putLock.newCondition();
  private final Lock takeLock = new ReentrantLock();
  private final Condition notEmpty = takeLock.newCondition();
  private final AtomicInteger elementCount = new AtomicInteger(0);
  private long capacity;

  public BytesBoundedLinkedQueue(long capacity)
  {
    delegate = new ConcurrentLinkedQueue<>();
    this.capacity = capacity;
  }

  private static void checkNotNull(Object v)
  {
    if (v == null) {
      throw new NullPointerException();
    }
  }

  private void checkSize(E e)
  {
    if (getBytesSize(e) > capacity) {
      throw new IAE("cannot add element of size[%d] greater than capacity[%d]", getBytesSize(e), capacity);
    }
  }

  public abstract long getBytesSize(E e);

  public void elementAdded(E e)
  {
    currentSize.addAndGet(getBytesSize(e));
    elementCount.getAndIncrement();
  }

  public void elementRemoved(E e)
  {
    currentSize.addAndGet(-1 * getBytesSize(e));
    elementCount.getAndDecrement();
  }

  private void fullyUnlock()
  {
    takeLock.unlock();
    putLock.unlock();
  }

  private void fullyLock()
  {
    takeLock.lock();
    putLock.lock();
  }

  private void signalNotEmpty()
  {
    takeLock.lock();
    try {
      notEmpty.signal();
    }
    finally {
      takeLock.unlock();
    }
  }

  private void signalNotFull()
  {
    putLock.lock();
    try {
      notFull.signal();
    }
    finally {
      putLock.unlock();
    }
  }

  @Override
  public int size()
  {
    return elementCount.get();
  }

  @Override
  public void put(E e) throws InterruptedException
  {
    while (!offer(e, Long.MAX_VALUE, TimeUnit.NANOSECONDS)) {
      // keep trying until added successfully
    }
  }

  @Override
  public boolean offer(
      E e, long timeout, TimeUnit unit
  ) throws InterruptedException
  {
    checkNotNull(e);
    checkSize(e);
    long nanos = unit.toNanos(timeout);
    boolean added = false;
    putLock.lockInterruptibly();
    try {
      while (currentSize.get() + getBytesSize(e) > capacity) {
        if (nanos <= 0) {
          return false;
        }
        nanos = notFull.awaitNanos(nanos);
      }
      delegate.add(e);
      elementAdded(e);
      added = true;
    }
    finally {
      putLock.unlock();
    }
    if (added) {
      signalNotEmpty();
    }
    return added;

  }

  @Override
  public E take() throws InterruptedException
  {
    E e;
    takeLock.lockInterruptibly();
    try {
      while (elementCount.get() == 0) {
        notEmpty.await();
      }
      e = delegate.remove();
      elementRemoved(e);
    }
    finally {
      takeLock.unlock();
    }
    if (e != null) {
      signalNotFull();
    }
    return e;
  }

  @Override
  public int remainingCapacity()
  {
    int delegateSize;
    long currentByteSize;
    fullyLock();
    try {
      delegateSize = elementCount.get();
      currentByteSize = currentSize.get();
    }
    finally {
      fullyUnlock();
    }
    // return approximate remaining capacity based on current data
    if (delegateSize == 0) {
      return (int) Math.min(capacity, Integer.MAX_VALUE);
    } else if (capacity > currentByteSize) {
      long averageElementSize = currentByteSize / delegateSize;
      return (int) ((capacity - currentByteSize) / averageElementSize);
    } else {
      // queue full
      return 0;
    }

  }

  @Override
  public int drainTo(Collection<? super E> c)
  {
    return drainTo(c, Integer.MAX_VALUE);
  }

  @Override
  public int drainTo(Collection<? super E> c, int maxElements)
  {
    if (c == null) {
      throw new NullPointerException();
    }
    if (c == this) {
      throw new IllegalArgumentException();
    }
    int n = 0;
    takeLock.lock();
    try {
      // elementCount.get provides visibility to first n Nodes
      n = Math.min(maxElements, elementCount.get());
      if (n < 0) {
        return 0;
      }
      for (int i = 0; i < n; i++) {
        E e = delegate.remove();
        elementRemoved(e);
        c.add(e);
      }
    }
    finally {
      takeLock.unlock();
    }
    if (n > 0) {
      signalNotFull();
    }
    return n;
  }

  @Override
  public boolean offer(E e)
  {
    checkNotNull(e);
    checkSize(e);
    boolean added = false;
    putLock.lock();
    try {
      if (currentSize.get() + getBytesSize(e) > capacity) {
        return false;
      } else {
        added = delegate.add(e);
        if (added) {
          elementAdded(e);
        }
      }
    }
    finally {
      putLock.unlock();
    }
    if (added) {
      signalNotEmpty();
    }
    return added;
  }

  @Override
  public E poll()
  {
    E e = null;
    takeLock.lock();
    try {
      e = delegate.poll();
      if (e != null) {
        elementRemoved(e);
      }
    }
    finally {
      takeLock.unlock();
    }
    if (e != null) {
      signalNotFull();
    }
    return e;
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException
  {
    long nanos = unit.toNanos(timeout);
    E e = null;
    takeLock.lockInterruptibly();
    try {
      while (elementCount.get() == 0) {
        if (nanos <= 0) {
          return null;
        }
        nanos = notEmpty.awaitNanos(nanos);
      }
      e = delegate.poll();
      if (e != null) {
        elementRemoved(e);
      }
    }
    finally {
      takeLock.unlock();
    }
    if (e != null) {
      signalNotFull();
    }
    return e;

  }

  @Override
  public E peek()
  {
    takeLock.lock();
    try {
      return delegate.peek();
    }
    finally {
      takeLock.unlock();
    }
  }

  @Override
  public Iterator<E> iterator()
  {
    return new Itr(delegate.iterator());
  }

  private class Itr implements Iterator<E>
  {

    private final Iterator<E> delegate;
    private E lastReturned;

    Itr(Iterator<E> delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public boolean hasNext()
    {
      fullyLock();
      try {
        return delegate.hasNext();
      }
      finally {
        fullyUnlock();
      }
    }

    @Override
    public E next()
    {
      fullyLock();
      try {
        this.lastReturned = delegate.next();
        return lastReturned;
      }
      finally {
        fullyUnlock();
      }
    }

    @Override
    public void remove()
    {
      fullyLock();
      try {
        if (this.lastReturned == null) {
          throw new IllegalStateException();
        }
        delegate.remove();
        elementRemoved(lastReturned);
        signalNotFull();
        lastReturned = null;
      }
      finally {
        fullyUnlock();
      }
    }
  }
}
