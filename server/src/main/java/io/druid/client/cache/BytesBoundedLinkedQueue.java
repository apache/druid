/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client.cache;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract implementation of a BlockingQueue bounded by the size of elements,
 * works similar to LinkedBlockingQueue except that capacity is bounded by the size in bytes of the elements in the queue.
 */
public abstract class BytesBoundedLinkedQueue<E> extends AbstractQueue<E> implements BlockingQueue<E>
{
  private LinkedList<E> delegate;
  private int capacity;
  private int currentSize;
  private Lock lock = new ReentrantLock();
  private Condition notFull = lock.newCondition();
  private Condition notEmpty = lock.newCondition();

  public BytesBoundedLinkedQueue(int capacity)
  {
    delegate = new LinkedList<>();
    this.capacity = capacity;
  }

  private static void checkNotNull(Object v)
  {
    if (v == null) {
      throw new NullPointerException();
    }
  }

  public abstract long getBytesSize(E e);

  public void operationAdded(E e)
  {
    currentSize += getBytesSize(e);
    notEmpty.signalAll();
  }

  public void operationRemoved(E e)
  {
    currentSize -= getBytesSize(e);
    notFull.signalAll();
  }

  @Override
  public int size()
  {
    lock.lock();
    try {
      return delegate.size();
    }
    finally {
      lock.unlock();
    }
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
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();
    try {
      while (currentSize > capacity) {
        if (nanos <= 0) {
          return false;
        }
        nanos = notFull.awaitNanos(nanos);
      }
      delegate.add(e);
      operationAdded(e);
      return true;
    }
    finally {
      lock.unlock();
    }

  }

  @Override
  public E take() throws InterruptedException
  {
    lock.lockInterruptibly();
    try {
      while (delegate.size() == 0) {
        notEmpty.await();
      }
      E e = delegate.remove();
      operationRemoved(e);
      return e;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public int remainingCapacity()
  {
    lock.lock();
    try {
      // return approximate remaining capacity based on current data
      if (delegate.size() == 0) {
        return capacity;
      } else {
        int averageByteSize = currentSize / delegate.size();
        return (capacity - currentSize) / averageByteSize;
      }
    }
    finally {
      lock.unlock();
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
    lock.lock();
    try {
      int n = Math.min(maxElements, delegate.size());
      if (n < 0) {
        return 0;
      }
      // count.get provides visibility to first n Nodes
      for (int i = 0; i < n; i++) {
        E e = delegate.remove(0);
        operationRemoved(e);
        c.add(e);
      }
      return n;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public boolean offer(E e)
  {
    checkNotNull(e);
    lock.lock();
    try {
      if (currentSize > capacity) {
        return false;
      } else {
        boolean added = delegate.add(e);
        if (added) {
          operationAdded(e);
        }
        return added;
      }
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public E poll()
  {
    lock.lock();
    try {
      E e = delegate.poll();
      if (e != null) {
        operationRemoved(e);
      }
      return e;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public E poll(long timeout, TimeUnit unit) throws InterruptedException
  {
    long nanos = unit.toNanos(timeout);
    lock.lockInterruptibly();
    try {
      while (delegate.size() == 0) {
        if (nanos <= 0) {
          return null;
        }
        nanos = notEmpty.awaitNanos(nanos);
      }
      return delegate.poll();
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public E peek()
  {
    lock.lock();
    try {
      return delegate.peek();
    }
    finally {
      lock.unlock();
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
      lock.lock();
      try {
        return delegate.hasNext();
      }
      finally {
        lock.unlock();
      }
    }

    @Override
    public E next()
    {
      lock.lock();
      try {
        this.lastReturned = delegate.next();
        return lastReturned;
      }
      finally {
        lock.unlock();
      }
    }

    @Override
    public void remove()
    {
      lock.lock();
      try {
        if (this.lastReturned == null) {
          throw new IllegalStateException();
        }
        delegate.remove();
        operationRemoved(lastReturned);
        lastReturned = null;
      }
      finally {
        lock.unlock();
      }
    }
  }
}