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

package org.apache.druid.java.util.common;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

import java.util.Collection;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/**
 * Similar to LinkedBlockingQueue but can be bounded by the total byte size of the items present in the queue
 * rather than number of items.
 */
public class MemoryBoundLinkedBlockingQueue<T>
{
  private final long memoryBound;
  private final AtomicLong currentMemory;
  private final LinkedBlockingQueue<ObjectContainer<T>> queue;
  private final ReentrantLock putLock = new ReentrantLock();
  private final Condition notFull = putLock.newCondition();

  public MemoryBoundLinkedBlockingQueue(long memoryBound)
  {
    this(new LinkedBlockingQueue<>(), memoryBound);
  }

  @VisibleForTesting
  MemoryBoundLinkedBlockingQueue(LinkedBlockingQueue<ObjectContainer<T>> queue, long memoryBound)
  {
    this.memoryBound = memoryBound;
    this.currentMemory = new AtomicLong(0L);
    this.queue = queue;
  }

  // returns true/false depending on whether item was added or not
  public boolean offer(ObjectContainer<T> item)
  {
    final long itemLength = item.getSize();

    if (currentMemory.addAndGet(itemLength) <= memoryBound) {
      if (queue.offer(item)) {
        return true;
      }
    }
    currentMemory.addAndGet(-itemLength);
    return false;
  }

  public boolean offer(ObjectContainer<T> item, long timeout, TimeUnit unit) throws InterruptedException
  {
    final long itemLength = item.getSize();

    long nanos = unit.toNanos(timeout);
    final ReentrantLock putLock = this.putLock;
    putLock.lockInterruptibly();
    try {
      while (currentMemory.get() + itemLength > memoryBound) {
        if (nanos <= 0L) {
          return false;
        }
        nanos = notFull.awaitNanos(nanos);
      }
      if (currentMemory.addAndGet(itemLength) <= memoryBound) {
        if (queue.offer(item, timeout, unit)) {
          return true;
        }
      }
    }
    catch (InterruptedException e) {
      currentMemory.addAndGet(-itemLength);
      throw e;
    }
    finally {
      putLock.unlock();
    }
    currentMemory.addAndGet(-itemLength);
    return false;
  }

  // blocks until at least one item is available to take
  public ObjectContainer<T> take() throws InterruptedException
  {
    final ObjectContainer<T> ret = queue.take();
    currentMemory.addAndGet(-ret.getSize());
    signalNotFull();
    return ret;
  }

  public Stream<ObjectContainer<T>> stream()
  {
    return queue.stream();
  }

  /**
   * Drain up to specified bytes worth of items from the queue into the provided buffer. At least one record is
   * drained from the queue, regardless of the value of bytes specified.
   *
   * @param buffer       The buffer to drain queue items into.
   * @param bytesToDrain The amount of bytes to drain from the queue
   * @param timeout      The maximum time allowed to drain the queue
   * @param unit         The time unit of the timeout.
   *
   * @return The number of items drained from the queue.
   * @throws InterruptedException
   */
  public int drain(Collection<? super ObjectContainer<T>> buffer, int bytesToDrain, long timeout, TimeUnit unit)
      throws InterruptedException
  {
    Preconditions.checkNotNull(buffer);
    boolean signalNotFull = false;
    try {
      long deadline = System.nanoTime() + unit.toNanos(timeout);
      int added = 0;
      long bytesAdded = 0;
      while (bytesAdded < bytesToDrain) {
        ObjectContainer<T> e = queue.poll(deadline - System.nanoTime(), TimeUnit.NANOSECONDS);
        if (e == null) {
          break;
        }
        currentMemory.addAndGet(-e.getSize());
        signalNotFull = true;
        buffer.add(e);
        ++added;
        bytesAdded += e.getSize();
        e = queue.peek();
        if (e != null && (bytesAdded + e.getSize()) > bytesToDrain) {
          break;
        }
      }
      return added;
    }
    finally {
      if (signalNotFull) {
        signalNotFull();
      }
    }
  }

  public int size()
  {
    return queue.size();
  }

  public long byteSize()
  {
    return currentMemory.get();
  }

  public long remainingCapacity()
  {
    return memoryBound - currentMemory.get();
  }

  private void signalNotFull()
  {
    final ReentrantLock putLock = this.putLock;
    putLock.lock();
    try {
      notFull.signal();
    }
    finally {
      putLock.unlock();
    }
  }

  public static class ObjectContainer<T>
  {
    private final T data;
    private final long size;

    public ObjectContainer(T data, long size)
    {
      this.data = data;
      this.size = size;
    }

    public T getData()
    {
      return data;
    }

    public long getSize()
    {
      return size;
    }
  }
}

