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

package io.druid.emitter.kafka;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Similar to LinkedBlockingQueue but can be bounded by the total byte size of the items present in the queue
 * rather than number of items.
 */
public class MemoryBoundLinkedBlockingQueue<T>
{
  private final long memoryBound;
  private final AtomicLong currentMemory;
  private final LinkedBlockingQueue<ObjectContainer<T>> queue;

  public MemoryBoundLinkedBlockingQueue(long memoryBound)
  {
    this.memoryBound = memoryBound;
    this.currentMemory = new AtomicLong(0L);
    this.queue = new LinkedBlockingQueue<>();
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

  // blocks until at least one item is available to take
  public ObjectContainer<T> take() throws InterruptedException
  {
    final ObjectContainer<T> ret = queue.take();
    currentMemory.addAndGet(-ret.getSize());
    return ret;
  }

  public long getAvailableBuffer()
  {
    return memoryBound - currentMemory.get();
  }

  public int size()
  {
    return queue.size();
  }

  public static class ObjectContainer<T>
  {
    private T data;
    private long size;

    ObjectContainer(T data, long size)
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
