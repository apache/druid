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

import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;

import io.druid.java.util.common.logger.Logger;

import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple load balancing pool that always returns the least used item.
 *
 * An item's usage is incremented every time one gets requested from the pool
 * and is decremented every time close is called on the holder.
 *
 * The pool eagerly instantiates all the items in the pool when created,
 * using the given supplier.
 *
 * @param <T> type of items to pool
 */
public class LoadBalancingPool<T> implements Supplier<ResourceHolder<T>>
{
  private static final Logger log = new Logger(LoadBalancingPool.class);

  private final Supplier<T> generator;
  private final int capacity;
  private final PriorityBlockingQueue<CountingHolder> queue;

  public LoadBalancingPool(int capacity, Supplier<T> generator)
  {
    Preconditions.checkArgument(capacity > 0, "capacity must be greater than 0");
    Preconditions.checkNotNull(generator);

    this.generator = generator;
    this.capacity = capacity;
    this.queue = new PriorityBlockingQueue<>(capacity);

    // eagerly intantiate all items in the pool
    init();
  }

  private void init() {
    for(int i = 0; i < capacity; ++i) {
      queue.offer(new CountingHolder(generator.get()));
    }
  }

  public ResourceHolder<T> get()
  {
    final CountingHolder holder;
    // items never stay out of the queue for long, so we'll get one eventually
    try {
      holder = queue.take();
    } catch(InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }

    // synchronize on item to ensure count cannot get changed by
    // CountingHolder.close right after we put the item back in the queue
    synchronized (holder) {
      holder.count.incrementAndGet();
      queue.offer(holder);
    }
    return holder;
  }

  private class CountingHolder implements ResourceHolder<T>, Comparable<CountingHolder>
  {
    private AtomicInteger count = new AtomicInteger(0);
    private final T object;

    public CountingHolder(final T object)
    {
      this.object = object;
    }

    @Override
    public T get()
    {
      return object;
    }

    /**
     * Not idempotent, should only be called once when done using the resource
     */
    @Override
    public void close()
    {
      // ensures count always gets adjusted while item is removed from the queue
      synchronized (this) {
        // item may not be in queue if another thread is calling LoadBalancingPool.get()
        // at the same time; in that case let the other thread put it back.
        boolean removed = queue.remove(this);
        count.decrementAndGet();
        if (removed) {
          queue.offer(this);
        }
      }
    }

    @Override
    public int compareTo(CountingHolder o)
    {
      return Integer.compare(count.get(), o.count.get());
    }


    @Override
    protected void finalize() throws Throwable
    {
      try {
        final int shouldBeZero = count.get();
        if (shouldBeZero != 0) {
          log.warn("Expected 0 resource count, got [%d]! Object was[%s].", shouldBeZero, object);
        }
      }
      finally {
        super.finalize();
      }
    }
  }
}
