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

package org.apache.druid.client.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import net.spy.memcached.MemcachedClientIF;
import org.apache.druid.collections.ResourceHolder;
import org.apache.druid.java.util.common.Cleaners;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simple load balancing pool that always returns the least used {@link MemcachedClientIF}.
 *
 * A client's usage is incremented every time one gets requested from the pool
 * and is decremented every time close is called on the holder.
 *
 * The pool eagerly instantiates all the clients in the pool when created,
 * using the given supplier.
 */
final class MemcacheClientPool implements Supplier<ResourceHolder<MemcachedClientIF>>
{
  private static final Logger log = new Logger(MemcacheClientPool.class);

  private static final AtomicLong LEAKED_CLIENTS = new AtomicLong(0);

  public static long leakedClients()
  {
    return LEAKED_CLIENTS.get();
  }

  /**
   * The number of memcached connections is not expected to be small (<= 8), so it's easier to find the least used
   * connection using a linear search over a simple array, than fiddling with PriorityBlockingQueue. This also allows
   * to reduce locking.
   */
  private final CountingHolder[] connections;

  MemcacheClientPool(int capacity, Supplier<MemcachedClientIF> generator)
  {
    Preconditions.checkArgument(capacity > 0, "capacity must be greater than 0");
    Preconditions.checkNotNull(generator);

    CountingHolder[] connections = new CountingHolder[capacity];
    // eagerly instantiate all items in the pool
    for (int i = 0; i < capacity; ++i) {
      connections[i] = new CountingHolder(generator.get());
    }
    // Assign the final field after filling the array to ensure visibility of elements
    this.connections = connections;
  }

  @Override
  public synchronized IdempotentCloseableHolder get()
  {
    CountingHolder leastUsedClientHolder = connections[0];
    int minCount = leastUsedClientHolder.count.get();
    for (int i = 1; i < connections.length; i++) {
      CountingHolder clientHolder = connections[i];
      int count = clientHolder.count.get();
      if (count < minCount) {
        leastUsedClientHolder = clientHolder;
        minCount = count;
      }
    }
    leastUsedClientHolder.count.incrementAndGet();
    return new IdempotentCloseableHolder(leastUsedClientHolder);
  }

  private static class CountingHolder
  {
    private final AtomicInteger count = new AtomicInteger(0);
    private final MemcachedClientIF clientIF;
    /**
     * The point of cleanable is to be referenced. Action is performed when it becomes unreachable, so it doesn't need
     * to be used directly.
     */
    @SuppressWarnings("unused")
    private final Cleaners.Cleanable cleanable;

    private CountingHolder(final MemcachedClientIF clientIF)
    {
      this.clientIF = clientIF;
      cleanable = Cleaners.register(this, new ClientLeakNotifier(count, clientIF));
    }
  }

  @VisibleForTesting
  static class IdempotentCloseableHolder implements ResourceHolder<MemcachedClientIF>
  {
    private CountingHolder countingHolder;

    private IdempotentCloseableHolder(CountingHolder countingHolder)
    {
      this.countingHolder = countingHolder;
    }

    @Override
    public MemcachedClientIF get()
    {
      return countingHolder.clientIF;
    }

    int count()
    {
      return countingHolder.count.get();
    }

    @Override
    public void close()
    {
      if (countingHolder != null) {
        countingHolder.count.decrementAndGet();
        countingHolder = null;
      }
    }
  }

  private static class ClientLeakNotifier implements Runnable
  {
    private final AtomicInteger count;
    private final MemcachedClientIF clientIF;

    private ClientLeakNotifier(AtomicInteger count, MemcachedClientIF clientIF)
    {
      this.count = count;
      this.clientIF = clientIF;
    }

    @Override
    public void run()
    {
      final int shouldBeZero = count.get();
      if (shouldBeZero != 0) {
        LEAKED_CLIENTS.incrementAndGet();
        log.warn("Expected 0 resource count, got [%d]! Object was[%s].", shouldBeZero, clientIF);
      }
    }
  }
}
