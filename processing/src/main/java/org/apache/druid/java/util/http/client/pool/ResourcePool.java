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

package org.apache.druid.java.util.http.client.pool;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Lends out at most {@link ResourcePoolConfig#getMaxPerKey()} resources per key, blocking further takers until one
 * comes back.
 *
 * A resource is discarded once it has gone {@link ResourcePoolConfig#getUnusedConnectionTimeoutMillis()} unused or
 * {@link ResourceFactory#isGood} rejects it. With eagerInitialization a key starts out full, otherwise empty.
 *
 * {@link ResourcePoolConfig#isUseSemaphorePool()} selects between {@link SemaphoreResourceHolderPerKey} and the older
 * {@link ResourceHolderPerKey}.
 */
public class ResourcePool<K, V> implements Closeable
{
  private static final Logger log = new Logger(ResourcePool.class);
  private final LoadingCache<K, PooledResources<V>> pool;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ResourcePool(final ResourceFactory<K, V> factory, final ResourcePoolConfig config,
                      final boolean eagerInitialization)
  {
    this.pool = CacheBuilder.newBuilder().build(
        new CacheLoader<>()
        {
          @Override
          public PooledResources<V> load(K input)
          {
            if (config.isUseSemaphorePool()) {
              final SemaphoreResourceHolderPerKey<K, V> holder = new SemaphoreResourceHolderPerKey<>(
                  config.getMaxPerKey(),
                  config.getUnusedConnectionTimeoutMillis(),
                  input,
                  factory
              );
              if (eagerInitialization) {
                holder.preload();
              }
              return holder;
            } else if (eagerInitialization) {
              return new EagerCreationResourceHolder<>(
                  config.getMaxPerKey(),
                  config.getUnusedConnectionTimeoutMillis(),
                  input,
                  factory
              );
            } else {
              return new LazyCreationResourceHolder<>(
                  config.getMaxPerKey(),
                  config.getUnusedConnectionTimeoutMillis(),
                  input,
                  factory
              );
            }
          }
        }
    );
  }

  /**
   * Takes a resource, blocking until one is free.
   *
   * Returns null if this pool is closed, or if the calling thread is interrupted while waiting; the interrupt is left
   * set on the thread.
   */
  @Nullable
  public ResourceContainer<V> take(final K key)
  {
    if (closed.get()) {
      log.error(StringUtils.format("take(%s) called even though I'm closed.", key));
      return null;
    }

    final PooledResources<V> holder;
    try {
      holder = pool.get(key);
    }
    catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    final V value = holder.get();
    if (value == null) {
      return null;
    }

    return new ResourceContainer<>()
    {
      private final AtomicBoolean returned = new AtomicBoolean(false);

      @Override
      public V get()
      {
        Preconditions.checkState(!returned.get(), "Resource for key[%s] has been returned, cannot get().", key);
        return value;
      }

      @Override
      public void returnResource()
      {
        if (returned.getAndSet(true)) {
          log.warn("Resource at key[%s] was returned multiple times?", key);
        } else {
          holder.giveBack(value);
        }
      }

      @Override
      protected void finalize() throws Throwable
      {
        if (!returned.get()) {
          log.warn(
              StringUtils.format(
                  "Resource[%s] at key[%s] was not returned before Container was finalized, potential resource leak.",
                  value,
                  key
              )
          );
          returnResource();
        }
        super.finalize();
      }
    };
  }

  @Override
  public void close()
  {
    closed.set(true);
    final ConcurrentMap<K, PooledResources<V>> mapView = pool.asMap();
    Closer closer = Closer.create();
    for (Iterator<Map.Entry<K, PooledResources<V>>> iterator =
         mapView.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<K, PooledResources<V>> e = iterator.next();
      iterator.remove();
      closer.register(e.getValue());
    }
    try {
      closer.close();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * The resources of a single key.
   */
  private abstract static class PooledResources<V> implements Closeable
  {
    /**
     * Takes a resource, blocking until one is free. Null if closed or interrupted.
     */
    @Nullable
    abstract V get();

    /**
     * Returns a resource taken from {@link #get()}, freeing the slot it occupied.
     */
    abstract void giveBack(V object);

    /**
     * Discards every idle resource and releases every waiting taker. Lent resources are closed on {@link #giveBack}.
     */
    @Override
    public abstract void close();
  }

  private static class EagerCreationResourceHolder<K, V> extends LazyCreationResourceHolder<K, V>
  {
    private EagerCreationResourceHolder(
        int maxSize,
        long unusedResourceTimeoutMillis,
        K key,
        ResourceFactory<K, V> factory
    )
    {
      super(maxSize, unusedResourceTimeoutMillis, key, factory);
      // Eagerly Instantiate
      for (int i = 0; i < maxSize; i++) {
        resourceHolderList.add(
            new ResourceHolder<>(
                System.currentTimeMillis(),
                Preconditions.checkNotNull(
                    factory.generate(key),
                    "factory.generate(key)"
                )
            )
        );
      }
    }
  }

  private static class LazyCreationResourceHolder<K, V> extends ResourceHolderPerKey<K, V>
  {
    private LazyCreationResourceHolder(
        int maxSize,
        long unusedResourceTimeoutMillis,
        K key,
        ResourceFactory<K, V> factory
    )
    {
      super(maxSize, unusedResourceTimeoutMillis, key, factory);
    }
  }

  private static class ResourceHolderPerKey<K, V> extends PooledResources<V>
  {
    protected final int maxSize;
    private final K key;
    private final ResourceFactory<K, V> factory;
    private final long unusedResourceTimeoutMillis;
    // Hold previously created / returned resources
    protected final ArrayDeque<ResourceHolder<V>> resourceHolderList;
    // To keep track of resources that have been successfully returned to caller.
    private int numLentResources = 0;
    private boolean closed = false;

    protected ResourceHolderPerKey(
        int maxSize,
        long unusedResourceTimeoutMillis,
        K key,
        ResourceFactory<K, V> factory
    )
    {
      this.maxSize = maxSize;
      this.key = key;
      this.factory = factory;
      this.unusedResourceTimeoutMillis = unusedResourceTimeoutMillis;
      this.resourceHolderList = new ArrayDeque<>();
    }

    /**
     * Returns a resource or null if this holder is already closed or the current thread is interrupted.
     *
     * Try to return a previously created resource if it isGood(). Else, generate a new resource
     */
    @Nullable
    V get()
    {
      final V poolVal;
      // resourceHolderList can't have nulls, so we'll use a null to signal that we need to create a new resource.
      boolean expired = false;
      synchronized (this) {
        while (!closed && (numLentResources == maxSize)) {
          try {
            this.wait();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
          }
        }

        if (closed) {
          log.info(StringUtils.format("get() called even though I'm closed. key[%s]", key));
          return null;
        } else if (numLentResources < maxSize) {
          // Attempt to take an existing resource or create one if list is empty, and increment numLentResources
          if (resourceHolderList.isEmpty()) {
            poolVal = factory.generate(key);
          } else {
            ResourceHolder<V> holder = resourceHolderList.removeFirst();
            poolVal = holder.getResource();
            if (System.currentTimeMillis() - holder.getLastAccessedTime() > unusedResourceTimeoutMillis) {
              expired = true;
            }
          }
          numLentResources++;
        } else {
          throw new IllegalStateException("Unexpected state: More objects lent than permissible");
        }
      }

      final V retVal;
      // At this point, we must either return a valid resource. Or throw and exception decrement "numLentResources"
      try {
        if (poolVal != null && !expired && factory.isGood(poolVal)) {
          retVal = poolVal;
        } else {
          if (poolVal != null) {
            factory.close(poolVal);
          }
          retVal = factory.generate(key);
        }
      }
      catch (Throwable e) {
        synchronized (this) {
          numLentResources--;
          this.notifyAll();
        }
        Throwables.propagateIfPossible(e);
        throw new RuntimeException(e);
      }

      return retVal;
    }

    void giveBack(V object)
    {
      Preconditions.checkNotNull(object, "object");

      synchronized (this) {
        if (closed) {
          log.info(StringUtils.format("giveBack called after being closed. key[%s]", key));
          factory.close(object);
          return;
        }

        if (resourceHolderList.size() >= maxSize) {
          if (holderListContains(object)) {
            log.warn(
                new Exception("Exception for stacktrace"),
                StringUtils.format(
                    "Returning object[%s] at key[%s] that has already been returned!? Skipping",
                    object,
                    key
                )
            );
          } else {
            log.warn(
                new Exception("Exception for stacktrace"),
                StringUtils.format(
                    "Returning object[%s] at key[%s] even though we already have all that we can hold[%s]!? Skipping",
                    object,
                    key,
                    resourceHolderList
                )
            );
          }
          return;
        }

        resourceHolderList.addLast(new ResourceHolder<>(System.currentTimeMillis(), object));
        numLentResources--;
        this.notifyAll();
      }
    }

    private boolean holderListContains(V object)
    {
      return resourceHolderList.stream().anyMatch(a -> a.getResource().equals(object));
    }

    @Override
    public void close()
    {
      synchronized (this) {
        closed = true;
        resourceHolderList.forEach(v -> factory.close(v.getResource()));
        resourceHolderList.clear();
        this.notifyAll();
      }
    }
  }

  /**
   * Pools the resources of one key behind a permit per lendable resource, holding no lock while creating, validating
   * or closing them.
   *
   * A taker discards every stale or broken resource it walks past rather than one per take, so the pool shrinks to
   * what the traffic needs instead of reconnecting one for one.
   */
  private static class SemaphoreResourceHolderPerKey<K, V> extends PooledResources<V>
  {
    /**
     * Released on close to wake every parked taker at once. Half of the range so that the permits still outstanding
     * cannot overflow the count when they come back.
     */
    private static final int CLOSE_PERMITS = Integer.MAX_VALUE / 2;

    private final int maxSize;
    private final K key;
    private final ResourceFactory<K, V> factory;
    private final long unusedResourceTimeoutMillis;
    private final Semaphore permits;
    private final Deque<ResourceHolder<V>> idleResources = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private SemaphoreResourceHolderPerKey(
        int maxSize,
        long unusedResourceTimeoutMillis,
        K key,
        ResourceFactory<K, V> factory
    )
    {
      this.maxSize = maxSize;
      this.key = key;
      this.factory = factory;
      this.unusedResourceTimeoutMillis = unusedResourceTimeoutMillis;
      this.permits = new Semaphore(maxSize);
    }

    /**
     * Fills the pool to its maximum size, closing what it created if that fails.
     */
    void preload()
    {
      try {
        for (int i = 0; i < maxSize; i++) {
          idleResources.addLast(new ResourceHolder<>(System.currentTimeMillis(), generate()));
        }
      }
      catch (Throwable t) {
        closeIdleResources();
        throw t;
      }
    }

    @Nullable
    @Override
    V get()
    {
      if (!acquirePermit()) {
        return null;
      }

      boolean lent = false;
      try {
        V resource = takeIdleResource();
        if (resource == null) {
          resource = createResource();
        }
        lent = true;
        return resource;
      }
      finally {
        if (!lent) {
          permits.release();
        }
      }
    }

    @Override
    void giveBack(V object)
    {
      Preconditions.checkNotNull(object, "object");

      if (closed.get()) {
        log.info("giveBack called after being closed. key[%s]", key);
        closeQuietly(object);
        permits.release();
        return;
      }

      idleResources.addLast(new ResourceHolder<>(System.currentTimeMillis(), object));
      permits.release();

      if (closed.get()) {
        // close() may have drained the idle resources before this one was parked.
        closeIdleResources();
      }
    }

    @Override
    public void close()
    {
      if (closed.compareAndSet(false, true)) {
        permits.release(CLOSE_PERMITS);
        closeIdleResources();
      }
    }

    private boolean acquirePermit()
    {
      try {
        permits.acquire();
      }
      catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return false;
      }

      if (closed.get()) {
        log.info("get() called even though I'm closed. key[%s]", key);
        permits.release();
        return false;
      }
      return true;
    }

    /**
     * Removes and returns the first usable idle resource, closing every expired or broken one it walks past. Null if
     * none is left.
     */
    @Nullable
    private V takeIdleResource()
    {
      final long expiredBefore = System.currentTimeMillis() - unusedResourceTimeoutMillis;
      for (ResourceHolder<V> holder = idleResources.pollFirst(); holder != null; holder = idleResources.pollFirst()) {
        final V resource = holder.getResource();
        final boolean usable;
        try {
          usable = holder.getLastAccessedTime() >= expiredBefore && factory.isGood(resource);
        }
        catch (Throwable t) {
          closeQuietly(resource);
          throw t;
        }
        if (usable) {
          return resource;
        }
        closeQuietly(resource);
      }
      return null;
    }

    /**
     * Creates a resource, replacing it once if it arrives broken. The replacement is not validated.
     */
    private V createResource()
    {
      final V resource = generate();
      final boolean usable;
      try {
        usable = factory.isGood(resource);
      }
      catch (Throwable t) {
        closeQuietly(resource);
        throw t;
      }
      if (usable) {
        return resource;
      }
      closeQuietly(resource);
      return generate();
    }

    private V generate()
    {
      return Preconditions.checkNotNull(factory.generate(key), "factory.generate(key)");
    }

    private void closeIdleResources()
    {
      for (ResourceHolder<V> holder = idleResources.pollFirst(); holder != null; holder = idleResources.pollFirst()) {
        closeQuietly(holder.getResource());
      }
    }

    /**
     * Closes a resource that is already out of the pool, where a failure has nothing left to abort.
     */
    private void closeQuietly(V resource)
    {
      try {
        factory.close(resource);
      }
      catch (Exception e) {
        log.warn(e, "Failed to close resource at key[%s]", key);
      }
    }
  }

  private static class ResourceHolder<V>
  {
    private final long lastAccessedTime;
    private final V resource;

    private ResourceHolder(long lastAccessedTime, V resource)
    {
      this.resource = resource;
      this.lastAccessedTime = lastAccessedTime;
    }

    private long getLastAccessedTime()
    {
      return lastAccessedTime;
    }

    public V getResource()
    {
      return resource;
    }

  }
}
