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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A resource pool based on {@link LoadingCache}. Resources are pooled per key and at most
 * {@link ResourcePoolConfig#getMaxPerKey()} of them are lent out at a time; a caller arriving when they are all lent
 * out waits for one to come back.
 *
 * With eagerInitialization the pool is filled to that maximum the first time a key is used, otherwise resources are
 * created on demand. Either way a resource is only ever created when no idle one is available, so the pool settles at
 * the size the traffic actually needs: an idle resource is discarded once it has gone
 * {@link ResourcePoolConfig#getUnusedConnectionTimeoutMillis()} unused, or as soon as {@link ResourceFactory#isGood}
 * rejects it.
 */
public class ResourcePool<K, V> implements Closeable
{
  private static final Logger log = new Logger(ResourcePool.class);
  private final LoadingCache<K, ResourceHolderPerKey<K, V>> pool;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ResourcePool(final ResourceFactory<K, V> factory, final ResourcePoolConfig config,
                      final boolean eagerInitialization)
  {
    this.pool = CacheBuilder.newBuilder().build(
        new CacheLoader<>()
        {
          @Override
          public ResourceHolderPerKey<K, V> load(K input)
          {
            final ResourceHolderPerKey<K, V> holder = new ResourceHolderPerKey<>(
                config.getMaxPerKey(),
                config.getUnusedConnectionTimeoutMillis(),
                input,
                factory
            );
            if (eagerInitialization) {
              holder.preload();
            }
            return holder;
          }
        }
    );
  }

  /**
   * Returns a {@link ResourceContainer} for the given key, or null if this pool is closed or the calling thread was
   * interrupted while waiting for a resource to become available.
   */
  @Nullable
  public ResourceContainer<V> take(final K key)
  {
    if (closed.get()) {
      log.error(StringUtils.format("take(%s) called even though I'm closed.", key));
      return null;
    }

    final ResourceHolderPerKey<K, V> holder;
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
    final ConcurrentMap<K, ResourceHolderPerKey<K, V>> mapView = pool.asMap();
    Closer closer = Closer.create();
    for (Iterator<Map.Entry<K, ResourceHolderPerKey<K, V>>> iterator =
         mapView.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<K, ResourceHolderPerKey<K, V>> e = iterator.next();
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
   * The resources pooled for a single key.
   *
   * A permit must be held to have a resource lent out, which is what bounds the pool to
   * {@link ResourcePoolConfig#getMaxPerKey()}: takers park until a permit frees up. Idle resources are parked
   * oldest-first, and a taker walks them from the front, discarding the ones that expired or that
   * {@link ResourceFactory#isGood} rejects, before falling back to creating one. No lock is held while doing so, so a
   * slow {@link ResourceFactory#close} never stalls the other takers of this key.
   */
  private static class ResourceHolderPerKey<K, V> implements Closeable
  {
    /**
     * Released on close to wake every parked taker at once. Half of the range keeps the permit count from overflowing
     * when the outstanding permits are handed back afterwards.
     */
    private static final int CLOSE_PERMITS = Integer.MAX_VALUE / 2;

    private final int maxSize;
    private final K key;
    private final ResourceFactory<K, V> factory;
    private final long unusedResourceTimeoutMillis;
    private final Semaphore permits;
    private final Deque<ResourceHolder<V>> idleResources = new ConcurrentLinkedDeque<>();
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private ResourceHolderPerKey(
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
     * Fills the pool up to its maximum size. Resources created before a failure are closed, since a holder whose
     * creation fails never reaches the cache and could not be reached again.
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

    /**
     * Returns a resource, waiting for one to be given back if they are all lent out, or null if this holder is closed
     * or the current thread is interrupted.
     */
    @Nullable
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
     * Returns the first idle resource still worth using, closing every expired or broken one it walks past, or null if
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
     * Creates a resource, replacing it once if it turns out to be broken on arrival.
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
