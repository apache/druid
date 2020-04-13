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
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A resource pool based on {@link LoadingCache}. When a resource is first requested for a new key,
 * all {@link ResourcePoolConfig#getMaxPerKey()} resources are initialized and cached in the {@link #pool}.
 * The individual resource in {@link ImmediateCreationResourceHolder} is valid while (current time - last access time)
 * <= {@link ResourcePoolConfig#getUnusedConnectionTimeoutMillis()}.
 *
 * A resource is closed and reinitialized if {@link ResourceFactory#isGood} returns false or it's expired based on
 * {@link ResourcePoolConfig#getUnusedConnectionTimeoutMillis()}.
 *
 * {@link ResourcePoolConfig#getMaxPerKey() is a hard limit for the max number of resources per cache entry. The total
 * number of resources in {@link ImmediateCreationResourceHolder} cannot be larger than the limit in any case.
 */
public class ResourcePool<K, V> implements Closeable
{
  private static final Logger log = new Logger(ResourcePool.class);
  private final LoadingCache<K, ImmediateCreationResourceHolder<K, V>> pool;
  private final AtomicBoolean closed = new AtomicBoolean(false);

  public ResourcePool(final ResourceFactory<K, V> factory, final ResourcePoolConfig config)
  {
    this.pool = CacheBuilder.newBuilder().build(
        new CacheLoader<K, ImmediateCreationResourceHolder<K, V>>()
        {
          @Override
          public ImmediateCreationResourceHolder<K, V> load(K input)
          {
            return new ImmediateCreationResourceHolder<>(
                config.getMaxPerKey(),
                config.getUnusedConnectionTimeoutMillis(),
                input,
                factory
            );
          }
        }
    );
  }

  /**
   * Returns a {@link ResourceContainer} for the given key or null if this pool is already closed.
   */
  @Nullable
  public ResourceContainer<V> take(final K key)
  {
    if (closed.get()) {
      log.error(StringUtils.format("take(%s) called even though I'm closed.", key));
      return null;
    }

    final ImmediateCreationResourceHolder<K, V> holder;
    try {
      holder = pool.get(key);
    }
    catch (ExecutionException e) {
      throw new RuntimeException(e);
    }
    final V value = holder.get();

    return new ResourceContainer<V>()
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
          log.warn(StringUtils.format("Resource at key[%s] was returned multiple times?", key));
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
    final ConcurrentMap<K, ImmediateCreationResourceHolder<K, V>> mapView = pool.asMap();
    Closer closer = Closer.create();
    for (Iterator<Map.Entry<K, ImmediateCreationResourceHolder<K, V>>> iterator =
         mapView.entrySet().iterator(); iterator.hasNext(); ) {
      Map.Entry<K, ImmediateCreationResourceHolder<K, V>> e = iterator.next();
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

  private static class ImmediateCreationResourceHolder<K, V> implements Closeable
  {
    private final int maxSize;
    private final K key;
    private final ResourceFactory<K, V> factory;
    private final ArrayDeque<ResourceHolder<V>> resourceHolderList;
    private int deficit = 0;
    private boolean closed = false;
    private final long unusedResourceTimeoutMillis;

    private ImmediateCreationResourceHolder(
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

      for (int i = 0; i < maxSize; ++i) {
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

    /**
     * Returns a resource or null if this holder is already closed or the current thread is interrupted.
     */
    @Nullable
    V get()
    {
      // resourceHolderList can't have nulls, so we'll use a null to signal that we need to create a new resource.
      final V poolVal;
      synchronized (this) {
        while (!closed && resourceHolderList.size() == 0 && deficit == 0) {
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
        } else if (!resourceHolderList.isEmpty()) {
          ResourceHolder<V> holder = resourceHolderList.removeFirst();
          if (System.currentTimeMillis() - holder.getLastAccessedTime() > unusedResourceTimeoutMillis) {
            factory.close(holder.getResource());
            poolVal = factory.generate(key);
          } else {
            poolVal = holder.getResource();
          }
        } else if (deficit > 0) {
          deficit--;
          poolVal = null;
        } else {
          throw new IllegalStateException("WTF?! No objects left, and no object deficit. This is probably a bug.");
        }
      }

      // At this point, we must either return a valid resource or increment "deficit".
      final V retVal;
      try {
        if (poolVal != null && factory.isGood(poolVal)) {
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
          deficit++;
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
