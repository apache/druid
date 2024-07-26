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

package org.apache.druid.server.lookup.namespace.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.druid.java.util.common.concurrent.ExecutorServices;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionConfig;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Usage:
 * <pre>{@code
 * CacheHandler cacheHandler = namespaceExtractionCacheManager.createCache();
 * Map<String, String> cache == cacheHandler.cache; // use this cache
 * ...
 * cacheHandler.close();
 * }</pre>
 */
public abstract class NamespaceExtractionCacheManager
{
  private static final Logger log = new Logger(NamespaceExtractionCacheManager.class);

  private final ScheduledThreadPoolExecutor scheduledExecutorService;

  public NamespaceExtractionCacheManager(
      final Lifecycle lifecycle,
      final ServiceEmitter serviceEmitter,
      final NamespaceExtractionConfig config
  )
  {
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(
        config.getNumExtractionThreads(),
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("NamespaceExtractionCacheManager-%d")
            .setPriority(Thread.MIN_PRIORITY)
            .build()
    );
    ExecutorServices.manageLifecycle(lifecycle, scheduledExecutorService);
    scheduledExecutorService.scheduleAtFixedRate(
        () -> {
          try {
            monitor(serviceEmitter);
          }
          catch (Exception e) {
            log.error(e, "Error emitting namespace stats");
            if (Thread.currentThread().isInterrupted()) {
              throw new RuntimeException(e);
            }
          }
        },
        1,
        10,
        TimeUnit.MINUTES
    );
  }

  /**
   * Return {@link ScheduledThreadPoolExecutor} rather than more generic {@link
   * java.util.concurrent.ScheduledExecutorService}, because the former guarantees that periodic runs of scheduled
   * tasks see all "local" changes from the previous runs, via happens-before (see {@link ScheduledThreadPoolExecutor}'s
   * class-level Javadoc).
   */
  final ScheduledThreadPoolExecutor scheduledExecutorService()
  {
    return scheduledExecutorService;
  }

  @VisibleForTesting
  boolean waitForServiceToEnd(long time, TimeUnit unit) throws InterruptedException
  {
    return scheduledExecutorService.awaitTermination(time, unit);
  }

  /**
   * Creates a thread-safe, permanently mutable cache. Use this method if you need a cache which can be incrementally
   * updated, indefinitely, at the cost of the overhead of synchronization
   */
  public abstract CacheHandler createCache();

  /**
   * Create a cache that is intended to be populated before use, and then passed to {@link #attachCache(CacheHandler)}.
   */
  public abstract CacheHandler allocateCache();

  /**
   * Attach a cache created with {@link #allocateCache()} to the cache manager. The cache return from this method
   * should be treated as read-only (though some implmentations may still allow modification, it is not safe to do so).
   */
  public abstract CacheHandler attachCache(CacheHandler cache);

  /**
   * Given a cache from {@link #createCache()} or {@link #allocateCache()}, return a {@link LookupExtractor}
   * view of it.
   */
  public abstract LookupExtractor asLookupExtractor(
      CacheHandler cacheHandler,
      boolean isOneToOne,
      Supplier<byte[]> cacheKeySupplier
  );

  abstract void disposeCache(CacheHandler cacheHandler);

  abstract int cacheCount();

  abstract void monitor(ServiceEmitter serviceEmitter);
}
