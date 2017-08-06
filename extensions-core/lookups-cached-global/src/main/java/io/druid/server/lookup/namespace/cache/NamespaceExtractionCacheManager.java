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

package io.druid.server.lookup.namespace.cache;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.java.util.common.concurrent.ExecutorServices;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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

  public NamespaceExtractionCacheManager(final Lifecycle lifecycle, final ServiceEmitter serviceEmitter)
  {
    this.scheduledExecutorService = new ScheduledThreadPoolExecutor(
        1,
        new ThreadFactoryBuilder()
            .setDaemon(true)
            .setNameFormat("NamespaceExtractionCacheManager-%d")
            .setPriority(Thread.MIN_PRIORITY)
            .build()
    );
    ExecutorServices.manageLifecycle(lifecycle, scheduledExecutorService);
    scheduledExecutorService.scheduleAtFixedRate(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              monitor(serviceEmitter);
            }
            catch (Exception e) {
              log.error(e, "Error emitting namespace stats");
              if (Thread.currentThread().isInterrupted()) {
                throw Throwables.propagate(e);
              }
            }
          }
        },
        1,
        10, TimeUnit.MINUTES
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

  public abstract CacheHandler createCache();

  abstract void disposeCache(CacheHandler cacheHandler);

  abstract int cacheCount();

  abstract void monitor(ServiceEmitter serviceEmitter);
}
