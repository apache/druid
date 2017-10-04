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

import com.google.common.primitives.Chars;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.lookup.namespace.NamespaceExtractionConfig;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class OnHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger LOG = new Logger(OnHeapNamespaceExtractionCacheManager.class);

  /**
   * Weak collection of caches is "the second level of defence". Normally all users of {@link #createCache()} must call
   * {@link CacheHandler#close()} on the returned CacheHandler instance manually. But if they don't do this for
   * whatever reason, JVM will cleanup the cache itself.
   *
   * <p>{@link WeakReference} doesn't override Object's identity equals() and hashCode(), so effectively this map plays
   * like concurrent {@link java.util.IdentityHashMap}.
   */
  private final Set<WeakReference<ConcurrentMap<String, String>>> caches = Collections.newSetFromMap(
      new ConcurrentHashMap<WeakReference<ConcurrentMap<String, String>>, Boolean>()
  );

  @Inject
  public OnHeapNamespaceExtractionCacheManager(
      Lifecycle lifecycle,
      ServiceEmitter serviceEmitter,
      NamespaceExtractionConfig config
  )
  {
    super(lifecycle, serviceEmitter, config);
  }

  private void expungeCollectedCaches()
  {
    for (Iterator<WeakReference<ConcurrentMap<String, String>>> iterator = caches.iterator(); iterator.hasNext(); ) {
      WeakReference<?> cacheRef = iterator.next();
      if (cacheRef.get() == null) {
        // This may not necessarily mean leak of CacheHandler, because disposeCache() may be called concurrently with
        // this iteration, and cacheHandler (hence the cache) could be already claimed by the GC. That is why we emit
        // no warning here. Also, "soft leak" (close() not called, but the objects becomes unreachable and claimed by
        // the GC) of on-heap cache is effectively harmless and logging may be useful here only for identifying bugs in
        // the code which uses NamespaceExtractionCacheManager, if there are plans to switch to
        // OffHeapNamespaceExtractionCacheManager. However in OffHeapNamespaceExtractionCacheManager CacheHandler leaks
        // are identified and logged better than in this class.
        iterator.remove();
      }
    }
  }

  @Override
  public CacheHandler createCache()
  {
    ConcurrentMap<String, String> cache = new ConcurrentHashMap<>();
    WeakReference<ConcurrentMap<String, String>> cacheRef = new WeakReference<>(cache);
    expungeCollectedCaches();
    caches.add(cacheRef);
    return new CacheHandler(this, cache, cacheRef);
  }

  @Override
  void disposeCache(CacheHandler cacheHandler)
  {
    if (!(cacheHandler.id instanceof WeakReference)) {
      throw new ISE("Expected WeakReference, got: %s", cacheHandler.id);
    }
    caches.remove(cacheHandler.id);
  }

  @Override
  int cacheCount()
  {
    expungeCollectedCaches();
    return caches.size();
  }

  @Override
  void monitor(ServiceEmitter serviceEmitter)
  {
    long numEntries = 0;
    long size = 0;
    expungeCollectedCaches();
    for (WeakReference<ConcurrentMap<String, String>> cacheRef : caches) {
      final Map<String, String> cache = cacheRef.get();
      if (cache == null) {
        continue;
      }
      numEntries += cache.size();
      for (Map.Entry<String, String> sEntry : cache.entrySet()) {
        final String key = sEntry.getKey();
        final String value = sEntry.getValue();
        if (key == null || value == null) {
          LOG.debug("Missing entries for cache key");
          continue;
        }
        size += key.length() + value.length();
      }
    }
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/numEntries", numEntries));
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/heapSizeInBytes", size * Chars.BYTES));
  }
}
