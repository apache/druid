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

import com.google.inject.Inject;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.extraction.MapLookupExtractor;
import org.apache.druid.query.lookup.ImmutableLookupMap;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.server.lookup.namespace.NamespaceExtractionConfig;

import java.lang.ref.WeakReference;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Supplier;

/**
 *
 */
public class OnHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  /**
   * Weak collection of caches is "the second level of defence". Normally all users of {@link #createCache()} must call
   * {@link CacheHandler#close()} on the returned CacheHandler instance manually. But if they don't do this for
   * whatever reason, JVM will cleanup the cache itself.
   *
   * <p>{@link WeakReference} doesn't override Object's identity equals() and hashCode(), so effectively this map plays
   * like concurrent {@link java.util.IdentityHashMap}.
   */
  private final Set<WeakReference<Map<String, String>>> caches = Collections.newSetFromMap(new ConcurrentHashMap<>());

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
    for (Iterator<WeakReference<Map<String, String>>> iterator = caches.iterator(); iterator.hasNext(); ) {
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
    WeakReference<Map<String, String>> cacheRef = new WeakReference<>(cache);
    expungeCollectedCaches();
    caches.add(cacheRef);
    return new CacheHandler(this, cache, cacheRef);
  }

  @Override
  public CacheHandler allocateCache()
  {
    // Object2ObjectOpenHashMap has a bit smaller footprint than HashMap
    Map<String, String> cache = new Object2ObjectOpenHashMap<>();
    // untracked, but disposing will explode if we don't create a weak reference here
    return new CacheHandler(this, cache, new WeakReference<>(cache));
  }

  @Override
  public CacheHandler attachCache(CacheHandler cache)
  {
    if (caches.contains((WeakReference<Map<String, String>>) cache.id)) {
      throw new ISE("cache [%s] is already attached", cache.id);
    }
    // replace Object2ObjectOpenHashMap with ImmutableLookupMap
    final ImmutableLookupMap immutable = ImmutableLookupMap.fromMap(cache.getCache());
    WeakReference<Map<String, String>> cacheRef = new WeakReference<>(immutable);
    expungeCollectedCaches();
    caches.add(cacheRef);
    return new CacheHandler(this, immutable, cacheRef);
  }

  @Override
  public LookupExtractor asLookupExtractor(
      final CacheHandler cache,
      final boolean isOneToOne,
      final Supplier<byte[]> cacheKeySupplier
  )
  {
    if (cache.getCache() instanceof ImmutableLookupMap) {
      return ((ImmutableLookupMap) cache.getCache()).asLookupExtractor(isOneToOne, cacheKeySupplier);
    } else {
      return new MapLookupExtractor(cache.getCache(), isOneToOne) {
        @Override
        public byte[] getCacheKey()
        {
          return cacheKeySupplier.get();
        }
      };
    }
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
    long heapSizeInBytes = 0;
    expungeCollectedCaches();
    for (WeakReference<Map<String, String>> cacheRef : caches) {
      final Map<String, String> cache = cacheRef.get();

      if (cache != null) {
        numEntries += cache.size();
        heapSizeInBytes += MapLookupExtractor.estimateHeapFootprint(cache.entrySet());
      }
    }

    serviceEmitter.emit(ServiceMetricEvent.builder().setMetric("namespace/cache/numEntries", numEntries));
    serviceEmitter.emit(ServiceMetricEvent.builder().setMetric("namespace/cache/heapSizeInBytes", heapSizeInBytes));
  }
}
