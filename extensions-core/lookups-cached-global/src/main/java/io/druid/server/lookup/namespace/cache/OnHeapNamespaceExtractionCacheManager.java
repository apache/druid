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
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import io.druid.java.util.common.IAE;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 *
 */
public class OnHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger LOG = new Logger(OnHeapNamespaceExtractionCacheManager.class);
  private final ConcurrentMap<String, ConcurrentMap<String, String>> mapMap = new ConcurrentHashMap<>();
  private final Striped<Lock> nsLocks = Striped.lock(32);

  @Inject
  public OnHeapNamespaceExtractionCacheManager(
      final Lifecycle lifecycle,
      final ServiceEmitter emitter,
      final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceCacheFactory<?>> namespaceFunctionFactoryMap
  )
  {
    super(lifecycle, emitter, namespaceFunctionFactoryMap);
  }

  @Override
  protected boolean swapAndClearCache(String namespaceKey, String cacheKey)
  {
    final Lock lock = nsLocks.get(namespaceKey);
    lock.lock();
    try {
      ConcurrentMap<String, String> cacheMap = mapMap.get(cacheKey);
      if (cacheMap == null) {
        throw new IAE("Extraction Cache [%s] does not exist", cacheKey);
      }
      ConcurrentMap<String, String> prior = mapMap.put(namespaceKey, cacheMap);
      mapMap.remove(cacheKey);
      if (prior != null) {
        // Old map will get GC'd when it is not used anymore
        return true;
      } else {
        return false;
      }
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  public ConcurrentMap<String, String> getCacheMap(String namespaceOrCacheKey)
  {
    ConcurrentMap<String, String> map = mapMap.get(namespaceOrCacheKey);
    if (map == null) {
      mapMap.putIfAbsent(namespaceOrCacheKey, new ConcurrentHashMap<String, String>());
      map = mapMap.get(namespaceOrCacheKey);
    }
    return map;
  }

  @Override
  public boolean delete(final String namespaceKey)
  {
    final Lock lock = nsLocks.get(namespaceKey);
    lock.lock();
    try {
      return super.delete(namespaceKey) && mapMap.remove(namespaceKey) != null;
    }
    finally {
      lock.unlock();
    }
  }

  @Override
  protected void monitor(ServiceEmitter serviceEmitter)
  {
    long numEntries = 0;
    long size = 0;
    for (Map.Entry<String, ConcurrentMap<String, String>> entry : mapMap.entrySet()) {
      final ConcurrentMap<String, String> map = entry.getValue();
      if (map == null) {
        LOG.debug("missing cache key for reporting [%s]", entry.getKey());
        continue;
      }
      numEntries += map.size();
      for (Map.Entry<String, String> sEntry : map.entrySet()) {
        final String key = sEntry.getKey();
        final String value = sEntry.getValue();
        if (key == null || value == null) {
          LOG.debug("Missing entries for cache key [%s]", entry.getKey());
          continue;
        }
        size += key.length() + value.length();
      }
    }
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/numEntries", numEntries));
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/heapSizeInBytes", size * Chars.BYTES));
  }
}
