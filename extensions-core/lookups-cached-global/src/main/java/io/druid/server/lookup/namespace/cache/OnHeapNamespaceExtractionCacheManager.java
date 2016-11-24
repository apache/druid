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
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.java.util.common.logger.Logger;
import io.druid.query.lookup.namespace.ExtractionNamespace;
import io.druid.query.lookup.namespace.ExtractionNamespaceCacheFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 *
 */
public class OnHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private static final Logger LOG = new Logger(OnHeapNamespaceExtractionCacheManager.class);
  private final ConcurrentMap<Pair, ConcurrentMap<String, String>> mapMap = new ConcurrentHashMap<>();

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
  public ConcurrentMap<String, String> getOrAllocateInnerCacheMap(Pair key)
  {
    ConcurrentMap<String, String> map = mapMap.get(key);
    if (map == null) {
      mapMap.putIfAbsent(key, new ConcurrentHashMap<String, String>());
      map = mapMap.get(key);
    }
    return map;
  }

  @Override
  protected boolean deleteInnerCacheMaps(final ConcurrentMap<Pair, Map<String, String>> map)
  {
    // do nothing because references to maps will be removed by NamesapceExtractionCacheManager after the return of this method
    // and GC will clean maps eventually
    return true;
  }

  @Override
  protected void monitor(ServiceEmitter serviceEmitter)
  {
    long numEntries = 0;
    long size = 0;
    for (Map.Entry<Pair, ConcurrentMap<String, String>> entry : mapMap.entrySet()) {
      final ConcurrentMap<String, String> map = entry.getValue();
      final Pair cacheKey = entry.getKey();
      if (map == null) {
        LOG.debug("missing cache key for reporting [%s of %s]", cacheKey.rhs, cacheKey.lhs);
        continue;
      }
      numEntries += map.size();
      for (Map.Entry<String, String> sEntry : map.entrySet()) {
        final String key = sEntry.getKey();
        final String value = sEntry.getValue();
        if (key == null || value == null) {
          LOG.debug("Missing entries for cache key [%s of %s]", cacheKey.rhs, cacheKey.lhs);
          continue;
        }
        size += key.length() + value.length();
      }
    }
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/numEntries", numEntries));
    serviceEmitter.emit(ServiceMetricEvent.builder().build("namespace/cache/heapSizeInBytes", size * Chars.BYTES));
  }
}
