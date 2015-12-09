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

package io.druid.server.namespace.cache;

import com.google.common.base.Function;
import com.google.common.util.concurrent.Striped;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.query.extraction.namespace.ExtractionNamespace;
import io.druid.query.extraction.namespace.ExtractionNamespaceFunctionFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;

/**
 *
 */
public class OnHeapNamespaceExtractionCacheManager extends NamespaceExtractionCacheManager
{
  private final ConcurrentMap<String, ConcurrentMap<String, String>> mapMap = new ConcurrentHashMap<>();
  private final Striped<Lock> nsLocks = Striped.lock(32);

  @Inject
  public OnHeapNamespaceExtractionCacheManager(
      final Lifecycle lifecycle,
      @Named("namespaceExtractionFunctionCache")
      final ConcurrentMap<String, Function<String, String>> fnCache,
      @Named("namespaceReverseExtractionFunctionCache")
      final ConcurrentMap<String, Function<String, List<String>>> reverseFnCache,
      final ServiceEmitter emitter,
      final Map<Class<? extends ExtractionNamespace>, ExtractionNamespaceFunctionFactory<?>> namespaceFunctionFactoryMap
  )
  {
    super(lifecycle, fnCache, reverseFnCache,emitter, namespaceFunctionFactoryMap);
  }

  @Override
  protected boolean swapAndClearCache(String namespaceKey, String cacheKey)
  {
    final Lock lock = nsLocks.get(namespaceKey);
    lock.lock();
    try {
      ConcurrentMap<String, String> cacheMap = mapMap.get(cacheKey);
      if (cacheMap == null) {
        // Sometimes cache will not be populated (for example: if it doesn't contain new data)
        return false;
      }
      dataSize.addAndGet(cacheMap.size());
      ConcurrentMap<String, String> prior = mapMap.put(namespaceKey, cacheMap);
      mapMap.remove(cacheKey);
      if (prior != null) {
        dataSize.addAndGet(-prior.size());
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
      mapMap.putIfAbsent(namespaceOrCacheKey, new ConcurrentHashMap<String, String>(32));
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
      super.delete(namespaceKey);
      return mapMap.remove(namespaceKey) != null;
    }
    finally {
      lock.unlock();
    }
  }
}
