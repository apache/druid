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

import io.druid.java.util.common.logger.Logger;

import java.util.concurrent.ConcurrentMap;

public final class CacheHandler implements AutoCloseable
{
  private static final Logger log = new Logger(CacheHandler.class);

  private final NamespaceExtractionCacheManager cacheManager;
  private final ConcurrentMap<String, String> cache;
  final Object id;

  CacheHandler(NamespaceExtractionCacheManager cacheManager, ConcurrentMap<String, String> cache, Object id)
  {
    log.debug("Creating %s", super.toString());
    this.cacheManager = cacheManager;
    this.cache = cache;
    this.id = id;
  }

  public ConcurrentMap<String, String> getCache()
  {
    return cache;
  }

  @Override
  public void close()
  {
    cacheManager.disposeCache(this);
    // Log statement after disposeCache(), because logging may fail (e. g. in shutdown hooks)
    log.debug("Closed %s", super.toString());
  }
}
