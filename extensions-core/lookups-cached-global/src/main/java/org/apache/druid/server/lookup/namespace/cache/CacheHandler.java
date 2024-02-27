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

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.lookup.LookupExtractor;

import java.io.Closeable;
import java.util.Map;
import java.util.function.Supplier;

public final class CacheHandler implements AutoCloseable, Closeable
{
  private static final Logger log = new Logger(CacheHandler.class);

  private final NamespaceExtractionCacheManager cacheManager;
  private final Map<String, String> cache;
  final Object id;

  CacheHandler(NamespaceExtractionCacheManager cacheManager, Map<String, String> cache, Object id)
  {
    log.debug("Creating %s", super.toString());
    this.cacheManager = cacheManager;
    this.cache = cache;
    this.id = id;
  }

  public Map<String, String> getCache()
  {
    return cache;
  }

  /**
   * Returns a {@link LookupExtractor} view of the cached data.
   */
  public LookupExtractor asLookupExtractor(final boolean isOneToOne, final Supplier<byte[]> cacheKeySupplier)
  {
    return cacheManager.asLookupExtractor(this, isOneToOne, cacheKeySupplier);
  }

  @Override
  public void close()
  {
    cacheManager.disposeCache(this);
    // Log statement after disposeCache(), because logging may fail (e.g. in shutdown hooks)
    log.debug("Closed %s", super.toString());
  }
}
