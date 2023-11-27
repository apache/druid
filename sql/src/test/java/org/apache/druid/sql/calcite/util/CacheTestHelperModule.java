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

package org.apache.druid.sql.calcite.util;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.server.EtagProvider;
import org.apache.druid.server.QueryStackTests.Testrelated;

public class CacheTestHelperModule extends AbstractModule
{

  protected final Cache cache;
  private CacheConfig cacheConfig;
  private EtagProvider etagProvider;

  static class TestCacheConfig extends CacheConfig
  {
    private boolean enableResultLevelCache;

    public TestCacheConfig(boolean enableResultLevelCache)
    {
      this.enableResultLevelCache = enableResultLevelCache;
    }

    @Override
    public boolean isPopulateResultLevelCache()
    {
      return enableResultLevelCache;

    }

    @Override
    public boolean isUseResultLevelCache()
    {
      return enableResultLevelCache;
    }

  }

  public CacheTestHelperModule(boolean enableResultLevelCache)
  {
    cacheConfig = new TestCacheConfig(enableResultLevelCache);

    if (enableResultLevelCache) {
      etagProvider = new EtagProvider.UseProvidedIfAvaliable();
      cache = MapCache.create(1_000_000L);
    } else {
      etagProvider = new EtagProvider.EmptyEtagProvider();
      cache = null;
    }
  }

  @Provides
  @EtagProvider.Annotation
  EtagProvider etagProvider()
  {
    return etagProvider;
  }

  @Provides
  @Testrelated
  CacheConfig getCacheConfig()
  {
    return cacheConfig;
  }

  @Provides
  @Testrelated
  Cache getCache()
  {
    return cache;
  }

  @Override
  public void configure()
  {
  }
}
