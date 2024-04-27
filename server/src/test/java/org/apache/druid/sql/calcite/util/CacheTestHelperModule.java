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
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.server.EtagProvider;

public class CacheTestHelperModule extends AbstractModule
{

  public enum ResultCacheMode
  {
    DISABLED,
    ENABLED;

    public Module makeModule()
    {
      return new CacheTestHelperModule(this);
    }

    public boolean isPopulateResultLevelCache()
    {
      return this != DISABLED;
    }

    public boolean isUseResultLevelCache()
    {
      return this != DISABLED;
    }
  }

  protected final Cache cache;
  private CacheConfig cacheConfig;
  private EtagProvider etagProvider;

  static class TestCacheConfig extends CacheConfig
  {
    private ResultCacheMode resultLevelCache;

    public TestCacheConfig(ResultCacheMode resultCacheMode)
    {
      this.resultLevelCache = resultCacheMode;
    }

    @Override
    public boolean isPopulateResultLevelCache()
    {
      return resultLevelCache.isPopulateResultLevelCache();
    }

    @Override
    public boolean isUseResultLevelCache()
    {
      return resultLevelCache.isUseResultLevelCache();
    }

  }

  public CacheTestHelperModule(ResultCacheMode resultCacheMode)
  {
    cacheConfig = new TestCacheConfig(resultCacheMode);

    switch (resultCacheMode) {
      case ENABLED:
        etagProvider = new EtagProvider.ProvideEtagBasedOnDatasource();
        cache = MapCache.create(1_000_000L);
        break;
      case DISABLED:
        etagProvider = new EtagProvider.EmptyEtagProvider();
        cache = null;
        break;
      default:
        throw new RuntimeException();
    }
  }

  @Provides
  EtagProvider etagProvider()
  {
    return etagProvider;
  }

  @Provides
  CacheConfig getCacheConfig()
  {
    return cacheConfig;
  }

  @Provides
  Cache getCache()
  {
    return cache;
  }

  @Override
  public void configure()
  {
  }
}
