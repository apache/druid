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

package org.apache.druid.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class CacheUtilTest
{
  private final TimeseriesQuery timeseriesQuery =
      Druids.newTimeseriesQueryBuilder()
            .dataSource("foo")
            .intervals("2000/3000")
            .granularity(Granularities.ALL)
            .build();

  @Test
  public void test_isQueryCacheable_cacheableOnBroker()
  {
    Assert.assertTrue(
        CacheUtil.isQueryCacheable(
            timeseriesQuery,
            new DummyCacheStrategy<>(true, true),
            makeCacheConfig(ImmutableMap.of()),
            CacheUtil.ServerType.BROKER
        )
    );
  }

  @Test
  public void test_isQueryCacheable_cacheableOnDataServer()
  {
    Assert.assertTrue(
        CacheUtil.isQueryCacheable(
            timeseriesQuery,
            new DummyCacheStrategy<>(true, true),
            makeCacheConfig(ImmutableMap.of()),
            CacheUtil.ServerType.DATA
        )
    );
  }

  @Test
  public void test_isQueryCacheable_unCacheableOnBroker()
  {
    Assert.assertFalse(
        CacheUtil.isQueryCacheable(
            timeseriesQuery,
            new DummyCacheStrategy<>(false, true),
            makeCacheConfig(ImmutableMap.of()),
            CacheUtil.ServerType.BROKER
        )
    );
  }

  @Test
  public void test_isQueryCacheable_unCacheableOnDataServer()
  {
    Assert.assertFalse(
        CacheUtil.isQueryCacheable(
            timeseriesQuery,
            new DummyCacheStrategy<>(true, false),
            makeCacheConfig(ImmutableMap.of()),
            CacheUtil.ServerType.DATA
        )
    );
  }

  @Test
  public void test_isQueryCacheable_unCacheableType()
  {
    Assert.assertFalse(
        CacheUtil.isQueryCacheable(
            timeseriesQuery,
            new DummyCacheStrategy<>(true, false),
            makeCacheConfig(ImmutableMap.of("unCacheable", ImmutableList.of("timeseries"))),
            CacheUtil.ServerType.BROKER
        )
    );
  }

  @Test
  public void test_isQueryCacheable_unCacheableDataSource()
  {
    Assert.assertFalse(
        CacheUtil.isQueryCacheable(
            timeseriesQuery.withDataSource(new LookupDataSource("lookyloo")),
            new DummyCacheStrategy<>(true, true),
            makeCacheConfig(ImmutableMap.of()),
            CacheUtil.ServerType.BROKER
        )
    );
  }

  @Test
  public void test_isQueryCacheable_nullCacheStrategy()
  {
    Assert.assertFalse(
        CacheUtil.isQueryCacheable(
            timeseriesQuery,
            null,
            makeCacheConfig(ImmutableMap.of()),
            CacheUtil.ServerType.BROKER
        )
    );
  }

  private static CacheConfig makeCacheConfig(final Map<String, Object> properties)
  {
    return TestHelper.makeJsonMapper().convertValue(properties, CacheConfig.class);
  }

  private static class DummyCacheStrategy<T, CacheType, QueryType extends Query<T>>
      implements CacheStrategy<T, CacheType, QueryType>
  {
    private final boolean cacheableOnBrokers;
    private final boolean cacheableOnDataServers;

    public DummyCacheStrategy(boolean cacheableOnBrokers, boolean cacheableOnDataServers)
    {
      this.cacheableOnBrokers = cacheableOnBrokers;
      this.cacheableOnDataServers = cacheableOnDataServers;
    }

    @Override
    public boolean isCacheable(QueryType query, boolean willMergeRunners)
    {
      return willMergeRunners ? cacheableOnDataServers : cacheableOnBrokers;
    }

    @Override
    public byte[] computeCacheKey(QueryType query)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[] computeResultLevelCacheKey(QueryType query)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public TypeReference<CacheType> getCacheObjectClazz()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Function<T, CacheType> prepareForCache(boolean isResultLevelCache)
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Function<CacheType, T> pullFromCache(boolean isResultLevelCache)
    {
      throw new UnsupportedOperationException();
    }
  }
}
