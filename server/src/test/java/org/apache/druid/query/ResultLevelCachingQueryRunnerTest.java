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

package org.apache.druid.query;

import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ResultLevelCachingQueryRunnerTest extends QueryRunnerBasedOnClusteredClientTestBase
{
  private Cache cache;

  @Before
  public void setup()
  {
    cache = MapCache.create(1024);
  }

  @After
  public void tearDown() throws IOException
  {
    cache.close();
  }

  @Test
  public void testNotPopulateAndNotUse()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(false, true),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(false, true),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(0, cache.getStats().getNumHits());
  }

  @Test
  public void testPopulateAndNotUse()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(true, true),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, false),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(0, cache.getStats().getNumHits());
  }

  @Test
  public void testNotPopulateAndUse()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(false, true),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, true),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(0, cache.getStats().getNumHits());
  }

  @Test
  public void testPopulateAndUse()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(true, true),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, true),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(1, cache.getStats().getNumHits());
  }

  private <T> ResultLevelCachingQueryRunner<T> createQueryRunner(
      CacheConfig cacheConfig,
      Query<T> query
  )
  {
    final QueryRunner<T> baseRunner = cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals());
    return new ResultLevelCachingQueryRunner<>(
        new RetryQueryRunner<>(
            baseRunner,
            cachingClusteredClient::getQueryRunnerForSegments,
            new RetryQueryRunnerConfig(),
            objectMapper
        ),
        toolChestWarehouse.getToolChest(query),
        query,
        objectMapper,
        cache,
        cacheConfig
    );
  }

  private CacheConfig newCacheConfig(boolean populateResultLevelCache, boolean useResultLevelCache)
  {
    return new CacheConfig()
    {
      @Override
      public boolean isPopulateResultLevelCache()
      {
        return populateResultLevelCache;
      }

      @Override
      public boolean isUseResultLevelCache()
      {
        return useResultLevelCache;
      }
    };
  }
}
