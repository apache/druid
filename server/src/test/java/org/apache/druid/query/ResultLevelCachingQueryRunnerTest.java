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

import org.apache.druid.client.SimpleServerView;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.collections.BlockingPool;
import org.apache.druid.collections.DefaultBlockingPool;
import org.apache.druid.collections.ReferenceCountingResourceHolder;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

import static org.junit.Assert.fail;

public class ResultLevelCachingQueryRunnerTest extends QueryRunnerBasedOnClusteredClientTestBase
{
  private Cache cache;
  private static final int DEFAULT_CACHE_ENTRY_MAX_SIZE = Integer.MAX_VALUE;

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
        newCacheConfig(false, false, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumMisses());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(false, false, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumMisses());
  }

  @Test
  public void testPopulateAndNotUse()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(true, false, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumMisses());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, false, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumMisses());
  }

  @Test
  public void testNotPopulateAndUse()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(false, false, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(0, cache.getStats().getNumMisses());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(false, true, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(1, cache.getStats().getNumMisses());
  }

  @Test
  public void testPopulateAndUse()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(1, cache.getStats().getNumMisses());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(1, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(1, cache.getStats().getNumMisses());
  }

  @Test
  public void testNoPopulateIfEntrySizeExceedsMaximum()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(true, true, 128),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(1, cache.getStats().getNumMisses());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(2, cache.getStats().getNumMisses());
  }

  @Test
  public void testPopulateCacheWhenQueryThrowExceptionShouldNotCache()
  {
    final Interval interval = Intervals.of("2000-01-01/PT1H");
    final DataSegment segment = newSegment(interval, 0, 1);
    addServer(
        SimpleServerView.createServer(0),
        segment,
        generateSegment(segment),
        true
    );

    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner = createQueryRunner(
        newCacheConfig(true, false, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence = queryRunner.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    try {
      sequence.toList();
      fail("Expected to throw an exception");
    }
    catch (RuntimeException e) {
      Assert.assertEquals("Exception for testing", e.getMessage());
    }
    finally {
      Assert.assertEquals(0, cache.getStats().getNumHits());
      Assert.assertEquals(0, cache.getStats().getNumEntries());
      Assert.assertEquals(0, cache.getStats().getNumMisses());
    }
  }

  @Test
  public void testUseCacheAndReleaseResourceFromClient()
  {
    final BlockingPool<ByteBuffer> mergePool = new DefaultBlockingPool<>(() -> ByteBuffer.allocate(1), 1);
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    CacheConfig cacheConfig = newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE);
    final QueryRunner<Result<TimeseriesResultValue>> baseRunner = cachingClusteredClient.getQueryRunnerForIntervals(query, query.getIntervals());
    RetryQueryRunner<Result<TimeseriesResultValue>> spyRunner = Mockito.spy(new RetryQueryRunner<>(
        baseRunner,
        cachingClusteredClient::getQueryRunnerForSegments,
        new RetryQueryRunnerConfig(),
        objectMapper
    ));
    Mockito.doAnswer((Answer<Object>) invocation -> {
      List<ReferenceCountingResourceHolder<ByteBuffer>> resoruce = mergePool.takeBatch(1, 1);
      if (resoruce.isEmpty()) {
        fail("Resource should not be empty");
      }
      Sequence<Result<TimeseriesResultValue>> realSequence = (Sequence<Result<TimeseriesResultValue>>) invocation.callRealMethod();
      Closer closer = Closer.create();
      closer.register(() -> resoruce.forEach(ReferenceCountingResourceHolder::close));
      return Sequences.withBaggage(realSequence, closer);
    }).when(spyRunner).run(ArgumentMatchers.any(), ArgumentMatchers.any());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = new ResultLevelCachingQueryRunner<>(
        spyRunner,
        conglomerate.getToolChest(query),
        query,
        objectMapper,
        cache,
        cacheConfig
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(1, cache.getStats().getNumMisses());


    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assert.assertEquals(results1, results2);
    Assert.assertEquals(1, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(1, cache.getStats().getNumMisses());

    final Sequence<Result<TimeseriesResultValue>> sequence3 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results3 = sequence3.toList();
    Assert.assertEquals(results1, results3);
    Assert.assertEquals(2, cache.getStats().getNumHits());
    Assert.assertEquals(1, cache.getStats().getNumEntries());
    Assert.assertEquals(1, cache.getStats().getNumMisses());
  }

  @Test
  public void testPopulateCacheThrowsException()
  {
    cache = Mockito.spy(cache);
    Mockito.doThrow(new RuntimeException("some error")).when(cache).put(ArgumentMatchers.any(), ArgumentMatchers.any());

    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = createQueryRunner(
        newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    ).toList();
    Assert.assertEquals(0, cache.getStats().getNumHits());
    Assert.assertEquals(0, cache.getStats().getNumEntries());
    Assert.assertEquals(1, cache.getStats().getNumMisses());
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
        conglomerate.getToolChest(query),
        query,
        objectMapper,
        cache,
        cacheConfig
    );
  }

  private CacheConfig newCacheConfig(
      boolean populateResultLevelCache,
      boolean useResultLevelCache,
      int resultLevelCacheLimit
  )
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

      @Override
      public int getResultLevelCacheLimit()
      {
        return resultLevelCacheLimit;
      }
    };
  }
}
