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
import org.apache.druid.query.spec.MultipleIntervalSegmentSpec;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class ResultLevelCachingQueryRunnerTest extends QueryRunnerBasedOnClusteredClientTestBase
{
  private Cache cache;
  private static final int DEFAULT_CACHE_ENTRY_MAX_SIZE = Integer.MAX_VALUE;

  @BeforeEach
  public void setup()
  {
    cache = MapCache.create(1024);
  }

  @AfterEach
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
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(0, cache.getStats().getNumEntries());
    Assertions.assertEquals(0, cache.getStats().getNumMisses());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(false, false, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assertions.assertEquals(results1, results2);
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(0, cache.getStats().getNumEntries());
    Assertions.assertEquals(0, cache.getStats().getNumMisses());

    emitter.verifyNotEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT);
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
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(0, cache.getStats().getNumMisses());

    emitter.verifyNotEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT);
    emitter.flush();

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, false, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assertions.assertEquals(results1, results2);
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(0, cache.getStats().getNumMisses());

    emitter.verifyNotEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT);
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
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(0, cache.getStats().getNumEntries());
    Assertions.assertEquals(0, cache.getStats().getNumMisses());

    emitter.verifyNotEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT);
    emitter.flush();

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(false, true, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assertions.assertEquals(results1, results2);
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(0, cache.getStats().getNumEntries());
    Assertions.assertEquals(1, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);
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
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(1, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);
    emitter.flush();

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assertions.assertEquals(results1, results2);
    Assertions.assertEquals(1, cache.getStats().getNumHits());
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(1, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
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
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(0, cache.getStats().getNumEntries());
    Assertions.assertEquals(1, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);
    emitter.flush();

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner2 = createQueryRunner(
        newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE),
        query
    );

    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner2.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assertions.assertEquals(results1, results2);
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(2, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);
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
      Assertions.fail("Expected to throw an exception");
    }
    catch (RuntimeException e) {
      Assertions.assertEquals("Exception for testing", e.getMessage());
    }
    finally {
      Assertions.assertEquals(0, cache.getStats().getNumHits());
      Assertions.assertEquals(0, cache.getStats().getNumEntries());
      Assertions.assertEquals(0, cache.getStats().getNumMisses());

      emitter.verifyNotEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT);
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
      List<ReferenceCountingResourceHolder<ByteBuffer>> resource = mergePool.takeBatch(1, 1);
      if (resource.isEmpty()) {
        Assertions.fail("Resource should not be empty");
      }
      Sequence<Result<TimeseriesResultValue>> realSequence = (Sequence<Result<TimeseriesResultValue>>) invocation.callRealMethod();
      Closer closer = Closer.create();
      closer.register(() -> resource.forEach(ReferenceCountingResourceHolder::close));
      return Sequences.withBaggage(realSequence, closer);
    }).when(spyRunner).run(ArgumentMatchers.any(), ArgumentMatchers.any());

    final ResultLevelCachingQueryRunner<Result<TimeseriesResultValue>> queryRunner1 = new ResultLevelCachingQueryRunner<>(
        spyRunner,
        conglomerate.getToolChest(query),
        query,
        objectMapper,
        cache,
        cacheConfig,
        emitter
    );

    final Sequence<Result<TimeseriesResultValue>> sequence1 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results1 = sequence1.toList();
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(1, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);
    emitter.flush();


    final Sequence<Result<TimeseriesResultValue>> sequence2 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results2 = sequence2.toList();
    Assertions.assertEquals(results1, results2);
    Assertions.assertEquals(1, cache.getStats().getNumHits());
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(1, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.flush();

    final Sequence<Result<TimeseriesResultValue>> sequence3 = queryRunner1.run(
        QueryPlus.wrap(query),
        responseContext()
    );
    final List<Result<TimeseriesResultValue>> results3 = sequence3.toList();
    Assertions.assertEquals(results1, results3);
    Assertions.assertEquals(2, cache.getStats().getNumHits());
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(1, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
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
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    Assertions.assertEquals(0, cache.getStats().getNumEntries());
    Assertions.assertEquals(1, cache.getStats().getNumMisses());

    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);
  }

  @Test
  public void testDifferentIntervalsShareOneEntryByDefault()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query1 = timeseriesQuery(Intervals.of("2000-01-01T00/PT2H"));
    final Query<Result<TimeseriesResultValue>> query2 = timeseriesQuery(Intervals.of("2000-01-01T01/PT2H"));
    final CacheConfig config = newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE);

    createQueryRunner(config, query1).run(QueryPlus.wrap(query1), responseContext()).toList();
    createQueryRunner(config, query2).run(QueryPlus.wrap(query2), responseContext()).toList();
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    emitter.flush();

    createQueryRunner(config, query1).run(QueryPlus.wrap(query1), responseContext()).toList();

    // query2 overwrote query1's entry, so the last run finds an entry (a hit for the cache) but can't use it.
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    Assertions.assertEquals(2, cache.getStats().getNumHits());
    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);
  }

  @Test
  public void testDifferentIntervalsUseSeparateEntriesWhenFlagEnabled()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query1 = timeseriesQuery(Intervals.of("2000-01-01T00/PT2H"));
    final Query<Result<TimeseriesResultValue>> query2 = timeseriesQuery(Intervals.of("2000-01-01T01/PT2H"));
    final CacheConfig config = newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE, true);

    final List<Result<TimeseriesResultValue>> results1 =
        createQueryRunner(config, query1).run(QueryPlus.wrap(query1), responseContext()).toList();
    final List<Result<TimeseriesResultValue>> results2 =
        createQueryRunner(config, query2).run(QueryPlus.wrap(query2), responseContext()).toList();
    Assertions.assertEquals(2, cache.getStats().getNumEntries());
    emitter.flush();

    Assertions.assertEquals(
        results1,
        createQueryRunner(config, query1).run(QueryPlus.wrap(query1), responseContext()).toList()
    );
    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.flush();

    Assertions.assertEquals(
        results2,
        createQueryRunner(config, query2).run(QueryPlus.wrap(query2), responseContext()).toList()
    );
    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    Assertions.assertEquals(2, cache.getStats().getNumEntries());
  }

  @Test
  public void testDatasourceAndIntervalsInCacheKeyOnlyWhenFlagEnabled()
  {
    cache = Mockito.spy(cache);
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(Intervals.of("2000-01-01T00/PT2H"));
    final Query<Result<TimeseriesResultValue>> sameQuery = timeseriesQuery(Intervals.of("2000-01-01T00/PT2H"));
    final Query<Result<TimeseriesResultValue>> otherInterval = timeseriesQuery(Intervals.of("2000-01-01T01/PT2H"));
    final Query<Result<TimeseriesResultValue>> otherEnd = timeseriesQuery(Intervals.of("2000-01-01T00/PT3H"));
    final Query<Result<TimeseriesResultValue>> otherStart = timeseriesQuery(Intervals.of("2000-01-01T01/PT1H"));
    final Query<Result<TimeseriesResultValue>> otherDatasource = query.withDataSource(TableDataSource.create("other"));

    final Cache.NamedKey key = resultLevelCacheKey(query, false);
    Assertions.assertEquals(key, resultLevelCacheKey(otherInterval, false));
    Assertions.assertEquals(key, resultLevelCacheKey(otherEnd, false));
    Assertions.assertEquals(key, resultLevelCacheKey(otherStart, false));
    Assertions.assertEquals(key, resultLevelCacheKey(otherDatasource, false));

    final Cache.NamedKey keyWithFlag = resultLevelCacheKey(query, true);
    Assertions.assertNotEquals(key, keyWithFlag);
    Assertions.assertEquals(keyWithFlag, resultLevelCacheKey(sameQuery, true));
    Assertions.assertNotEquals(keyWithFlag, resultLevelCacheKey(otherInterval, true));
    Assertions.assertNotEquals(keyWithFlag, resultLevelCacheKey(otherEnd, true));
    Assertions.assertNotEquals(keyWithFlag, resultLevelCacheKey(otherStart, true));
    Assertions.assertNotEquals(keyWithFlag, resultLevelCacheKey(otherDatasource, true));
  }

  @Test
  public void testCacheKeyUsesCondensedIntervalsWhenFlagEnabled()
  {
    cache = Mockito.spy(cache);
    prepareCluster(10);
    final Interval first = Intervals.of("2000-01-01T00/PT2H");
    final Interval later = Intervals.of("2000-01-01T03/PT2H");

    // A query sorts and merges its intervals, so their order doesn't change the key, and intervals that abut or
    // overlap get the key of the single interval they merge into.
    final Cache.NamedKey withGap = resultLevelCacheKey(timeseriesQuery(List.of(first, later)), true);
    Assertions.assertEquals(withGap, resultLevelCacheKey(timeseriesQuery(List.of(later, first)), true));
    Assertions.assertEquals(
        resultLevelCacheKey(timeseriesQuery(Intervals.of("2000-01-01T00/PT4H")), true),
        resultLevelCacheKey(timeseriesQuery(List.of(first, Intervals.of("2000-01-01T02/PT2H"))), true)
    );
    Assertions.assertEquals(
        resultLevelCacheKey(timeseriesQuery(Intervals.of("2000-01-01T00/PT3H")), true),
        resultLevelCacheKey(timeseriesQuery(List.of(first, Intervals.of("2000-01-01T01/PT2H"))), true)
    );

    // Intervals with a gap between them stay separate.
    Assertions.assertNotEquals(resultLevelCacheKey(timeseriesQuery(first), true), withGap);
    Assertions.assertNotEquals(resultLevelCacheKey(timeseriesQuery(Intervals.of("2000-01-01T00/PT5H")), true), withGap);
  }

  @Test
  public void testStaleEntryRejectedAfterTurningFlagOnAndOff()
  {
    prepareCluster(10);
    final Query<Result<TimeseriesResultValue>> query = timeseriesQuery(BASE_SCHEMA_INFO.getDataInterval());
    final CacheConfig flagOff = newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE, false);
    final CacheConfig flagOn = newCacheConfig(true, true, DEFAULT_CACHE_ENTRY_MAX_SIZE, true);

    // Each run builds a new runner over the same cache, as a Broker restarting with an external cache would.
    final List<Result<TimeseriesResultValue>> results1 =
        createQueryRunner(flagOff, query).run(QueryPlus.wrap(query), responseContext()).toList();
    Assertions.assertEquals(1, cache.getStats().getNumEntries());
    emitter.flush();

    // With the flag on, the entry stored under the default key isn't read.
    Assertions.assertEquals(
        results1,
        createQueryRunner(flagOn, query).run(QueryPlus.wrap(query), responseContext()).toList()
    );
    Assertions.assertEquals(2, cache.getStats().getNumEntries());
    Assertions.assertEquals(0, cache.getStats().getNumHits());
    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);

    // A new segment changes the data, so the ETag stored with the entry under the default key no longer matches.
    final DataSegment segment = newSegment(Intervals.of("2000-01-01T05/PT1H"), 0, 1);
    addServer(SimpleServerView.createServer(11), segment, generateSegment(segment));
    final List<Result<TimeseriesResultValue>> currentResults =
        createQueryRunner(newCacheConfig(false, false, DEFAULT_CACHE_ENTRY_MAX_SIZE), query)
            .run(QueryPlus.wrap(query), responseContext())
            .toList();
    Assertions.assertNotEquals(results1, currentResults);
    emitter.flush();

    // With the flag off again, the old entry is found but rejected, and the query returns the current results.
    Assertions.assertEquals(
        currentResults,
        createQueryRunner(flagOff, query).run(QueryPlus.wrap(query), responseContext()).toList()
    );
    Assertions.assertEquals(1, cache.getStats().getNumHits());
    emitter.verifyEmitted(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 1);
    emitter.verifyValue(DefaultQueryMetrics.QUERY_RESULT_CACHE_HIT, 0);
  }

  private static Query<Result<TimeseriesResultValue>> timeseriesQuery(List<Interval> intervals)
  {
    return timeseriesQuery(intervals.get(0)).withQuerySegmentSpec(new MultipleIntervalSegmentSpec(intervals));
  }

  /**
   * Runs the query with the result-level cache in use-only mode and returns the key it looked up. Requires
   * {@link #cache} to be a Mockito spy.
   */
  private <T> Cache.NamedKey resultLevelCacheKey(Query<T> query, boolean includeDatasourceAndIntervals)
  {
    Mockito.clearInvocations(cache);
    createQueryRunner(
        newCacheConfig(false, true, DEFAULT_CACHE_ENTRY_MAX_SIZE, includeDatasourceAndIntervals),
        query
    ).run(QueryPlus.wrap(query), responseContext()).toList();
    final ArgumentCaptor<Cache.NamedKey> captor = ArgumentCaptor.forClass(Cache.NamedKey.class);
    Mockito.verify(cache).get(captor.capture());
    return captor.getValue();
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
        cacheConfig,
        emitter
    );
  }

  private CacheConfig newCacheConfig(
      boolean populateResultLevelCache,
      boolean useResultLevelCache,
      int resultLevelCacheLimit
  )
  {
    return newCacheConfig(populateResultLevelCache, useResultLevelCache, resultLevelCacheLimit, false);
  }

  private CacheConfig newCacheConfig(
      boolean populateResultLevelCache,
      boolean useResultLevelCache,
      int resultLevelCacheLimit,
      boolean includeDatasourceAndIntervals
  )
  {
    return new CacheConfig()
    {
      @Override
      public boolean isIncludeDatasourceAndIntervalsInResultLevelCacheKey()
      {
        return includeDatasourceAndIntervals;
      }

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
