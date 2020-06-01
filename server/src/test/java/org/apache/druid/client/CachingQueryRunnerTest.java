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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.druid.client.cache.BackgroundCachePopulator;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.client.cache.CachePopulator;
import org.apache.druid.client.cache.CachePopulatorStats;
import org.apache.druid.client.cache.CacheStats;
import org.apache.druid.client.cache.ForegroundCachePopulator;
import org.apache.druid.client.cache.MapCache;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceWrapper;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.CacheStrategy;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.query.topn.TopNQueryBuilder;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.query.topn.TopNQueryQueryToolChest;
import org.apache.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public class CachingQueryRunnerTest
{
  @Parameterized.Parameters(name = "numBackgroundThreads={0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return QueryRunnerTestHelper.cartesian(Arrays.asList(5, 1, 0));
  }

  private static final List<AggregatorFactory> AGGS = Arrays.asList(
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("imps", "imps"),
      new LongSumAggregatorFactory("impers", "imps")
  );

  private static final Object[] OBJECTS = new Object[]{
      DateTimes.of("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
      DateTimes.of("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
      DateTimes.of("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
      DateTimes.of("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
      DateTimes.of("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
  };

  private ObjectMapper objectMapper;
  private CachePopulator cachePopulator;

  public CachingQueryRunnerTest(int numBackgroundThreads)
  {
    objectMapper = new DefaultObjectMapper();

    if (numBackgroundThreads > 0) {
      cachePopulator = new BackgroundCachePopulator(
          Execs.multiThreaded(numBackgroundThreads, "CachingQueryRunnerTest-%d"),
          objectMapper,
          new CachePopulatorStats(),
          -1
      );
    } else {
      cachePopulator = new ForegroundCachePopulator(objectMapper, new CachePopulatorStats(), -1);
    }
  }

  @Test
  public void testCloseAndPopulate() throws Exception
  {
    List<Result> expectedRes = makeTopNResults(false, OBJECTS);
    List<Result> expectedCacheRes = makeTopNResults(true, OBJECTS);

    TopNQueryBuilder builder = new TopNQueryBuilder()
        .dataSource("ds")
        .dimension("top_dim")
        .metric("imps")
        .threshold(3)
        .intervals("2011-01-05/2011-01-10")
        .aggregators(AGGS)
        .granularity(Granularities.ALL);

    QueryToolChest toolchest = new TopNQueryQueryToolChest(new TopNQueryConfig());

    testCloseAndPopulate(expectedRes, expectedCacheRes, builder.build(), toolchest);
    testUseCache(expectedCacheRes, builder.build(), toolchest);
  }

  @Test
  public void testTimeseries() throws Exception
  {
    for (boolean descending : new boolean[]{false, true}) {
      TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.DATA_SOURCE)
                                    .granularity(QueryRunnerTestHelper.DAY_GRAN)
                                    .intervals(QueryRunnerTestHelper.FIRST_TO_THIRD)
                                    .aggregators(
                                        Arrays.asList(
                                            QueryRunnerTestHelper.ROWS_COUNT,
                                            new LongSumAggregatorFactory(
                                                "idx",
                                                "index"
                                            ),
                                            QueryRunnerTestHelper.QUALITY_UNIQUES
                                        )
                                    )
                                    .descending(descending)
                                    .build();

      Result row1 = new Result(
          DateTimes.of("2011-04-01"),
          new TimeseriesResultValue(
              ImmutableMap.of("rows", 13L, "idx", 6619L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
          )
      );
      Result row2 = new Result<>(
          DateTimes.of("2011-04-02"),
          new TimeseriesResultValue(
              ImmutableMap.of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
          )
      );
      List<Result> expectedResults;
      if (descending) {
        expectedResults = Lists.newArrayList(row2, row1);
      } else {
        expectedResults = Lists.newArrayList(row1, row2);
      }

      QueryToolChest toolChest = new TimeseriesQueryQueryToolChest();

      testCloseAndPopulate(expectedResults, expectedResults, query, toolChest);
      testUseCache(expectedResults, query, toolChest);
    }
  }

  private void testCloseAndPopulate(
      List<Result> expectedRes,
      List<Result> expectedCacheRes,
      Query query,
      QueryToolChest toolchest
  )
      throws Exception
  {
    final AssertingClosable closable = new AssertingClosable();
    final Sequence resultSeq = Sequences.wrap(
        Sequences.simple(expectedRes),
        new SequenceWrapper()
        {
          @Override
          public void before()
          {
            Assert.assertFalse(closable.isClosed());
          }

          @Override
          public void after(boolean isDone, Throwable thrown)
          {
            closable.close();
          }
        }
    );

    final CountDownLatch cacheMustBePutOnce = new CountDownLatch(1);
    Cache cache = new Cache()
    {
      private final ConcurrentMap<NamedKey, byte[]> baseMap = new ConcurrentHashMap<>();

      @Override
      public byte[] get(NamedKey key)
      {
        return baseMap.get(key);
      }

      @Override
      public void put(NamedKey key, byte[] value)
      {
        baseMap.put(key, value);
        cacheMustBePutOnce.countDown();
      }

      @Override
      public Map<NamedKey, byte[]> getBulk(Iterable<NamedKey> keys)
      {
        return null;
      }

      @Override
      public void close(String namespace)
      {
      }

      @Override
      public void close()
      {
      }

      @Override
      public CacheStats getStats()
      {
        return null;
      }

      @Override
      public boolean isLocal()
      {
        return true;
      }

      @Override
      public void doMonitor(ServiceEmitter emitter)
      {
      }
    };

    String cacheId = "segment";
    SegmentDescriptor segmentDescriptor = new SegmentDescriptor(Intervals.of("2011/2012"), "version", 0);

    DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    CachingQueryRunner runner = new CachingQueryRunner(
        cacheId,
        segmentDescriptor,
        objectMapper,
        cache,
        toolchest,
        new QueryRunner()
        {
          @Override
          public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
          {
            return resultSeq;
          }
        },
        cachePopulator,
        new CacheConfig()
        {
          @Override
          public boolean isPopulateCache()
          {
            return true;
          }

          @Override
          public boolean isUseCache()
          {
            return true;
          }
        }
    );

    CacheStrategy cacheStrategy = toolchest.getCacheStrategy(query);
    Cache.NamedKey cacheKey = CacheUtil.computeSegmentCacheKey(
        cacheId,
        segmentDescriptor,
        cacheStrategy.computeCacheKey(query)
    );

    Sequence res = runner.run(QueryPlus.wrap(query));
    // base sequence is not closed yet
    Assert.assertFalse("sequence must not be closed", closable.isClosed());
    Assert.assertNull("cache must be empty", cache.get(cacheKey));

    List results = res.toList();
    Assert.assertTrue(closable.isClosed());
    Assert.assertEquals(expectedRes.toString(), results.toString());

    // wait for background caching finish
    // wait at most 10 seconds to fail the test to avoid block overall tests
    Assert.assertTrue("cache must be populated", cacheMustBePutOnce.await(10, TimeUnit.SECONDS));
    byte[] cacheValue = cache.get(cacheKey);
    Assert.assertNotNull(cacheValue);

    Function<Object, Result> fn = cacheStrategy.pullFromSegmentLevelCache();
    List<Result> cacheResults = Lists.newArrayList(
        Iterators.transform(
            objectMapper.readValues(
                objectMapper.getFactory().createParser(cacheValue),
                cacheStrategy.getCacheObjectClazz()
            ),
            fn
        )
    );
    Assert.assertEquals(expectedCacheRes.toString(), cacheResults.toString());
  }

  private void testUseCache(
      List<Result> expectedResults,
      Query query,
      QueryToolChest toolchest
  ) throws IOException
  {
    DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    String cacheId = "segment";
    SegmentDescriptor segmentDescriptor = new SegmentDescriptor(Intervals.of("2011/2012"), "version", 0);

    CacheStrategy cacheStrategy = toolchest.getCacheStrategy(query);
    Cache.NamedKey cacheKey = CacheUtil.computeSegmentCacheKey(
        cacheId,
        segmentDescriptor,
        cacheStrategy.computeCacheKey(query)
    );

    Cache cache = MapCache.create(1024 * 1024);
    cache.put(cacheKey, toByteArray(Iterables.transform(expectedResults, cacheStrategy.prepareForSegmentLevelCache())));

    CachingQueryRunner runner = new CachingQueryRunner(
        cacheId,
        segmentDescriptor,
        objectMapper,
        cache,
        toolchest,
        // return an empty sequence since results should get pulled from cache
        new QueryRunner()
        {
          @Override
          public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
          {
            return Sequences.empty();
          }
        },
        cachePopulator,
        new CacheConfig()
        {
          @Override
          public boolean isPopulateCache()
          {
            return true;
          }

          @Override
          public boolean isUseCache()
          {
            return true;
          }
        }

    );
    List<Result> results = runner.run(QueryPlus.wrap(query)).toList();
    Assert.assertEquals(expectedResults.toString(), results.toString());
  }

  private List<Result> makeTopNResults(boolean cachedResults, Object... objects)
  {
    List<Result> retVal = new ArrayList<>();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<Map<String, Object>> values = new ArrayList<>();
      while (index < objects.length && !(objects[index] instanceof DateTime)) {
        if (objects.length - index < 3) {
          throw new ISE(
              "expect 3 values for each entry in the top list, had %d values left.", objects.length - index
          );
        }
        final double imps = ((Number) objects[index + 2]).doubleValue();
        final double rows = ((Number) objects[index + 1]).doubleValue();

        if (cachedResults) {
          values.add(
              ImmutableMap.of(
                  "top_dim", objects[index],
                  "rows", rows,
                  "imps", imps,
                  "impers", imps
              )
          );
        } else {
          values.add(
              ImmutableMap.of(
                  "top_dim", objects[index],
                  "rows", rows,
                  "imps", imps,
                  "impers", imps,
                  "avg_imps_per_row", imps / rows
              )
          );
        }
        index += 3;
      }

      retVal.add(new Result<>(timestamp, new TopNResultValue(values)));
    }
    return retVal;
  }

  private <T> byte[] toByteArray(final Iterable<T> results) throws IOException
  {
    final ByteArrayOutputStream bytes = new ByteArrayOutputStream();

    try (JsonGenerator gen = objectMapper.getFactory().createGenerator(bytes)) {
      for (T result : results) {
        gen.writeObject(result);
      }
    }

    return bytes.toByteArray();
  }

  private static class AssertingClosable implements Closeable
  {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void close()
    {
      Assert.assertFalse(closed.get());
      Assert.assertTrue(closed.compareAndSet(false, true));
    }

    public boolean isClosed()
    {
      return closed.get();
    }
  }

}
