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

package io.druid.client;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.CacheStats;
import io.druid.client.cache.MapCache;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.granularity.Granularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.SequenceWrapper;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.CacheStrategy;
import io.druid.query.Druids;
import io.druid.query.Query;
import io.druid.query.QueryPlus;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.QueryToolChest;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.timeseries.TimeseriesQuery;
import io.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import io.druid.query.timeseries.TimeseriesResultValue;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@RunWith(Parameterized.class)
public class CachingQueryRunnerTest
{
  @Parameterized.Parameters(name = "numBackgroundThreads={0}")
  public static Iterable<Object[]> constructorFeeder() throws IOException
  {
    return QueryRunnerTestHelper.cartesian(Arrays.asList(5, 1, 0));
  }

  private static final List<AggregatorFactory> AGGS = Arrays.asList(
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("imps", "imps"),
      new LongSumAggregatorFactory("impers", "imps")
  );

  private static final Object[] objects = new Object[]{
      new DateTime("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
      new DateTime("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
      new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
      new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
      new DateTime("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
  };

  private ExecutorService backgroundExecutorService;

  public CachingQueryRunnerTest(int numBackgroundThreads)
  {
    if (numBackgroundThreads > 0) {
      backgroundExecutorService = Executors.newFixedThreadPool(numBackgroundThreads);
    } else {
      backgroundExecutorService = MoreExecutors.sameThreadExecutor();
    }
  }

  @Test
  public void testCloseAndPopulate() throws Exception
  {
    List<Result> expectedRes = makeTopNResults(false, objects);
    List<Result> expectedCacheRes = makeTopNResults(true, objects);

    TopNQueryBuilder builder = new TopNQueryBuilder()
        .dataSource("ds")
        .dimension("top_dim")
        .metric("imps")
        .threshold(3)
        .intervals("2011-01-05/2011-01-10")
        .aggregators(AGGS)
        .granularity(Granularities.ALL);

    QueryToolChest toolchest = new TopNQueryQueryToolChest(
        new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
    );

    testCloseAndPopulate(expectedRes, expectedCacheRes, builder.build(), toolchest);
    testUseCache(expectedCacheRes, builder.build(), toolchest);
  }

  @Test
  public void testTimeseries() throws Exception
  {
    for (boolean descending : new boolean[]{false, true}) {
      TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                    .dataSource(QueryRunnerTestHelper.dataSource)
                                    .granularity(QueryRunnerTestHelper.dayGran)
                                    .intervals(QueryRunnerTestHelper.firstToThird)
                                    .aggregators(
                                        Arrays.<AggregatorFactory>asList(
                                            QueryRunnerTestHelper.rowsCount,
                                            new LongSumAggregatorFactory(
                                                "idx",
                                                "index"
                                            ),
                                            QueryRunnerTestHelper.qualityUniques
                                        )
                                    )
                                    .descending(descending)
                                    .build();

      Result row1 = new Result(
          new DateTime("2011-04-01"),
          new TimeseriesResultValue(
              ImmutableMap.<String, Object>of("rows", 13L, "idx", 6619L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
          )
      );
      Result row2 = new Result<>(
          new DateTime("2011-04-02"),
          new TimeseriesResultValue(
              ImmutableMap.<String, Object>of("rows", 13L, "idx", 5827L, "uniques", QueryRunnerTestHelper.UNIQUES_9)
          )
      );
      List<Result> expectedResults;
      if (descending) {
        expectedResults = Lists.newArrayList(row2, row1);
      } else {
        expectedResults = Lists.newArrayList(row1, row2);
      }

      QueryToolChest toolChest = new TimeseriesQueryQueryToolChest(
          QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator()
      );

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
          public void after(boolean isDone, Throwable thrown) throws Exception
          {
            closable.close();
          }
        }
    );

    final CountDownLatch cacheMustBePutOnce = new CountDownLatch(1);
    Cache cache = new Cache()
    {
      private final Map<NamedKey, byte[]> baseMap = new ConcurrentHashMap<>();

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

    String segmentIdentifier = "segment";
    SegmentDescriptor segmentDescriptor = new SegmentDescriptor(new Interval("2011/2012"), "version", 0);

    DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    CachingQueryRunner runner = new CachingQueryRunner(
        segmentIdentifier,
        segmentDescriptor,
        objectMapper,
        cache,
        toolchest,
        new QueryRunner()
        {
          @Override
          public Sequence run(QueryPlus queryPlus, Map responseContext)
          {
            return resultSeq;
          }
        },
        backgroundExecutorService,
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
        segmentIdentifier,
        segmentDescriptor,
        cacheStrategy.computeCacheKey(query)
    );

    HashMap<String, Object> context = new HashMap<String, Object>();
    Sequence res = runner.run(query, context);
    // base sequence is not closed yet
    Assert.assertFalse("sequence must not be closed", closable.isClosed());
    Assert.assertNull("cache must be empty", cache.get(cacheKey));

    ArrayList results = Sequences.toList(res, new ArrayList());
    Assert.assertTrue(closable.isClosed());
    Assert.assertEquals(expectedRes.toString(), results.toString());

    // wait for background caching finish
    // wait at most 10 seconds to fail the test to avoid block overall tests
    Assert.assertTrue("cache must be populated", cacheMustBePutOnce.await(10, TimeUnit.SECONDS));
    byte[] cacheValue = cache.get(cacheKey);
    Assert.assertNotNull(cacheValue);

    Function<Object, Result> fn = cacheStrategy.pullFromCache();
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
  ) throws Exception
  {
    DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    String segmentIdentifier = "segment";
    SegmentDescriptor segmentDescriptor = new SegmentDescriptor(new Interval("2011/2012"), "version", 0);

    CacheStrategy cacheStrategy = toolchest.getCacheStrategy(query);
    Cache.NamedKey cacheKey = CacheUtil.computeSegmentCacheKey(
        segmentIdentifier,
        segmentDescriptor,
        cacheStrategy.computeCacheKey(query)
    );

    Cache cache = MapCache.create(1024 * 1024);
    CacheUtil.populate(
        cache,
        objectMapper,
        cacheKey,
        Iterables.transform(expectedResults, cacheStrategy.prepareForCache())
    );

    CachingQueryRunner runner = new CachingQueryRunner(
        segmentIdentifier,
        segmentDescriptor,
        objectMapper,
        cache,
        toolchest,
        // return an empty sequence since results should get pulled from cache
        new QueryRunner()
        {
          @Override
          public Sequence run(QueryPlus queryPlus, Map responseContext)
          {
            return Sequences.empty();
          }
        },
        backgroundExecutorService,
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
    HashMap<String, Object> context = new HashMap<String, Object>();
    List<Result> results = Sequences.toList(runner.run(query, context), new ArrayList());
    Assert.assertEquals(expectedResults.toString(), results.toString());
  }

  private List<Result> makeTopNResults
      (boolean cachedResults, Object... objects)
  {
    List<Result> retVal = Lists.newArrayList();
    int index = 0;
    while (index < objects.length) {
      DateTime timestamp = (DateTime) objects[index++];

      List<Map<String, Object>> values = Lists.newArrayList();
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

  private static class AssertingClosable implements Closeable
  {

    private final AtomicBoolean closed = new AtomicBoolean(false);

    @Override
    public void close() throws IOException
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
