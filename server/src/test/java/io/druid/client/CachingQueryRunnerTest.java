/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.client;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.client.cache.MapCache;
import io.druid.granularity.AllGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.CacheStrategy;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.topn.TopNQuery;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNResultValue;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CachingQueryRunnerTest
{

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

  @Test
  public void testCloseAndPopulate() throws Exception
  {
    Iterable<Result<TopNResultValue>> expectedRes = makeTopNResults(false, objects);
    final TopNQueryBuilder builder = new TopNQueryBuilder()
        .dataSource("ds")
        .dimension("top_dim")
        .metric("imps")
        .threshold(3)
        .intervals("2011-01-05/2011-01-10")
        .aggregators(AGGS)
        .granularity(AllGranularity.ALL);

    final AssertingClosable closable = new AssertingClosable();
    final Sequence resultSeq = new ResourceClosingSequence(
        Sequences.simple(expectedRes), closable
    )
    {
      @Override
      public Yielder toYielder(Object initValue, YieldingAccumulator accumulator)
      {
        Assert.assertFalse(closable.isClosed());
        return super.toYielder(
            initValue,
            accumulator
        );
      }
    };

    Cache cache = MapCache.create(1024 * 1024);

    String segmentIdentifier = "segment";
    SegmentDescriptor segmentDescriptor = new SegmentDescriptor(new Interval("2011/2012"), "version", 0);

    TopNQueryQueryToolChest toolchest = new TopNQueryQueryToolChest(new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator());
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
          public Sequence run(Query query, Map responseContext)
          {
            return resultSeq;
          }
        },
        MoreExecutors.sameThreadExecutor(),
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

    TopNQuery query = builder.build();
    CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> cacheStrategy = toolchest.getCacheStrategy(query);
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
    Assert.assertEquals(expectedRes, results);

    Iterable<Result<TopNResultValue>> expectedCacheRes = makeTopNResults(true, objects);

    byte[] cacheValue = cache.get(cacheKey);
    Assert.assertNotNull(cacheValue);

    Function<Object, Result<TopNResultValue>> fn = cacheStrategy.pullFromCache();
    List<Result<TopNResultValue>> cacheResults = Lists.newArrayList(
        Iterators.transform(
            objectMapper.readValues(
                objectMapper.getFactory().createParser(cacheValue),
                cacheStrategy.getCacheObjectClazz()
            ),
            fn
        )
    );
    Assert.assertEquals(expectedCacheRes, cacheResults);
  }

  @Test
  public void testUseCache() throws Exception
  {
    DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    Iterable<Result<TopNResultValue>> expectedResults = makeTopNResults(true, objects);
    String segmentIdentifier = "segment";
    SegmentDescriptor segmentDescriptor = new SegmentDescriptor(new Interval("2011/2012"), "version", 0);
    TopNQueryQueryToolChest toolchest = new TopNQueryQueryToolChest(new TopNQueryConfig(),
        QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator());

    final TopNQueryBuilder builder = new TopNQueryBuilder()
        .dataSource("ds")
        .dimension("top_dim")
        .metric("imps")
        .threshold(3)
        .intervals("2011-01-05/2011-01-10")
        .aggregators(AGGS)
        .granularity(AllGranularity.ALL);

    final TopNQuery query = builder.build();

    CacheStrategy<Result<TopNResultValue>, Object, TopNQuery> cacheStrategy = toolchest.getCacheStrategy(query);
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
          public Sequence run(Query query, Map responseContext)
          {
            return Sequences.empty();
          }
        },
        MoreExecutors.sameThreadExecutor(),
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
    List<Object> results = Sequences.toList(runner.run(query, context), new ArrayList());
    Assert.assertEquals(expectedResults, results);
  }

  private Iterable<Result<TopNResultValue>> makeTopNResults
      (boolean cachedResults, Object... objects)
  {
    List<Result<TopNResultValue>> retVal = Lists.newArrayList();
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
