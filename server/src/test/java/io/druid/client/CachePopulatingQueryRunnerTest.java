/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.client;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.common.guava.ResourceClosingSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import io.druid.client.cache.Cache;
import io.druid.client.cache.CacheConfig;
import io.druid.granularity.AllGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.query.topn.TopNQueryBuilder;
import io.druid.query.topn.TopNQueryConfig;
import io.druid.query.topn.TopNQueryQueryToolChest;
import io.druid.query.topn.TopNResultValue;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class CachePopulatingQueryRunnerTest
{

  private static final List<AggregatorFactory> AGGS = Arrays.asList(
      new CountAggregatorFactory("rows"),
      new LongSumAggregatorFactory("imps", "imps"),
      new LongSumAggregatorFactory("impers", "imps")
  );

  @Test
  public void testCachePopulatingQueryRunnerResourceClosing() throws Exception
  {
    Iterable<Result<TopNResultValue>> expectedRes = makeTopNResults(
        new DateTime("2011-01-05"), "a", 50, 4994, "b", 50, 4993, "c", 50, 4992,
        new DateTime("2011-01-06"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
        new DateTime("2011-01-07"), "a", 50, 4991, "b", 50, 4990, "c", 50, 4989,
        new DateTime("2011-01-08"), "a", 50, 4988, "b", 50, 4987, "c", 50, 4986,
        new DateTime("2011-01-09"), "a", 50, 4985, "b", 50, 4984, "c", 50, 4983
    );
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

    Cache cache = EasyMock.createMock(Cache.class);
    // cache populater ignores populating for local cache, so a dummy cache
    EasyMock.expect(cache.isLocal()).andReturn(false);
    CachePopulatingQueryRunner runner = new CachePopulatingQueryRunner(
        "segment",
        new SegmentDescriptor(new Interval("2011/2012"), "version", 0),
        new DefaultObjectMapper(),
        cache,
        new TopNQueryQueryToolChest(new TopNQueryConfig()),
        new QueryRunner()
        {
          @Override
          public Sequence run(Query query)
          {
            return resultSeq;
          }
        },
        new CacheConfig()

    );

    Sequence res = runner.run(builder.build());
    // base sequence is not closed yet
    Assert.assertTrue(closable.isClosed());
    ArrayList results = Sequences.toList(res, new ArrayList());
    Assert.assertTrue(closable.isClosed());
    Assert.assertEquals(expectedRes, results);

  }

  private Iterable<Result<TopNResultValue>> makeTopNResults
      (Object... objects)
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
        values.add(
            ImmutableMap.of(
                "top_dim", objects[index],
                "rows", rows,
                "imps", imps,
                "impers", imps,
                "avg_imps_per_row", imps / rows
            )
        );
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
