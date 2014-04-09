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

package io.druid.query;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class ChainedExecutionQueryRunnerTest
{
  @Test @Ignore
  public void testQueryCancellation() throws Exception
  {
    ExecutorService exec = PrioritizedExecutorService.create(
        new Lifecycle(), new ExecutorServiceConfig()
        {
          @Override
          public String getFormatString()
          {
            return "test";
          }

          @Override
          public int getNumThreads()
          {
            return 2;
          }
        }
    );

    final CountDownLatch queriesStarted = new CountDownLatch(2);
    final CountDownLatch queryIsRegistered = new CountDownLatch(1);

    final Map<Query, ListenableFuture> queries = Maps.newHashMap();
    QueryWatcher watcher = new QueryWatcher()
    {
      @Override
      public void registerQuery(Query query, ListenableFuture future)
      {
        queries.put(query, future);
        queryIsRegistered.countDown();
      }
    };

    ChainedExecutionQueryRunner chainedRunner = new ChainedExecutionQueryRunner<>(
        exec,
        Ordering.<Integer>natural(),
        watcher,
        Lists.<QueryRunner<Integer>>newArrayList(
            new DyingQueryRunner(1, queriesStarted),
            new DyingQueryRunner(2, queriesStarted),
            new DyingQueryRunner(3, queriesStarted)
        )
    );

    final Sequence seq = chainedRunner.run(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .build()
    );

    Future f = Executors.newFixedThreadPool(1).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            Sequences.toList(seq, Lists.newArrayList());
          }
        }
    );

    // wait for query to register
    queryIsRegistered.await();
    queriesStarted.await();

    // cancel the query
    queries.values().iterator().next().cancel(true);
    f.get();
  }

  private static class DyingQueryRunner implements QueryRunner<Integer>
  {
    private final int id;
    private final CountDownLatch latch;

    public DyingQueryRunner(int id, CountDownLatch latch) {
      this.id = id;
      this.latch = latch;
    }

    @Override
    public Sequence<Integer> run(Query<Integer> query)
    {
      latch.countDown();

      int i = 0;
      while (i >= 0) {
        if(Thread.interrupted()) {
          throw new QueryInterruptedException("I got killed");
        }

        // do a lot of work
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          throw new QueryInterruptedException("I got killed");
        }
        ++i;
      }
      return Sequences.simple(Lists.newArrayList(i));
    }
  }
}
