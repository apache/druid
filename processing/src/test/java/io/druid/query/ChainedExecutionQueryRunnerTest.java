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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.common.concurrent.ExecutorServiceConfig;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.lifecycle.Lifecycle;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class ChainedExecutionQueryRunnerTest
{
  @Test
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

    Capture<ListenableFuture> capturedFuture = new Capture<>();
    QueryWatcher watcher = EasyMock.createStrictMock(QueryWatcher.class);
    watcher.registerQuery(EasyMock.<Query>anyObject(), EasyMock.and(EasyMock.<ListenableFuture>anyObject(), EasyMock.capture(capturedFuture)));
    EasyMock.expectLastCall()
            .andAnswer(
                new IAnswer<Void>()
                {
                  @Override
                  public Void answer() throws Throwable
                  {
                    queryIsRegistered.countDown();
                    return null;
                  }
                }
            )
            .once();

    EasyMock.replay(watcher);

    DyingQueryRunner runner1 = new DyingQueryRunner(queriesStarted);
    DyingQueryRunner runner2 = new DyingQueryRunner(queriesStarted);
    DyingQueryRunner runner3 = new DyingQueryRunner(queriesStarted);
    ChainedExecutionQueryRunner chainedRunner = new ChainedExecutionQueryRunner<>(
        exec,
        Ordering.<Integer>natural(),
        watcher,
        Lists.<QueryRunner<Integer>>newArrayList(
            runner1,
            runner2,
            runner3
        )
    );

    final Sequence seq = chainedRunner.run(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .build()
    );

    Future resultFuture = Executors.newFixedThreadPool(1).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            Sequences.toList(seq, Lists.newArrayList());
          }
        }
    );

    // wait for query to register and start
    Assert.assertTrue(queryIsRegistered.await(1, TimeUnit.SECONDS));
    Assert.assertTrue(queriesStarted.await(1, TimeUnit.SECONDS));

    // cancel the query
    Assert.assertTrue(capturedFuture.hasCaptured());
    ListenableFuture future = capturedFuture.getValue();
    future.cancel(true);

    QueryInterruptedException cause = null;
    try {
      resultFuture.get();
    } catch(ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof QueryInterruptedException);
      cause = (QueryInterruptedException)e.getCause();
    }
    Assert.assertNotNull(cause);
    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(runner1.hasStarted);
    Assert.assertTrue(runner2.hasStarted);
    Assert.assertFalse(runner3.hasStarted);
    Assert.assertFalse(runner1.hasCompleted);
    Assert.assertFalse(runner2.hasCompleted);
    Assert.assertFalse(runner3.hasCompleted);

    EasyMock.verify(watcher);
  }

  @Test
  public void testQueryTimeout() throws Exception
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

    Capture<ListenableFuture> capturedFuture = new Capture<>();
    QueryWatcher watcher = EasyMock.createStrictMock(QueryWatcher.class);
    watcher.registerQuery(EasyMock.<Query>anyObject(), EasyMock.and(EasyMock.<ListenableFuture>anyObject(), EasyMock.capture(capturedFuture)));
    EasyMock.expectLastCall()
            .andAnswer(
                new IAnswer<Void>()
                {
                  @Override
                  public Void answer() throws Throwable
                  {
                    queryIsRegistered.countDown();
                    return null;
                  }
                }
            )
            .once();

    EasyMock.replay(watcher);

    DyingQueryRunner runner1 = new DyingQueryRunner(queriesStarted);
    DyingQueryRunner runner2 = new DyingQueryRunner(queriesStarted);
    DyingQueryRunner runner3 = new DyingQueryRunner(queriesStarted);
    ChainedExecutionQueryRunner chainedRunner = new ChainedExecutionQueryRunner<>(
        exec,
        Ordering.<Integer>natural(),
        watcher,
        Lists.<QueryRunner<Integer>>newArrayList(
            runner1,
            runner2,
            runner3
        )
    );

    final Sequence seq = chainedRunner.run(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .context(ImmutableMap.<String, Object>of("timeout", (100), "queryId", "test"))
              .build()
    );

    Future resultFuture = Executors.newFixedThreadPool(1).submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            Sequences.toList(seq, Lists.newArrayList());
          }
        }
    );

    // wait for query to register and start
    Assert.assertTrue(queryIsRegistered.await(1, TimeUnit.SECONDS));
    Assert.assertTrue(queriesStarted.await(1, TimeUnit.SECONDS));

    // cancel the query
    Assert.assertTrue(capturedFuture.hasCaptured());
    ListenableFuture future = capturedFuture.getValue();

    QueryInterruptedException cause = null;
    try {
      resultFuture.get();
    } catch(ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof QueryInterruptedException);
      Assert.assertEquals("Query timeout", e.getCause().getMessage());
      cause = (QueryInterruptedException)e.getCause();
    }
    Assert.assertNotNull(cause);
    Assert.assertTrue(future.isCancelled());
    Assert.assertTrue(runner1.hasStarted);
    Assert.assertTrue(runner2.hasStarted);
    Assert.assertTrue(runner1.interrupted);
    Assert.assertTrue(runner2.interrupted);
    Assert.assertTrue(!runner3.hasStarted || runner3.interrupted);
    Assert.assertFalse(runner1.hasCompleted);
    Assert.assertFalse(runner2.hasCompleted);
    Assert.assertFalse(runner3.hasCompleted);

    EasyMock.verify(watcher);
  }

  private static class DyingQueryRunner implements QueryRunner<Integer>
  {
    private final CountDownLatch latch;
    private boolean hasStarted = false;
    private boolean hasCompleted = false;
    private boolean interrupted = false;

    public DyingQueryRunner(CountDownLatch latch)
    {
      this.latch = latch;
    }

    @Override
    public Sequence<Integer> run(Query<Integer> query)
    {
      hasStarted = true;
      latch.countDown();
      if (Thread.interrupted()) {
        interrupted = true;
        throw new QueryInterruptedException("I got killed");
      }

      // do a lot of work
      try {
        Thread.sleep(500);
      }
      catch (InterruptedException e) {
        interrupted = true;
        throw new QueryInterruptedException("I got killed");
      }

      hasCompleted = true;
      return Sequences.simple(Lists.newArrayList(123));
    }
  }
}
