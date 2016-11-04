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

package io.druid.query;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.java.util.common.lifecycle.Lifecycle;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ChainedExecutionQueryRunnerTest
{
  private final Lock neverRelease = new ReentrantLock();

  @Before
  public void setup()
  {
    neverRelease.lock();
  }
  
  @Test(timeout = 60000)
  public void testQueryCancellation() throws Exception
  {
    ExecutorService exec = PrioritizedExecutorService.create(
        new Lifecycle(), new DruidProcessingConfig()
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
    final CountDownLatch queriesInterrupted = new CountDownLatch(2);
    final CountDownLatch queryIsRegistered = new CountDownLatch(1);

    Capture<ListenableFuture> capturedFuture = EasyMock.newCapture();
    QueryWatcher watcher = EasyMock.createStrictMock(QueryWatcher.class);
    watcher.registerQuery(
        EasyMock.<Query>anyObject(),
        EasyMock.and(EasyMock.<ListenableFuture>anyObject(), EasyMock.capture(capturedFuture))
    );
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

    ArrayBlockingQueue<DyingQueryRunner> interrupted = new ArrayBlockingQueue<>(3);
    Set<DyingQueryRunner> runners = Sets.newHashSet(
        new DyingQueryRunner(queriesStarted, queriesInterrupted, interrupted),
        new DyingQueryRunner(queriesStarted, queriesInterrupted, interrupted),
        new DyingQueryRunner(queriesStarted, queriesInterrupted, interrupted)
    );

    ChainedExecutionQueryRunner chainedRunner = new ChainedExecutionQueryRunner<>(
        exec,
        watcher,
        Lists.<QueryRunner<Integer>>newArrayList(
         runners
        )
    );
    Map<String, Object> context = ImmutableMap.<String, Object>of();
    final Sequence seq = chainedRunner.run(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .build(),
        context
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
    queryIsRegistered.await();
    queriesStarted.await();

    // cancel the query
    Assert.assertTrue(capturedFuture.hasCaptured());
    ListenableFuture future = capturedFuture.getValue();
    future.cancel(true);

    QueryInterruptedException cause = null;
    try {
      resultFuture.get();
    }
    catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof QueryInterruptedException);
      cause = (QueryInterruptedException) e.getCause();
    }
    queriesInterrupted.await();
    Assert.assertNotNull(cause);
    Assert.assertTrue(future.isCancelled());

    DyingQueryRunner interrupted1 = interrupted.poll();
    synchronized (interrupted1) {
      Assert.assertTrue("runner 1 started", interrupted1.hasStarted);
      Assert.assertTrue("runner 1 interrupted", interrupted1.interrupted);
    }
    DyingQueryRunner interrupted2 = interrupted.poll();
    synchronized (interrupted2) {
      Assert.assertTrue("runner 2 started", interrupted2.hasStarted);
      Assert.assertTrue("runner 2 interrupted", interrupted2.interrupted);
    }
    runners.remove(interrupted1);
    runners.remove(interrupted2);
    DyingQueryRunner remainingRunner = runners.iterator().next();
    synchronized (remainingRunner) {
      Assert.assertTrue("runner 3 should be interrupted or not have started",
                        !remainingRunner.hasStarted || remainingRunner.interrupted);
    }
    Assert.assertFalse("runner 1 not completed", interrupted1.hasCompleted);
    Assert.assertFalse("runner 2 not completed", interrupted2.hasCompleted);
    Assert.assertFalse("runner 3 not completed", remainingRunner.hasCompleted);

    EasyMock.verify(watcher);
  }

  @Test(timeout = 60000)
  public void testQueryTimeout() throws Exception
  {
    ExecutorService exec = PrioritizedExecutorService.create(
        new Lifecycle(), new DruidProcessingConfig()
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
    final CountDownLatch queriesInterrupted = new CountDownLatch(2);
    final CountDownLatch queryIsRegistered = new CountDownLatch(1);

    Capture<ListenableFuture> capturedFuture = new Capture<>();
    QueryWatcher watcher = EasyMock.createStrictMock(QueryWatcher.class);
    watcher.registerQuery(
        EasyMock.<Query>anyObject(),
        EasyMock.and(EasyMock.<ListenableFuture>anyObject(), EasyMock.capture(capturedFuture))
    );
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


    ArrayBlockingQueue<DyingQueryRunner> interrupted = new ArrayBlockingQueue<>(3);
    Set<DyingQueryRunner> runners = Sets.newHashSet(
        new DyingQueryRunner(queriesStarted, queriesInterrupted, interrupted),
        new DyingQueryRunner(queriesStarted, queriesInterrupted, interrupted),
        new DyingQueryRunner(queriesStarted, queriesInterrupted, interrupted)
    );

    ChainedExecutionQueryRunner chainedRunner = new ChainedExecutionQueryRunner<>(
        exec,
        watcher,
        Lists.<QueryRunner<Integer>>newArrayList(
            runners
        )
    );
    HashMap<String, Object> context = new HashMap<String, Object>();
    final Sequence seq = chainedRunner.run(
        Druids.newTimeseriesQueryBuilder()
              .dataSource("test")
              .intervals("2014/2015")
              .aggregators(Lists.<AggregatorFactory>newArrayList(new CountAggregatorFactory("count")))
              .context(ImmutableMap.<String, Object>of(QueryContextKeys.TIMEOUT, 100, "queryId", "test"))
              .build(),
        context
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
    queryIsRegistered.await();
    queriesStarted.await();

    Assert.assertTrue(capturedFuture.hasCaptured());
    ListenableFuture future = capturedFuture.getValue();

    // wait for query to time out
    QueryInterruptedException cause = null;
    try {
      resultFuture.get();
    }
    catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof QueryInterruptedException);
      Assert.assertEquals("Query timeout", ((QueryInterruptedException) e.getCause()).getErrorCode());
      cause = (QueryInterruptedException) e.getCause();
    }
    queriesInterrupted.await();
    Assert.assertNotNull(cause);
    Assert.assertTrue(future.isCancelled());

    DyingQueryRunner interrupted1 = interrupted.poll();
    synchronized (interrupted1) {
      Assert.assertTrue("runner 1 started", interrupted1.hasStarted);
      Assert.assertTrue("runner 1 interrupted", interrupted1.interrupted);
    }
    DyingQueryRunner interrupted2 = interrupted.poll();
    synchronized (interrupted2) {
      Assert.assertTrue("runner 2 started", interrupted2.hasStarted);
      Assert.assertTrue("runner 2 interrupted", interrupted2.interrupted);
    }
    runners.remove(interrupted1);
    runners.remove(interrupted2);
    DyingQueryRunner remainingRunner = runners.iterator().next();
    synchronized (remainingRunner) {
      Assert.assertTrue("runner 3 should be interrupted or not have started",
                        !remainingRunner.hasStarted || remainingRunner.interrupted);
    }
    Assert.assertFalse("runner 1 not completed", interrupted1.hasCompleted);
    Assert.assertFalse("runner 2 not completed", interrupted2.hasCompleted);
    Assert.assertFalse("runner 3 not completed", remainingRunner.hasCompleted);

    EasyMock.verify(watcher);
  }

  private class DyingQueryRunner implements QueryRunner<Integer>
  {
    private final CountDownLatch start;
    private final CountDownLatch stop;
    private final Queue<DyingQueryRunner> interruptedRunners;

    private volatile boolean hasStarted = false;
    private volatile boolean hasCompleted = false;
    private volatile boolean interrupted = false;

    public DyingQueryRunner(CountDownLatch start, CountDownLatch stop, Queue<DyingQueryRunner> interruptedRunners)
    {
      this.start = start;
      this.stop = stop;
      this.interruptedRunners = interruptedRunners;
    }

    @Override
    public Sequence<Integer> run(Query<Integer> query, Map<String, Object> responseContext)
    {
      // do a lot of work
      synchronized (this) {
        try {
          hasStarted = true;
          start.countDown();
          neverRelease.lockInterruptibly();
        }
        catch (InterruptedException e) {
          interrupted = true;
          interruptedRunners.offer(this);
          stop.countDown();
          throw new QueryInterruptedException(e);
        }
      }

      hasCompleted = true;
      stop.countDown();
      return Sequences.simple(Lists.newArrayList(123));
    }
  }
}
