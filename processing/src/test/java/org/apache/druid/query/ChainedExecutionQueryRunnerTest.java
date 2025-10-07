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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

@SuppressWarnings("DoNotMock")
public class ChainedExecutionQueryRunnerTest
{
  private final Lock neverRelease = new ReentrantLock();
  private QueryProcessingPool processingPool;

  @Before
  public void setup()
  {
    neverRelease.lock();
    processingPool = new ForwardingQueryProcessingPool(
        Execs.multiThreaded(2, "ChainedExecutionQueryRunnerTestExecutor-%d"),
        Execs.scheduledSingleThreaded("ChainedExecutionQueryRunnerTestExecutor-Timeout-%d")
    );
  }

  @After
  public void tearDown()
  {
    processingPool.shutdown();
  }

  @Test(timeout = 60_000L)
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
    watcher.registerQueryFuture(
        EasyMock.anyObject(),
        EasyMock.and(EasyMock.anyObject(), EasyMock.capture(capturedFuture))
    );
    EasyMock.expectLastCall()
            .andAnswer(
                new IAnswer<Void>()
                {
                  @Override
                  public Void answer()
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
        processingPool,
        watcher,
        Lists.newArrayList(
            runners
        )
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .intervals("2014/2015")
                                  .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
                                  .build();
    final Sequence seq = chainedRunner.run(QueryPlus.wrap(query));

    Future resultFuture = Execs.multiThreaded(1, "ChainedExecutionQueryRunnerTest-%d").submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            seq.toList();
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

  @Test(timeout = 60_000L)
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

    Capture<ListenableFuture> capturedFuture = Capture.newInstance();
    QueryWatcher watcher = EasyMock.createStrictMock(QueryWatcher.class);
    watcher.registerQueryFuture(
        EasyMock.anyObject(),
        EasyMock.and(EasyMock.anyObject(), EasyMock.capture(capturedFuture))
    );
    EasyMock.expectLastCall()
            .andAnswer(
                new IAnswer<Void>()
                {
                  @Override
                  public Void answer()
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
        processingPool,
        watcher,
        Lists.newArrayList(
            runners
        )
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .intervals("2014/2015")
                                  .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
                                  .context(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 100, "queryId", "test"))
                                  .build();
    final Sequence seq = chainedRunner.run(QueryPlus.wrap(query));

    Future resultFuture = Execs.multiThreaded(1, "ChainedExecutionQueryRunnerTest-%d").submit(
        new Runnable()
        {
          @Override
          public void run()
          {
            seq.toList();
          }
        }
    );

    // wait for query to register and start
    queryIsRegistered.await();
    queriesStarted.await();

    Assert.assertTrue(capturedFuture.hasCaptured());
    ListenableFuture future = capturedFuture.getValue();

    // wait for query to time out
    QueryTimeoutException cause = null;
    try {
      resultFuture.get();
    }
    catch (ExecutionException e) {
      Assert.assertTrue(e.getCause() instanceof QueryTimeoutException);
      Assert.assertEquals("Query timeout", ((QueryTimeoutException) e.getCause()).getErrorCode());
      cause = (QueryTimeoutException) e.getCause();
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

  @Test
  public void testSubmittedTaskType()
  {
    QueryProcessingPool queryProcessingPool = Mockito.mock(QueryProcessingPool.class);
    QueryWatcher watcher = EasyMock.createStrictMock(QueryWatcher.class);
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
        .dataSource("test")
        .intervals("2014/2015")
        .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
        .context(ImmutableMap.of(QueryContexts.TIMEOUT_KEY, 100, "queryId", "test"))
        .build();
    List<QueryRunner<Result<TimeseriesResultValue>>> runners = Arrays.asList(
        Mockito.mock(QueryRunner.class),
        Mockito.mock(QueryRunner.class)
    );
    ChainedExecutionQueryRunner<Result<TimeseriesResultValue>> chainedRunner = new ChainedExecutionQueryRunner<>(
        queryProcessingPool,
        watcher,
        runners
    );

    Mockito.when(queryProcessingPool.submitRunnerTask(ArgumentMatchers.any())).thenReturn(Futures.immediateFuture(Collections.singletonList(123)));
    chainedRunner.run(QueryPlus.wrap(query)).toList();
    ArgumentCaptor<PrioritizedQueryRunnerCallable> captor = ArgumentCaptor.forClass(PrioritizedQueryRunnerCallable.class);
    Mockito.verify(queryProcessingPool, Mockito.times(2)).submitRunnerTask(captor.capture());
    List<QueryRunner> actual = captor.getAllValues().stream().map(PrioritizedQueryRunnerCallable::getRunner).collect(Collectors.toList());
    Assert.assertEquals(runners, actual);
  }

  @Test(timeout = 10_000L)
  public void testPerSegmentTimeout()
  {
    QueryRunner<Integer> slowRunner = (queryPlus, responseContext) -> {
      try {
        Thread.sleep(500);
        return Sequences.of(2);
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    };
    QueryRunner<Integer> fastRunner = (queryPlus, responseContext) -> Sequences.of(1);

    QueryWatcher watcher = EasyMock.createStrictMock(QueryWatcher.class);
    watcher.registerQueryFuture(
        EasyMock.anyObject(),
        EasyMock.anyObject()
    );
    EasyMock.expectLastCall().anyTimes();
    EasyMock.replay(watcher);

    ChainedExecutionQueryRunner chainedRunner = new ChainedExecutionQueryRunner<>(
        processingPool,
        watcher,
        Arrays.asList(slowRunner, fastRunner)
    );
    TimeseriesQuery query = Druids.newTimeseriesQueryBuilder()
                                  .dataSource("test")
                                  .intervals("2014/2015")
                                  .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
                                  .context(
                                      ImmutableMap.of(
                                          QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 100L,
                                          QueryContexts.TIMEOUT_KEY, 5_000L
                                      )
                                  )
                                  .queryId("test")
                                  .build();
    Sequence seq = chainedRunner.run(QueryPlus.wrap(query));

    List<Integer> results = null;
    Exception thrown = null;
    try {
      results = seq.toList();
    }
    catch (Exception e) {
      thrown = e;
    }

    Assert.assertNull("No results expected due to timeout", results);
    Assert.assertNotNull("Exception should be thrown", thrown);
    Assert.assertTrue(
        "Should be QueryTimeoutException or caused by it",
        Throwables.getRootCause(thrown) instanceof QueryTimeoutException
    );
    Assert.assertEquals("Query timeout, cancelling pending results for query [test]. Per-segment timeout exceeded.", thrown.getMessage());

    EasyMock.verify(watcher);
  }

  @Test(timeout = 5_000L)
  public void test_perSegmentTimeout_crossQuery() throws Exception
  {
    final CountDownLatch slowStarted = new CountDownLatch(2);
    final CountDownLatch fastStarted = new CountDownLatch(1);

    QueryRunner<Result<TimeseriesResultValue>> slowRunner = (queryPlus, responseContext) -> {
      slowStarted.countDown();
      try {
        Thread.sleep(60_000L);
      }
      catch (InterruptedException e) {
        throw new QueryInterruptedException(e);
      }
      return Sequences.empty();
    };

    QueryRunner<Result<TimeseriesResultValue>> fastRunner = (queryPlus, responseContext) -> {
      fastStarted.countDown();
      return Sequences.simple(Collections.singletonList(
          new Result<>(null, new TimeseriesResultValue(ImmutableMap.of("count", 1)))
      ));
    };

    TimeseriesQuery slowQuery = Druids.newTimeseriesQueryBuilder()
                                      .dataSource("test")
                                      .intervals("2014/2015")
                                      .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
                                      .context(ImmutableMap.of(
                                          QueryContexts.TIMEOUT_KEY, 300_000L,
                                          QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 1_000L
                                      ))
                                      .queryId("slow")
                                      .build();

    TimeseriesQuery fastQuery = Druids.newTimeseriesQueryBuilder()
                                      .dataSource("test")
                                      .intervals("2014/2015")
                                      .aggregators(Collections.singletonList(new CountAggregatorFactory("count")))
                                      .context(ImmutableMap.of(
                                          QueryContexts.TIMEOUT_KEY, 5_000L,
                                          QueryContexts.PER_SEGMENT_TIMEOUT_KEY, 3_000L
                                      ))
                                      .queryId("fast")
                                      .build();

    ChainedExecutionQueryRunner<Result<TimeseriesResultValue>> slowChainedRunner = new ChainedExecutionQueryRunner<>(
        processingPool,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        Arrays.asList(slowRunner, slowRunner)
    );
    ChainedExecutionQueryRunner<Result<TimeseriesResultValue>> fastChainedRunner = new ChainedExecutionQueryRunner<>(
        processingPool,
        QueryRunnerTestHelper.NOOP_QUERYWATCHER,
        Collections.singletonList(fastRunner)
    );

    ExecutorService exec = Execs.multiThreaded(2, "QueryExecutor-%d");
    try {
      Future<List<Result<TimeseriesResultValue>>> slowFuture = exec.submit(() -> slowChainedRunner.run(QueryPlus.wrap(
          slowQuery)).toList());

      slowStarted.await();

      Future<List<Result<TimeseriesResultValue>>> fastFuture = exec.submit(() -> fastChainedRunner.run(QueryPlus.wrap(
          fastQuery)).toList());

      boolean fastStartedEarly = fastStarted.await(500, TimeUnit.MILLISECONDS);
      Assert.assertFalse(
          "Fast query should be blocked and not started while slow queries are running",
          fastStartedEarly
      );

      ExecutionException ex = Assert.assertThrows(ExecutionException.class, slowFuture::get);
      Assert.assertTrue(Throwables.getRootCause(ex) instanceof QueryTimeoutException);
      Assert.assertEquals("Query timeout, cancelling pending results for query [slow]. Per-segment timeout exceeded.", ex.getCause().getMessage());
      Assert.assertEquals(
          Collections.singletonList(
              new Result<>(null, new TimeseriesResultValue(ImmutableMap.of("count", 1)))
          ), fastFuture.get()
      );
    }
    finally {
      exec.shutdownNow();
    }
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
    public Sequence<Integer> run(QueryPlus<Integer> queryPlus, ResponseContext responseContext)
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
      return Sequences.simple(Collections.singletonList(123));
    }
  }
}
