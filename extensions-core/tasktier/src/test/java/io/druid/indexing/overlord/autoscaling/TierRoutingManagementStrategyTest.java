/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.autoscaling;

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TierRoutingTaskRunner;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import io.druid.indexing.overlord.routing.TierTaskRunnerFactory;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TierRoutingManagementStrategyTest
{
  private static final String TIER = "test_tier";
  private static final ScheduledExecutorFactory SCHEDULED_EXECUTOR_FACTORY = new ScheduledExecutorFactory()
  {
    @Override
    public ScheduledExecutorService create(int corePoolSize, String nameFormat)
    {
      return Executors.newScheduledThreadPool(corePoolSize, Execs.makeThreadFactory(nameFormat));
    }
  };
  private final TierRoutingTaskRunner runner = new TierRoutingTaskRunner(
      Suppliers.ofInstance(new TierRouteConfig()),
      SCHEDULED_EXECUTOR_FACTORY
  );

  private final TaskRunner foreignRunner = EasyMock.createStrictMock(TaskRunner.class);
  private final TierRouteConfig tierRouteConfig = new TierRouteConfig()
  {
    @Override
    public Set<String> getTiers()
    {
      return ImmutableSet.of(TIER);
    }

    @Override
    public
    @NotNull
    TierTaskRunnerFactory getRouteFactory(@NotNull String tier)
    {
      if (TIER.equals(tier)) {
        return new TierTaskRunnerFactory()
        {
          @Override
          public TaskRunner build()
          {
            return foreignRunner;
          }
        };
      } else {
        throw new IAE("Expected `%s` found `%s`", TIER, tier);
      }
    }
  };

  private final TierRoutingManagementStrategy strategy = new TierRoutingManagementStrategy(
      Suppliers.ofInstance(tierRouteConfig),
      new ScheduledExecutorFactory()
      {
        @Override
        public ScheduledExecutorService create(int corePoolSize, String nameFormat)
        {
          return Executors.newScheduledThreadPool(corePoolSize, Execs.makeThreadFactory(nameFormat));
        }
      }
  );

  @Test
  public void testStartManagement() throws Exception
  {
    final ExecutorService service = Execs.singleThreaded("TestWatcher");
    try {
      final Future<?> future = service.submit(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            strategy.waitForUpdate();
          }
          catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
          }
        }
      });
      strategy.startManagement(runner);
      future.get();
    }
    finally {
      service.shutdownNow();
    }
  }

  @Test(expected = ISE.class)
  public void testMultipleStartsFails() throws Exception
  {
    strategy.startManagement(runner);
    strategy.startManagement(runner);
  }

  @Test
  public void testMultipleStarts() throws Exception
  {
    strategy.startManagement(runner);
    strategy.stopManagement();
    strategy.startManagement(runner);
    strategy.stopManagement();
    strategy.startManagement(runner);
    strategy.stopManagement();
    strategy.startManagement(runner);
    strategy.stopManagement();
    strategy.startManagement(runner);
    strategy.stopManagement();
    strategy.startManagement(runner);
  }


  @Test
  public void testStopManagement() throws Exception
  {
    testStartManagement();
    strategy.stopManagement();
  }

  @Test
  public void testStopManagementMultiple() throws Exception
  {
    testStartManagement();
    strategy.stopManagement();
    strategy.stopManagement();
  }

  @Test
  public void testGetEmptyStats() throws Exception
  {
    testStartManagement();
    EasyMock.reset(foreignRunner);
    EasyMock.expect(foreignRunner.getScalingStats()).andReturn(Optional.<ScalingStats>absent()).once();
    EasyMock.replay(foreignRunner);
    runner.getRunnerMap().put("some_tier", foreignRunner);
    Assert.assertNull(strategy.getStats());
    EasyMock.verify(foreignRunner);
    EasyMock.reset(foreignRunner);
  }

  @Test
  public void testGetNoStats() throws Exception
  {
    testStartManagement();
    Assert.assertNull(strategy.getStats());
  }

  @Test
  public void testSimpleOneStates() throws Exception
  {
    testStartManagement();
    final ScalingStats.ScalingEvent event = EasyMock.createStrictMock(ScalingStats.ScalingEvent.class);
    synchronized (this) {
      final ScalingStats stats = new ScalingStats(0);
      stats.addAllEvents(ImmutableList.of(event));
      EasyMock.reset(foreignRunner);
      EasyMock.expect(foreignRunner.getScalingStats()).andReturn(Optional.of(stats)).once();
      EasyMock.replay(foreignRunner);
    }
    runner.getRunnerMap().put("some_tier", foreignRunner);
    final ScalingStats stats = strategy.getStats();
    Assert.assertFalse(stats.toList().isEmpty());
    Assert.assertEquals(event, stats.toList().get(0));
    EasyMock.verify(foreignRunner);
    // Reset for teardown
    EasyMock.reset(foreignRunner);
  }

  @Test
  public void testGetRunner() throws Exception
  {
    testStartManagement();
    final TaskRunner defaultRunner = EasyMock.createStrictMock(TaskRunner.class);
    runner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, defaultRunner);
    runner.getRunnerMap().put(TIER, foreignRunner);
    Assert.assertEquals(foreignRunner, strategy.getRunner(
        new NoopTask("task_id", 0, 0, "YES", null, ImmutableMap.<String, Object>of(
            TierRoutingManagementStrategy.ROUTING_TARGET_CONTEXT_KEY,
            TIER
        ))
    ));
    Assert.assertEquals(defaultRunner, strategy.getRunner(
        new NoopTask("task_id", 0, 0, "YES", null, ImmutableMap.<String, Object>of())
    ));

    Assert.assertEquals(defaultRunner, strategy.getRunner(
        new NoopTask("task_id", 0, 0, "YES", null, null)
    ));
  }

  @Test
  public void testConcurrency() throws Exception
  {
    final int numTests = 100;
    final ListeningExecutorService executorService = MoreExecutors.listeningDecorator(Execs.multiThreaded(
        numTests,
        "test-hammer-%d"
    ));
    final ArrayList<ListenableFuture<?>> futures = new ArrayList<>(numTests);
    final CyclicBarrier barrier = new CyclicBarrier(numTests);
    final Random random = new Random(374898704198L);
    final List<? extends Runnable> tasks = ImmutableList.of(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              strategy.startManagement(runner);
            }
            catch (ISE e) {
              if (!"Already started".equals(e.getMessage())) {
                throw e;
              }
            }
          }
        },
        new Runnable()
        {
          @Override
          public void run()
          {
            strategy.stopManagement();
          }
        },
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              strategy.getStats();
            }
            catch (ISE e) {
              if (!"Management not started".equals(e.getMessage())) {
                throw e;
              }
            }
          }
        },
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              strategy.getRunner(new NoopTask("task_id", 0, 0, "YES", null, ImmutableMap.<String, Object>of(
                  TierRoutingManagementStrategy.ROUTING_TARGET_CONTEXT_KEY,
                  TIER
              )));
            }
            catch (ISE e) {
              if (!"Management not started".equals(e.getMessage())) {
                throw e;
              }
            }
          }
        }
    );
    for (int i = 0; i < numTests; ++i) {
      final Runnable task = tasks.get(random.nextInt(tasks.size()));
      futures.add(executorService.submit(new Runnable()
      {
        @Override
        public void run()
        {
          try {
            barrier.await();
          }
          catch (InterruptedException | BrokenBarrierException e) {
            throw Throwables.propagate(e);
          }
          for (int j = 0; j < 1000; ++j) {
            task.run();
          }
        }
      }));
    }
    try {
      Futures.allAsList(futures).get(1, TimeUnit.MINUTES);
    }
    finally {
      executorService.shutdownNow();
    }
  }

  @After
  public void tearDown()
  {
    strategy.stopManagement();
  }
}

