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
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TierRoutingTaskRunner;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import io.druid.indexing.overlord.routing.TierTaskRunnerFactory;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.constraints.NotNull;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

public class TierRoutingManagementStrategyTest
{
  private static final String TIER = "test_tier";
  private static final ScheduledExecutorFactory scheduledExecutorFactory = new ScheduledExecutorFactory()
  {
    @Override
    public ScheduledExecutorService create(int corePoolSize, String nameFormat)
    {
      return Executors.newScheduledThreadPool(corePoolSize, Execs.makeThreadFactory(nameFormat));
    }
  };
  private final TierRoutingTaskRunner runner = new TierRoutingTaskRunner(
      Suppliers.ofInstance(new TierRouteConfig()),
      scheduledExecutorFactory
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

  private TierRoutingManagementStrategy strategy = new TierRoutingManagementStrategy(
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
    Assert.assertNull(strategy.getStats());
    EasyMock.verify(foreignRunner);
    EasyMock.reset(foreignRunner);
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

  @After
  public void tearDown()
  {
    strategy.stopManagement();
  }
}