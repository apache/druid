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
import io.druid.concurrent.Execs;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import io.druid.indexing.overlord.routing.TierTaskRunnerFactory;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.constraints.NotNull;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

public class TierRoutingManagementStrategyTest
{
  private static final String TIER = "test_tier";
  private final ScheduledExecutorService scheduledExecutorService = Execs.scheduledSingleThreaded("routing-test-%s");
  private final TaskRunner runner = EasyMock.createStrictMock(TaskRunner.class);
  private final Object memoryBarrier = new Object();
  private final ConcurrentHashMap<String, TaskRunner> taskRunnerMap = new ConcurrentHashMap<>();
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
            synchronized (memoryBarrier) {
              return runner;
            }
          }
        };
      } else {
        throw new IAE("Expected `%s` found `%s`", TIER, tier);
      }
    }
  };

  private TierRoutingManagementStrategy strategy = new TierRoutingManagementStrategy(
      taskRunnerMap,
      Suppliers.ofInstance(tierRouteConfig),
      scheduledExecutorService
  );

  @Test
  public void testStartManagement() throws Exception
  {
    synchronized (memoryBarrier) {
      runner.start();
      EasyMock.expectLastCall().once();
      EasyMock.replay(runner);
    }
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
      strategy.startManagement(null);
      future.get();
      EasyMock.verify(runner);
    }
    finally {
      service.shutdownNow();
    }
  }

  @Test(expected = ISE.class)
  public void testMultipleStartsFails() throws Exception
  {
    strategy.startManagement(null);
    strategy.startManagement(null);
  }

  @Test(expected = ISE.class)
  public void testMultipleStartsFailsAfterStop() throws Exception
  {
    strategy.startManagement(null);
    strategy.stopManagement();
    strategy.startManagement(null);
  }


  @Test
  public void testStopManagement() throws Exception
  {
    testStartManagement();
    synchronized (memoryBarrier) {
      EasyMock.reset(runner);
      runner.stop();
      EasyMock.expectLastCall().once();
      EasyMock.replay(runner);
    }
    strategy.stopManagement();
    Assert.assertTrue(scheduledExecutorService.isShutdown());
    EasyMock.verify(runner);
  }

  @Test
  public void testStopManagementMultiple() throws Exception
  {
    testStartManagement();
    synchronized (memoryBarrier) {
      EasyMock.reset(runner);
      runner.stop();
      EasyMock.expectLastCall().once();
      EasyMock.replay(runner);
    }
    strategy.stopManagement();
    strategy.stopManagement();
    EasyMock.verify(runner);
  }

  @Test
  public void testGetEmptyStats() throws Exception
  {
    testStartManagement();
    synchronized (memoryBarrier) {
      EasyMock.reset(runner);
      EasyMock.expect(runner.getScalingStats()).andReturn(Optional.<ScalingStats>absent()).once();
      EasyMock.replay(runner);
    }
    Assert.assertNull(strategy.getStats());
    EasyMock.verify(runner);
  }

  @Test
  public void testSimpleOneStates() throws Exception
  {
    testStartManagement();
    final ScalingStats.ScalingEvent event = EasyMock.createStrictMock(ScalingStats.ScalingEvent.class);
    synchronized (memoryBarrier) {
      final ScalingStats stats = new ScalingStats(0);
      stats.addAllEvents(ImmutableList.of(event));
      EasyMock.reset(runner);
      EasyMock.expect(runner.getScalingStats()).andReturn(Optional.of(stats)).once();
      EasyMock.replay(runner);
    }
    final ScalingStats stats = strategy.getStats();
    EasyMock.verify(runner);
    Assert.assertFalse(stats.toList().isEmpty());
    Assert.assertEquals(event, stats.toList().get(0));
  }

  @Test
  public void testGetRunner() throws Exception
  {
    final TaskRunner defaultRunner = EasyMock.createStrictMock(TaskRunner.class);
    taskRunnerMap.put(TierRoutingManagementStrategy.DEFAULT_ROUTE, defaultRunner);
    testStartManagement();
    Assert.assertEquals(runner, strategy.getRunner(
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
    if (!scheduledExecutorService.isShutdown()) {
      scheduledExecutorService.shutdownNow();
    }
  }
}