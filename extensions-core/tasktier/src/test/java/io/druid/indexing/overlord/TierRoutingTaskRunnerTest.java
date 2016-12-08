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

package io.druid.indexing.overlord;

import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteSource;
import com.google.common.util.concurrent.SettableFuture;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.overlord.autoscaling.AutoScalingData;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.autoscaling.TierRoutingManagementStrategy;
import io.druid.indexing.overlord.routing.TierRouteConfig;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.tasklogs.TaskLogStreamer;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TierRoutingTaskRunnerTest
{
  private static final String TASK_ID = "some task id";
  private final AtomicReference<ScheduledExecutorService> executorService = new AtomicReference<>();
  private final ScheduledExecutorFactory scheduledExecutorServiceFactory = new ScheduledExecutorFactory()
  {
    @Override
    public ScheduledExecutorService create(int corePoolSize, final String nameFormat)
    {
      if (!executorService.compareAndSet(null, Execs.scheduledSingleThreaded(nameFormat))) {
        throw new ISE("Already created");
      }
      return executorService.get();
    }
  };
  private final TierRouteConfig tierRouteConfig = new TierRouteConfig();

  private TierRoutingTaskRunner tierRoutingTaskRunner = null;

  @Before
  public void setUp()
  {
    tierRoutingTaskRunner = new TierRoutingTaskRunner(
        Suppliers.ofInstance(tierRouteConfig),
        scheduledExecutorServiceFactory
    );
  }

  @Test
  public void testRestore() throws Exception
  {
    Assert.assertTrue(tierRoutingTaskRunner.restore().isEmpty());
  }

  @Test
  public void testDefaultRun() throws Exception
  {
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    final SettableFuture<TaskStatus> future = SettableFuture.create();
    EasyMock.expect(taskRunner.run(EasyMock.anyObject(NoopTask.class))).andReturn(future).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    Assert.assertEquals(future, tierRoutingTaskRunner.run(new NoopTask(
        TASK_ID,
        0,
        0,
        null,
        null,
        null
    )));
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testShutdown() throws Exception
  {
    // Test no routes
    tierRoutingTaskRunner.shutdown(TASK_ID);
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    taskRunner.shutdown(EasyMock.same(TASK_ID));
    EasyMock.expectLastCall().once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    tierRoutingTaskRunner.shutdown(TASK_ID);
    EasyMock.verify(taskRunner);
  }


  @Test
  public void testShutdownExceptional() throws Exception
  {
    // Test no routes
    tierRoutingTaskRunner.shutdown(TASK_ID);
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    taskRunner.shutdown(EasyMock.same(TASK_ID));
    EasyMock.expectLastCall().andThrow(new RuntimeException("test exception")).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    tierRoutingTaskRunner.shutdown(TASK_ID);
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testGetRunningTasks() throws Exception
  {
    Assert.assertTrue(tierRoutingTaskRunner.getRunningTasks().isEmpty());
    final Collection tasks = ImmutableList.of(new TaskRunnerWorkItem(TASK_ID, SettableFuture.<TaskStatus>create())
    {
      @Override
      public TaskLocation getLocation()
      {
        return null;
      }
    });
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    EasyMock.expect(taskRunner.getRunningTasks()).andReturn(tasks).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    Assert.assertEquals(tasks, tierRoutingTaskRunner.getRunningTasks());
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testGetRunningTasksExceptional() throws Exception
  {
    Assert.assertTrue(tierRoutingTaskRunner.getRunningTasks().isEmpty());
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    EasyMock.expect(taskRunner.getRunningTasks()).andThrow(new RuntimeException("test exception")).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    Assert.assertTrue(tierRoutingTaskRunner.getRunningTasks().isEmpty());
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testGetPendingTasks() throws Exception
  {
    Assert.assertTrue(tierRoutingTaskRunner.getPendingTasks().isEmpty());
    final Collection tasks = ImmutableList.of(new TaskRunnerWorkItem(TASK_ID, SettableFuture.<TaskStatus>create())
    {
      @Override
      public TaskLocation getLocation()
      {
        return null;
      }
    });
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    EasyMock.expect(taskRunner.getPendingTasks()).andReturn(tasks).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    Assert.assertEquals(tasks, tierRoutingTaskRunner.getPendingTasks());
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testGetPendingTasksExceptional() throws Exception
  {
    Assert.assertTrue(tierRoutingTaskRunner.getPendingTasks().isEmpty());
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    EasyMock.expect(taskRunner.getPendingTasks()).andThrow(new RuntimeException("test exception")).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    Assert.assertTrue(tierRoutingTaskRunner.getPendingTasks().isEmpty());
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testGetKnownTasks() throws Exception
  {
    Assert.assertTrue(tierRoutingTaskRunner.getKnownTasks().isEmpty());
    final Collection tasks = ImmutableList.of(new TaskRunnerWorkItem(TASK_ID, SettableFuture.<TaskStatus>create())
    {
      @Override
      public TaskLocation getLocation()
      {
        return null;
      }
    });
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    EasyMock.expect(taskRunner.getKnownTasks()).andReturn(tasks).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    Assert.assertEquals(tasks, tierRoutingTaskRunner.getKnownTasks());
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testGetKnownTasksExceptional() throws Exception
  {
    Assert.assertTrue(tierRoutingTaskRunner.getKnownTasks().isEmpty());
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    EasyMock.expect(taskRunner.getKnownTasks()).andThrow(new RuntimeException("test exception")).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    Assert.assertTrue(tierRoutingTaskRunner.getKnownTasks().isEmpty());
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testGetScalingStats() throws Exception
  {
    Assert.assertFalse(tierRoutingTaskRunner.getScalingStats().isPresent());
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    final ScalingStats scalingStats = new ScalingStats(1);
    scalingStats.addProvisionEvent(new AutoScalingData(ImmutableList.of("node0")));
    Assert.assertEquals(1, scalingStats.toList().size());
    EasyMock.expect(taskRunner.getScalingStats()).andReturn(Optional.of(scalingStats)).once();
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    final Optional<ScalingStats> maybeStats = tierRoutingTaskRunner.getScalingStats();
    EasyMock.verify(taskRunner);
    Assert.assertTrue(maybeStats.isPresent());
    Assert.assertEquals(scalingStats.toList(), maybeStats.get().toList());
  }

  @Test
  public void testStartStop() throws Exception
  {
    Assert.assertFalse(executorService.get().isShutdown());
    tierRoutingTaskRunner.start();
    tierRoutingTaskRunner.stop();
    Assert.assertTrue(executorService.get().isShutdown());
  }

  @Test
  public void testStreamTaskLog() throws Exception
  {
    Assert.assertFalse(tierRoutingTaskRunner.streamTaskLog(TASK_ID, 0).isPresent());
    final TaskRunnerStreamer taskRunner = EasyMock.createStrictMock(TaskRunnerStreamer.class);
    final ByteSource bs = ByteSource.empty();
    EasyMock.expect(taskRunner.streamTaskLog(EasyMock.same(TASK_ID), EasyMock.anyLong())).andReturn(Optional.of(bs));
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    final Optional<ByteSource> maybeByteSource = tierRoutingTaskRunner.streamTaskLog(TASK_ID, 0);
    Assert.assertTrue(maybeByteSource.isPresent());
    Assert.assertEquals(bs, maybeByteSource.get());
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testStreamTaskLogExceptional() throws Exception
  {
    Assert.assertFalse(tierRoutingTaskRunner.streamTaskLog(TASK_ID, 0).isPresent());
    final TaskRunnerStreamer taskRunner = EasyMock.createStrictMock(TaskRunnerStreamer.class);
    EasyMock.expect(taskRunner.streamTaskLog(EasyMock.same(TASK_ID), EasyMock.anyLong()))
            .andThrow(new RuntimeException("test exception"));
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    final Optional<ByteSource> maybeByteSource = tierRoutingTaskRunner.streamTaskLog(TASK_ID, 0);
    Assert.assertFalse(maybeByteSource.isPresent());
    EasyMock.verify(taskRunner);
  }

  @Test
  public void testStreamTaskLogSkipNotStreamer() throws Exception
  {
    Assert.assertFalse(tierRoutingTaskRunner.streamTaskLog(TASK_ID, 0).isPresent());
    final TaskRunner taskRunner = EasyMock.createStrictMock(TaskRunner.class);
    EasyMock.replay(taskRunner);
    tierRoutingTaskRunner.getRunnerMap().put(TierRoutingManagementStrategy.DEFAULT_ROUTE, taskRunner);
    final Optional<ByteSource> maybeByteSource = tierRoutingTaskRunner.streamTaskLog(TASK_ID, 0);
    Assert.assertFalse(maybeByteSource.isPresent());
    EasyMock.verify(taskRunner);
  }

  @After
  public void tearDown() throws InterruptedException
  {
    final ScheduledExecutorService service = executorService.get();
    if (service != null) {
      service.shutdownNow();
      service.awaitTermination(10, TimeUnit.SECONDS);
    }
  }
}

abstract class TaskRunnerStreamer implements TaskRunner, TaskLogStreamer
{
  // nothing
}
