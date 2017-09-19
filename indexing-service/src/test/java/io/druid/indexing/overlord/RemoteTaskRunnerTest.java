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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.indexing.common.IndexingServiceCondition;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TestRealtimeTask;
import io.druid.indexing.common.TestTasks;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.worker.Worker;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.zookeeper.CreateMode;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RemoteTaskRunnerTest
{
  private static final Joiner joiner = RemoteTaskRunnerTestUtils.joiner;
  private static final String workerHost = "worker";
  private static final String announcementsPath = joiner.join(RemoteTaskRunnerTestUtils.announcementsPath, workerHost);
  private static final String statusPath = joiner.join(RemoteTaskRunnerTestUtils.statusPath, workerHost);
  private static final int TIMEOUT_SECONDS = 20;

  private RemoteTaskRunner remoteTaskRunner;
  private RemoteTaskRunnerTestUtils rtrTestUtils = new RemoteTaskRunnerTestUtils();
  private ObjectMapper jsonMapper;
  private CuratorFramework cf;

  private Task task;
  private Worker worker;

  @Before
  public void setUp() throws Exception
  {
    rtrTestUtils.setUp();
    jsonMapper = rtrTestUtils.getObjectMapper();
    cf = rtrTestUtils.getCuratorFramework();

    task = TestTasks.unending("task");
  }

  @After
  public void tearDown() throws Exception
  {
    if (remoteTaskRunner != null) {
      remoteTaskRunner.stop();
    }
    rtrTestUtils.tearDown();
  }

  @Test
  public void testRun() throws Exception
  {
    doSetup();

    ListenableFuture<TaskStatus> result = remoteTaskRunner.run(task);

    Assert.assertTrue(taskAnnounced(task.getId()));
    mockWorkerRunningTask(task);
    Assert.assertTrue(workerRunningTask(task.getId()));
    mockWorkerCompleteSuccessfulTask(task);
    Assert.assertTrue(workerCompletedTask(result));

    Assert.assertEquals(task.getId(), result.get().getId());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, result.get().getStatusCode());
  }

  @Test
  public void testStartWithNoWorker() throws Exception
  {
    makeRemoteTaskRunner(new TestRemoteTaskRunnerConfig(new Period("PT1S")));
  }

  @Test
  public void testRunExistingTaskThatHasntStartedRunning() throws Exception
  {
    doSetup();

    remoteTaskRunner.run(task);
    Assert.assertTrue(taskAnnounced(task.getId()));

    ListenableFuture<TaskStatus> result = remoteTaskRunner.run(task);

    Assert.assertFalse(result.isDone());
    mockWorkerRunningTask(task);
    Assert.assertTrue(workerRunningTask(task.getId()));
    mockWorkerCompleteSuccessfulTask(task);
    Assert.assertTrue(workerCompletedTask(result));

    Assert.assertEquals(task.getId(), result.get().getId());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, result.get().getStatusCode());
  }

  @Test
  public void testRunExistingTaskThatHasStartedRunning() throws Exception
  {
    doSetup();

    remoteTaskRunner.run(task);
    Assert.assertTrue(taskAnnounced(task.getId()));
    mockWorkerRunningTask(task);
    Assert.assertTrue(workerRunningTask(task.getId()));

    ListenableFuture<TaskStatus> result = remoteTaskRunner.run(task);

    Assert.assertFalse(result.isDone());

    mockWorkerCompleteSuccessfulTask(task);
    Assert.assertTrue(workerCompletedTask(result));

    Assert.assertEquals(task.getId(), result.get().getId());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, result.get().getStatusCode());
  }

  @Test
  public void testRunTooMuchZKData() throws Exception
  {
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    EasyMock.replay(emitter);

    doSetup();

    remoteTaskRunner.run(TestTasks.unending(new String(new char[5000])));

    EasyMock.verify(emitter);
  }

  @Test
  public void testRunSameAvailabilityGroup() throws Exception
  {
    doSetup();

    TestRealtimeTask task1 = new TestRealtimeTask(
        "rt1",
        new TaskResource("rt1", 1),
        "foo",
        TaskStatus.running("rt1"),
        jsonMapper
    );
    remoteTaskRunner.run(task1);
    Assert.assertTrue(taskAnnounced(task1.getId()));
    mockWorkerRunningTask(task1);

    TestRealtimeTask task2 = new TestRealtimeTask(
        "rt2",
        new TaskResource("rt1", 1),
        "foo",
        TaskStatus.running("rt2"),
        jsonMapper
    );
    remoteTaskRunner.run(task2);

    TestRealtimeTask task3 = new TestRealtimeTask(
        "rt3",
        new TaskResource("rt2", 1),
        "foo",
        TaskStatus.running("rt3"),
        jsonMapper
    );
    remoteTaskRunner.run(task3);

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                return remoteTaskRunner.getRunningTasks().size() == 2;
              }
            }
        )
    );

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                return remoteTaskRunner.getPendingTasks().size() == 1;
              }
            }
        )
    );

    Assert.assertTrue(remoteTaskRunner.getPendingTasks().iterator().next().getTaskId().equals("rt2"));
  }

  @Test
  public void testRunWithCapacity() throws Exception
  {
    doSetup();

    TestRealtimeTask task1 = new TestRealtimeTask(
        "rt1",
        new TaskResource("rt1", 1),
        "foo",
        TaskStatus.running("rt1"),
        jsonMapper
    );
    remoteTaskRunner.run(task1);
    Assert.assertTrue(taskAnnounced(task1.getId()));
    mockWorkerRunningTask(task1);

    TestRealtimeTask task2 = new TestRealtimeTask(
        "rt2",
        new TaskResource("rt2", 3),
        "foo",
        TaskStatus.running("rt2"),
        jsonMapper
    );
    remoteTaskRunner.run(task2);

    TestRealtimeTask task3 = new TestRealtimeTask(
        "rt3",
        new TaskResource("rt3", 2),
        "foo",
        TaskStatus.running("rt3"),
        jsonMapper
    );
    remoteTaskRunner.run(task3);
    Assert.assertTrue(taskAnnounced(task3.getId()));
    mockWorkerRunningTask(task3);

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                return remoteTaskRunner.getRunningTasks().size() == 2;
              }
            }
        )
    );

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                return remoteTaskRunner.getPendingTasks().size() == 1;
              }
            }
        )
    );

    Assert.assertTrue(remoteTaskRunner.getPendingTasks().iterator().next().getTaskId().equals("rt2"));
  }

  @Test
  public void testStatusRemoved() throws Exception
  {
    doSetup();

    ListenableFuture<TaskStatus> future = remoteTaskRunner.run(task);
    Assert.assertTrue(taskAnnounced(task.getId()));
    mockWorkerRunningTask(task);

    Assert.assertTrue(workerRunningTask(task.getId()));

    Assert.assertTrue(remoteTaskRunner.getRunningTasks().iterator().next().getTaskId().equals("task"));

    cf.delete().forPath(joiner.join(statusPath, task.getId()));

    TaskStatus status = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    Assert.assertEquals(status.getStatusCode(), TaskStatus.Status.FAILED);
  }

  @Test
  public void testBootstrap() throws Exception
  {
    cf.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .forPath(joiner.join(statusPath, "first"), jsonMapper.writeValueAsBytes(TaskStatus.running("first")));
    cf.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .forPath(joiner.join(statusPath, "second"), jsonMapper.writeValueAsBytes(TaskStatus.running("second")));

    doSetup();

    final Set<String> existingTasks = Sets.newHashSet();
    for (ImmutableWorkerInfo workerInfo : remoteTaskRunner.getWorkers()) {
      existingTasks.addAll(workerInfo.getRunningTasks());
    }
    Assert.assertEquals("existingTasks", ImmutableSet.of("first", "second"), existingTasks);

    final Set<String> runningTasks = Sets.newHashSet(
        Iterables.transform(
            remoteTaskRunner.getRunningTasks(),
            new Function<RemoteTaskRunnerWorkItem, String>()
            {
              @Override
              public String apply(RemoteTaskRunnerWorkItem input)
              {
                return input.getTaskId();
              }
            }
        )
    );
    Assert.assertEquals("runningTasks", ImmutableSet.of("first", "second"), runningTasks);
  }

  @Test
  public void testRunWithTaskComplete() throws Exception
  {
    cf.create()
      .creatingParentsIfNeeded()
      .withMode(CreateMode.EPHEMERAL)
      .forPath(joiner.join(statusPath, task.getId()), jsonMapper.writeValueAsBytes(TaskStatus.success(task.getId())));

    doSetup();

    ListenableFuture<TaskStatus> future = remoteTaskRunner.run(task);

    TaskStatus status = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    Assert.assertEquals(TaskStatus.Status.SUCCESS, status.getStatusCode());
  }

  @Test
  public void testWorkerRemoved() throws Exception
  {
    doSetup();
    Future<TaskStatus> future = remoteTaskRunner.run(task);

    Assert.assertTrue(taskAnnounced(task.getId()));
    mockWorkerRunningTask(task);

    Assert.assertTrue(workerRunningTask(task.getId()));

    cf.delete().forPath(announcementsPath);

    TaskStatus status = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

    Assert.assertEquals(TaskStatus.Status.FAILED, status.getStatusCode());
    RemoteTaskRunnerConfig config = remoteTaskRunner.getRemoteTaskRunnerConfig();
    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                return remoteTaskRunner.getRemovedWorkerCleanups().isEmpty();
              }
            },
            // cleanup task is independently scheduled by event listener. we need to wait some more time.
            config.getTaskCleanupTimeout().toStandardDuration().getMillis() * 2
        )
    );
    Assert.assertNull(cf.checkExists().forPath(statusPath));
  }

  @Test
  public void testWorkerDisabled() throws Exception
  {
    doSetup();
    final ListenableFuture<TaskStatus> result = remoteTaskRunner.run(task);

    Assert.assertTrue(taskAnnounced(task.getId()));
    mockWorkerRunningTask(task);
    Assert.assertTrue(workerRunningTask(task.getId()));

    // Disable while task running
    disableWorker();

    // Continue test
    mockWorkerCompleteSuccessfulTask(task);
    Assert.assertTrue(workerCompletedTask(result));
    Assert.assertEquals(task.getId(), result.get().getId());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, result.get().getStatusCode());

    // Confirm RTR thinks the worker is disabled.
    Assert.assertEquals("", Iterables.getOnlyElement(remoteTaskRunner.getWorkers()).getWorker().getVersion());
  }

  private void doSetup() throws Exception
  {
    makeWorker();
    makeRemoteTaskRunner(new TestRemoteTaskRunnerConfig(new Period("PT5S")));
  }

  private void makeRemoteTaskRunner(RemoteTaskRunnerConfig config) throws Exception
  {
    remoteTaskRunner = rtrTestUtils.makeRemoteTaskRunner(config);
  }

  private void makeWorker() throws Exception
  {
    worker = rtrTestUtils.makeWorker(workerHost, 3);
  }

  private void disableWorker() throws Exception
  {
    rtrTestUtils.disableWorker(worker);
  }

  private boolean taskAnnounced(final String taskId)
  {
    return rtrTestUtils.taskAnnounced(workerHost, taskId);
  }

  private boolean workerRunningTask(final String taskId)
  {
    return rtrTestUtils.workerRunningTask(workerHost, taskId);
  }

  private boolean workerCompletedTask(final ListenableFuture<TaskStatus> result)
  {
    return TestUtils.conditionValid(
        new IndexingServiceCondition()
        {
          @Override
          public boolean isValid()
          {
            return result.isDone();
          }
        }
    );
  }

  private void mockWorkerRunningTask(final Task task) throws Exception
  {
    rtrTestUtils.mockWorkerRunningTask("worker", task);
  }

  private void mockWorkerCompleteSuccessfulTask(final Task task) throws Exception
  {
    rtrTestUtils.mockWorkerCompleteSuccessfulTask("worker", task);
  }

  private void mockWorkerCompleteFailedTask(final Task task) throws Exception
  {
    rtrTestUtils.mockWorkerCompleteFailedTask("worker", task);
  }

  @Test
  public void testFindLazyWorkerTaskRunning() throws Exception
  {
    doSetup();
    remoteTaskRunner.start();
    remoteTaskRunner.run(task);
    Assert.assertTrue(taskAnnounced(task.getId()));
    mockWorkerRunningTask(task);
    Collection<Worker> lazyworkers = remoteTaskRunner.markWorkersLazy(
        new Predicate<ImmutableWorkerInfo>()
        {
          @Override
          public boolean apply(ImmutableWorkerInfo input)
          {
            return true;
          }
        }, 1
    );
    Assert.assertTrue(lazyworkers.isEmpty());
    Assert.assertTrue(remoteTaskRunner.getLazyWorkers().isEmpty());
    Assert.assertEquals(1, remoteTaskRunner.getWorkers().size());
  }

  @Test
  public void testFindLazyWorkerForWorkerJustAssignedTask() throws Exception
  {
    doSetup();
    remoteTaskRunner.run(task);
    Assert.assertTrue(taskAnnounced(task.getId()));
    Collection<Worker> lazyworkers = remoteTaskRunner.markWorkersLazy(
        new Predicate<ImmutableWorkerInfo>()
        {
          @Override
          public boolean apply(ImmutableWorkerInfo input)
          {
            return true;
          }
        }, 1
    );
    Assert.assertTrue(lazyworkers.isEmpty());
    Assert.assertTrue(remoteTaskRunner.getLazyWorkers().isEmpty());
    Assert.assertEquals(1, remoteTaskRunner.getWorkers().size());
  }

  @Test
  public void testFindLazyWorkerNotRunningAnyTask() throws Exception
  {
    doSetup();
    Collection<Worker> lazyworkers = remoteTaskRunner.markWorkersLazy(
        new Predicate<ImmutableWorkerInfo>()
        {
          @Override
          public boolean apply(ImmutableWorkerInfo input)
          {
            return true;
          }
        }, 1
    );
    Assert.assertEquals(1, lazyworkers.size());
    Assert.assertEquals(1, remoteTaskRunner.getLazyWorkers().size());
  }

  @Test
  public void testWorkerZKReconnect() throws Exception
  {
    makeWorker();
    makeRemoteTaskRunner(new TestRemoteTaskRunnerConfig(new Period("PT5M")));
    Future<TaskStatus> future = remoteTaskRunner.run(task);

    Assert.assertTrue(taskAnnounced(task.getId()));
    mockWorkerRunningTask(task);

    Assert.assertTrue(workerRunningTask(task.getId()));
    byte[] bytes = cf.getData().forPath(announcementsPath);
    cf.delete().forPath(announcementsPath);
    // worker task cleanup scheduled
    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                return remoteTaskRunner.getRemovedWorkerCleanups().containsKey(worker.getHost());
              }
            }
        )
    );

    // Worker got reconnected
    cf.create().forPath(announcementsPath, bytes);

    // worker task cleanup should get cancelled and removed
    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                return !remoteTaskRunner.getRemovedWorkerCleanups().containsKey(worker.getHost());
              }
            }
        )
    );

    mockWorkerCompleteSuccessfulTask(task);
    TaskStatus status = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    Assert.assertEquals(status.getStatusCode(), TaskStatus.Status.SUCCESS);
    Assert.assertEquals(TaskStatus.Status.SUCCESS, status.getStatusCode());
  }

  @Test
  public void testSortByInsertionTime() throws Exception
  {
    RemoteTaskRunnerWorkItem item1 = new RemoteTaskRunnerWorkItem("b", null, null)
        .withQueueInsertionTime(DateTimes.of("2015-01-01T00:00:03Z"));
    RemoteTaskRunnerWorkItem item2 = new RemoteTaskRunnerWorkItem("a", null, null)
        .withQueueInsertionTime(DateTimes.of("2015-01-01T00:00:02Z"));
    RemoteTaskRunnerWorkItem item3 = new RemoteTaskRunnerWorkItem("c", null, null)
        .withQueueInsertionTime(DateTimes.of("2015-01-01T00:00:01Z"));
    ArrayList<RemoteTaskRunnerWorkItem> workItems = Lists.newArrayList(item1, item2, item3);
    RemoteTaskRunner.sortByInsertionTime(workItems);
    Assert.assertEquals(item3, workItems.get(0));
    Assert.assertEquals(item2, workItems.get(1));
    Assert.assertEquals(item1, workItems.get(2));
  }

  @Test
  public void testBlacklistZKWorkers() throws Exception
  {
    Period timeoutPeriod = Period.millis(1000);
    makeWorker();

    RemoteTaskRunnerConfig rtrConfig = new TestRemoteTaskRunnerConfig(timeoutPeriod);
    rtrConfig.setMaxPercentageBlacklistWorkers(100);

    makeRemoteTaskRunner(rtrConfig);

    TestRealtimeTask task1 = new TestRealtimeTask(
        "realtime1",
        new TaskResource("realtime1", 1),
        "foo",
        TaskStatus.success("realtime1"),
        jsonMapper
    );
    Future<TaskStatus> taskFuture1 = remoteTaskRunner.run(task1);
    Assert.assertTrue(taskAnnounced(task1.getId()));
    mockWorkerRunningTask(task1);
    mockWorkerCompleteFailedTask(task1);
    Assert.assertTrue(taskFuture1.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).isFailure());
    Assert.assertEquals(0, remoteTaskRunner.getBlackListedWorkers().size());
    Assert.assertEquals(
        1,
        remoteTaskRunner.findWorkerRunningTask(task1.getId()).getContinuouslyFailedTasksCount()
    );

    TestRealtimeTask task2 = new TestRealtimeTask(
        "realtime2",
        new TaskResource("realtime2", 1),
        "foo",
        TaskStatus.running("realtime2"),
        jsonMapper
    );
    Future<TaskStatus> taskFuture2 = remoteTaskRunner.run(task2);
    Assert.assertTrue(taskAnnounced(task2.getId()));
    mockWorkerRunningTask(task2);
    mockWorkerCompleteFailedTask(task2);
    Assert.assertTrue(taskFuture2.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).isFailure());
    Assert.assertEquals(1, remoteTaskRunner.getBlackListedWorkers().size());
    Assert.assertEquals(
        2,
        remoteTaskRunner.findWorkerRunningTask(task2.getId()).getContinuouslyFailedTasksCount()
    );

    ((RemoteTaskRunnerTestUtils.TestableRemoteTaskRunner) remoteTaskRunner)
        .setCurrentTimeMillis(System.currentTimeMillis());
    remoteTaskRunner.checkBlackListedNodes();

    Assert.assertEquals(1, remoteTaskRunner.getBlackListedWorkers().size());

    ((RemoteTaskRunnerTestUtils.TestableRemoteTaskRunner) remoteTaskRunner)
        .setCurrentTimeMillis(System.currentTimeMillis() + 2 * timeoutPeriod.toStandardDuration().getMillis());
    remoteTaskRunner.checkBlackListedNodes();

    // After backOffTime the nodes are removed from blacklist
    Assert.assertEquals(0, remoteTaskRunner.getBlackListedWorkers().size());
    Assert.assertEquals(
        0,
        remoteTaskRunner.findWorkerRunningTask(task2.getId()).getContinuouslyFailedTasksCount()
    );

    TestRealtimeTask task3 = new TestRealtimeTask(
        "realtime3",
        new TaskResource("realtime3", 1),
        "foo",
        TaskStatus.running("realtime3"),
        jsonMapper
    );
    Future<TaskStatus> taskFuture3 = remoteTaskRunner.run(task3);
    Assert.assertTrue(taskAnnounced(task3.getId()));
    mockWorkerRunningTask(task3);
    mockWorkerCompleteSuccessfulTask(task3);
    Assert.assertTrue(taskFuture3.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).isSuccess());
    Assert.assertEquals(0, remoteTaskRunner.getBlackListedWorkers().size());
    Assert.assertEquals(
        0,
        remoteTaskRunner.findWorkerRunningTask(task3.getId()).getContinuouslyFailedTasksCount()
    );
  }

  /**
   * With 2 workers and maxPercentageBlacklistWorkers(25), neither worker should ever be blacklisted even after
   * exceeding maxRetriesBeforeBlacklist.
   */
  @Test
  public void testBlacklistZKWorkers25Percent() throws Exception
  {
    Period timeoutPeriod = Period.millis(1000);

    rtrTestUtils.makeWorker("worker", 10);
    rtrTestUtils.makeWorker("worker2", 10);

    RemoteTaskRunnerConfig rtrConfig = new TestRemoteTaskRunnerConfig(timeoutPeriod);
    rtrConfig.setMaxPercentageBlacklistWorkers(25);

    makeRemoteTaskRunner(rtrConfig);

    String firstWorker = null;
    String secondWorker = null;

    for (int i = 1; i < 13; i++) {
      String taskId = StringUtils.format("rt-%d", i);
      TestRealtimeTask task = new TestRealtimeTask(
          taskId, new TaskResource(taskId, 1), "foo", TaskStatus.success(taskId), jsonMapper
      );

      Future<TaskStatus> taskFuture = remoteTaskRunner.run(task);

      if (i == 1) {
        if (rtrTestUtils.taskAnnounced("worker2", task.getId())) {
          firstWorker = "worker2";
          secondWorker = "worker";
        } else {
          firstWorker = "worker";
          secondWorker = "worker2";
        }
      }

      final String expectedWorker = i % 2 == 0 ? secondWorker : firstWorker;

      Assert.assertTrue(rtrTestUtils.taskAnnounced(expectedWorker, task.getId()));
      rtrTestUtils.mockWorkerRunningTask(expectedWorker, task);
      rtrTestUtils.mockWorkerCompleteFailedTask(expectedWorker, task);

      Assert.assertTrue(taskFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).isFailure());
      Assert.assertEquals(0, remoteTaskRunner.getBlackListedWorkers().size());
      Assert.assertEquals(
          ((i + 1) / 2),
          remoteTaskRunner.findWorkerRunningTask(task.getId()).getContinuouslyFailedTasksCount()
      );
    }
  }

  /**
   * With 2 workers and maxPercentageBlacklistWorkers(50), one worker should get blacklisted after the second failure
   * and the second worker should never be blacklisted even after exceeding maxRetriesBeforeBlacklist.
   */
  @Test
  public void testBlacklistZKWorkers50Percent() throws Exception
  {
    Period timeoutPeriod = Period.millis(1000);

    rtrTestUtils.makeWorker("worker", 10);
    rtrTestUtils.makeWorker("worker2", 10);

    RemoteTaskRunnerConfig rtrConfig = new TestRemoteTaskRunnerConfig(timeoutPeriod);
    rtrConfig.setMaxPercentageBlacklistWorkers(50);

    makeRemoteTaskRunner(rtrConfig);

    String firstWorker = null;
    String secondWorker = null;

    for (int i = 1; i < 13; i++) {
      String taskId = StringUtils.format("rt-%d", i);
      TestRealtimeTask task = new TestRealtimeTask(
          taskId, new TaskResource(taskId, 1), "foo", TaskStatus.success(taskId), jsonMapper
      );

      Future<TaskStatus> taskFuture = remoteTaskRunner.run(task);

      if (i == 1) {
        if (rtrTestUtils.taskAnnounced("worker2", task.getId())) {
          firstWorker = "worker2";
          secondWorker = "worker";
        } else {
          firstWorker = "worker";
          secondWorker = "worker2";
        }
      }

      final String expectedWorker = i % 2 == 0 || i > 4 ? secondWorker : firstWorker;

      Assert.assertTrue(rtrTestUtils.taskAnnounced(expectedWorker, task.getId()));
      rtrTestUtils.mockWorkerRunningTask(expectedWorker, task);
      rtrTestUtils.mockWorkerCompleteFailedTask(expectedWorker, task);

      Assert.assertTrue(taskFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).isFailure());
      Assert.assertEquals(i > 2 ? 1 : 0, remoteTaskRunner.getBlackListedWorkers().size());
      Assert.assertEquals(
          i > 4 ? i - 2 : ((i + 1) / 2),
          remoteTaskRunner.findWorkerRunningTask(task.getId()).getContinuouslyFailedTasksCount()
      );
    }
  }

  @Test
  public void testSuccessfulTaskOnBlacklistedWorker() throws Exception
  {
    Period timeoutPeriod = Period.millis(1000);
    makeWorker();

    RemoteTaskRunnerConfig rtrConfig = new TestRemoteTaskRunnerConfig(timeoutPeriod);
    rtrConfig.setMaxPercentageBlacklistWorkers(100);

    makeRemoteTaskRunner(rtrConfig);

    TestRealtimeTask task1 = new TestRealtimeTask(
        "realtime1", new TaskResource("realtime1", 1), "foo", TaskStatus.success("realtime1"), jsonMapper
    );
    TestRealtimeTask task2 = new TestRealtimeTask(
        "realtime2", new TaskResource("realtime2", 1), "foo", TaskStatus.success("realtime2"), jsonMapper
    );
    TestRealtimeTask task3 = new TestRealtimeTask(
        "realtime3", new TaskResource("realtime3", 1), "foo", TaskStatus.success("realtime3"), jsonMapper
    );

    Future<TaskStatus> taskFuture1 = remoteTaskRunner.run(task1);
    Assert.assertTrue(taskAnnounced(task1.getId()));
    mockWorkerRunningTask(task1);
    mockWorkerCompleteFailedTask(task1);
    Assert.assertTrue(taskFuture1.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).isFailure());
    Assert.assertEquals(0, remoteTaskRunner.getBlackListedWorkers().size());

    Future<TaskStatus> taskFuture2 = remoteTaskRunner.run(task2);
    Assert.assertTrue(taskAnnounced(task2.getId()));
    mockWorkerRunningTask(task2);

    Future<TaskStatus> taskFuture3 = remoteTaskRunner.run(task3);
    Assert.assertTrue(taskAnnounced(task3.getId()));
    mockWorkerRunningTask(task3);
    mockWorkerCompleteFailedTask(task3);
    Assert.assertTrue(taskFuture3.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).isFailure());
    Assert.assertEquals(1, remoteTaskRunner.getBlackListedWorkers().size());

    mockWorkerCompleteSuccessfulTask(task2);
    Assert.assertTrue(taskFuture2.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).isSuccess());
    Assert.assertEquals(0, remoteTaskRunner.getBlackListedWorkers().size());
  }
}
