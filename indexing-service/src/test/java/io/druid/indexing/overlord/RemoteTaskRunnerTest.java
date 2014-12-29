/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.cache.SimplePathChildrenCacheFactory;
import io.druid.indexing.common.IndexingServiceCondition;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TestMergeTask;
import io.druid.indexing.common.TestRealtimeTask;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.FillCapacityWorkerSelectStrategy;
import io.druid.indexing.worker.TaskAnnouncement;
import io.druid.indexing.worker.Worker;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.CreateMode;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RemoteTaskRunnerTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private static final Joiner joiner = Joiner.on("/");
  private static final String basePath = "/test/druid";
  private static final String announcementsPath = String.format("%s/indexer/announcements/worker", basePath);
  private static final String tasksPath = String.format("%s/indexer/tasks/worker", basePath);
  private static final String statusPath = String.format("%s/indexer/status/worker", basePath);
  private static final int TIMEOUT_SECONDS = 5;

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private RemoteTaskRunner remoteTaskRunner;

  private TestMergeTask task;

  private Worker worker;

  @Before
  public void setUp() throws Exception
  {
    testingCluster = new TestingCluster(1);
    testingCluster.start();

    cf = CuratorFrameworkFactory.builder()
                                .connectString(testingCluster.getConnectString())
                                .retryPolicy(new ExponentialBackoffRetry(1, 10))
                                .compressionProvider(new PotentiallyGzippedCompressionProvider(false))
                                .build();
    cf.start();
    cf.create().creatingParentsIfNeeded().forPath(basePath);
    cf.create().creatingParentsIfNeeded().forPath(tasksPath);
    cf.create().creatingParentsIfNeeded().forPath(statusPath);


    task = TestMergeTask.createDummyTask("task");
  }

  @After
  public void tearDown() throws Exception
  {
    remoteTaskRunner.stop();
    cf.close();
    testingCluster.stop();
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

    remoteTaskRunner.run(TestMergeTask.createDummyTask(new String(new char[5000])));

    EasyMock.verify(emitter);
  }

  @Test
  public void testRunSameAvailabilityGroup() throws Exception
  {
    doSetup();

    TestRealtimeTask task1 = new TestRealtimeTask("rt1", new TaskResource("rt1", 1), "foo", TaskStatus.running("rt1"));
    remoteTaskRunner.run(task1);
    Assert.assertTrue(taskAnnounced(task1.getId()));
    mockWorkerRunningTask(task1);

    TestRealtimeTask task2 = new TestRealtimeTask("rt2", new TaskResource("rt1", 1), "foo", TaskStatus.running("rt2"));
    remoteTaskRunner.run(task2);

    TestRealtimeTask task3 = new TestRealtimeTask("rt3", new TaskResource("rt2", 1), "foo", TaskStatus.running("rt3"));
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

    TestRealtimeTask task1 = new TestRealtimeTask("rt1", new TaskResource("rt1", 1), "foo", TaskStatus.running("rt1"));
    remoteTaskRunner.run(task1);
    Assert.assertTrue(taskAnnounced(task1.getId()));
    mockWorkerRunningTask(task1);

    TestRealtimeTask task2 = new TestRealtimeTask("rt2", new TaskResource("rt2", 3), "foo", TaskStatus.running("rt2"));
    remoteTaskRunner.run(task2);

    TestRealtimeTask task3 = new TestRealtimeTask("rt3", new TaskResource("rt3", 2), "foo", TaskStatus.running("rt3"));
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
    for (ZkWorker zkWorker : remoteTaskRunner.getWorkers()) {
      existingTasks.addAll(zkWorker.getRunningTasks().keySet());
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
    makeRemoteTaskRunner();
  }

  private void makeRemoteTaskRunner() throws Exception
  {
    RemoteTaskRunnerConfig config = new TestRemoteTaskRunnerConfig();
    remoteTaskRunner = new RemoteTaskRunner(
        jsonMapper,
        config,
        new IndexerZkConfig().setZkPathsConfig(new ZkPathsConfig()
        {
          @Override
          public String getBase()
          {
            return basePath;
          }
        }),
        cf,
        new SimplePathChildrenCacheFactory.Builder().build(),
        null,
        new FillCapacityWorkerSelectStrategy()
    );

    remoteTaskRunner.start();
  }

  private void makeWorker() throws Exception
  {
    worker = new Worker(
        "worker",
        "localhost",
        3,
        "0"
    );

    cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
        announcementsPath,
        jsonMapper.writeValueAsBytes(worker)
    );
  }

  private void disableWorker() throws Exception
  {
    cf.setData().forPath(
        announcementsPath,
        jsonMapper.writeValueAsBytes(new Worker(worker.getHost(), worker.getIp(), worker.getCapacity(), ""))
    );
  }

  private boolean taskAnnounced(final String taskId)
  {
    return pathExists(joiner.join(tasksPath, taskId));
  }

  private boolean workerRunningTask(final String taskId)
  {
    return pathExists(joiner.join(statusPath, taskId));
  }

  private boolean pathExists(final String path)
  {
    return TestUtils.conditionValid(
        new IndexingServiceCondition()
        {
          @Override
          public boolean isValid()
          {
            try {
              return cf.checkExists().forPath(path) != null;
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );
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
    cf.delete().forPath(joiner.join(tasksPath, task.getId()));

    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(task, TaskStatus.running(task.getId()));
    cf.create().forPath(joiner.join(statusPath, task.getId()), jsonMapper.writeValueAsBytes(taskAnnouncement));
  }

  private void mockWorkerCompleteSuccessfulTask(final Task task) throws Exception
  {
    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(task, TaskStatus.success(task.getId()));
    cf.setData().forPath(joiner.join(statusPath, task.getId()), jsonMapper.writeValueAsBytes(taskAnnouncement));
  }
}
