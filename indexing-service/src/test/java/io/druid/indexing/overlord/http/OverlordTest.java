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

package io.druid.indexing.overlord.http;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.Pair;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.concurrent.Execs;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.curator.discovery.NoopServiceAnnouncer;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskStorageConfig;
import io.druid.indexing.common.task.NoopTask;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.HeapMemoryTaskStorage;
import io.druid.indexing.overlord.TaskLockbox;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerFactory;
import io.druid.indexing.overlord.TaskRunnerListener;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.TaskStorageQueryAdapter;
import io.druid.indexing.overlord.autoscaling.ScalingStats;
import io.druid.indexing.overlord.config.TaskQueueConfig;
import io.druid.server.DruidNode;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import io.druid.server.security.AuthConfig;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class OverlordTest
{
  private static final TaskLocation TASK_LOCATION = new TaskLocation("dummy", 1000);

  private TestingServer server;
  private Timing timing;
  private CuratorFramework curator;
  private TaskMaster taskMaster;
  private TaskLockbox taskLockbox;
  private TaskStorage taskStorage;
  private TaskActionClientFactory taskActionClientFactory;
  private CountDownLatch announcementLatch;
  private DruidNode druidNode;
  private OverlordResource overlordResource;
  private CountDownLatch[] taskCompletionCountDownLatches;
  private CountDownLatch[] runTaskCountDownLatches;
  private HttpServletRequest req;

  private void setupServerAndCurator() throws Exception
  {
    server = new TestingServer();
    timing = new Timing();
    curator = CuratorFrameworkFactory
        .builder()
        .connectString(server.getConnectString())
        .sessionTimeoutMs(timing.session())
        .connectionTimeoutMs(timing.connection())
        .retryPolicy(new RetryOneTime(1))
        .compressionProvider(new PotentiallyGzippedCompressionProvider(true))
        .build();
  }

  private void tearDownServerAndCurator()
  {
    CloseQuietly.close(curator);
    CloseQuietly.close(server);
  }

  @Before
  public void setUp() throws Exception
  {
    req = EasyMock.createStrictMock(HttpServletRequest.class);
    taskLockbox = EasyMock.createStrictMock(TaskLockbox.class);
    taskLockbox.syncFromStorage();
    EasyMock.expectLastCall().atLeastOnce();
    taskLockbox.add(EasyMock.<Task>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    taskLockbox.remove(EasyMock.<Task>anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    // for second Noop Task directly added to deep storage.
    taskLockbox.add(EasyMock.<Task>anyObject());
    EasyMock.expectLastCall().atLeastOnce();
    taskLockbox.remove(EasyMock.<Task>anyObject());
    EasyMock.expectLastCall().atLeastOnce();

    taskActionClientFactory = EasyMock.createStrictMock(TaskActionClientFactory.class);
    EasyMock.expect(taskActionClientFactory.create(EasyMock.<Task>anyObject()))
            .andReturn(null).anyTimes();
    EasyMock.replay(taskLockbox, taskActionClientFactory);

    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));
    runTaskCountDownLatches = new CountDownLatch[2];
    runTaskCountDownLatches[0] = new CountDownLatch(1);
    runTaskCountDownLatches[1] = new CountDownLatch(1);
    taskCompletionCountDownLatches = new CountDownLatch[2];
    taskCompletionCountDownLatches[0] = new CountDownLatch(1);
    taskCompletionCountDownLatches[1] = new CountDownLatch(1);
    announcementLatch = new CountDownLatch(1);
    IndexerZkConfig indexerZkConfig = new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null, null);
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    curator.create().creatingParentsIfNeeded().forPath(indexerZkConfig.getLeaderLatchPath());
    druidNode = new DruidNode("hey", "what", 1234);
    ServiceEmitter serviceEmitter = new NoopServiceEmitter();
    taskMaster = new TaskMaster(
        new TaskQueueConfig(null, new Period(1), null, new Period(10)),
        taskLockbox,
        taskStorage,
        taskActionClientFactory,
        druidNode,
        indexerZkConfig,
        new TaskRunnerFactory<MockTaskRunner>()
        {
          @Override
          public MockTaskRunner build()
          {
            return new MockTaskRunner(runTaskCountDownLatches, taskCompletionCountDownLatches);
          }
        },
        curator,
        new NoopServiceAnnouncer()
        {
          @Override
          public void announce(DruidNode node)
          {
            announcementLatch.countDown();
          }
        },
        serviceEmitter
    );
    EmittingLogger.registerEmitter(serviceEmitter);
  }

  @Test(timeout = 2000L)
  public void testOverlordRun() throws Exception
  {
    // basic task master lifecycle test
    taskMaster.start();
    announcementLatch.await();
    while (!taskMaster.isLeading()) {
      // I believe the control will never reach here and thread will never sleep but just to be on safe side
      Thread.sleep(10);
    }
    Assert.assertEquals(taskMaster.getLeader(), druidNode.getHostAndPort());
    // Test Overlord resource stuff
    overlordResource = new OverlordResource(
        taskMaster,
        new TaskStorageQueryAdapter(taskStorage),
        null,
        null,
        null,
        new AuthConfig()
    );
    Response response = overlordResource.getLeader();
    Assert.assertEquals(druidNode.getHostAndPort(), response.getEntity());

    final String taskId_0 = "0";
    NoopTask task_0 = new NoopTask(taskId_0, 0, 0, null, null, null);
    response = overlordResource.taskPost(task_0, req);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("task", taskId_0), response.getEntity());

    // Duplicate task - should fail
    response = overlordResource.taskPost(task_0, req);
    Assert.assertEquals(400, response.getStatus());

    // Task payload for task_0 should be present in taskStorage
    response = overlordResource.getTaskPayload(taskId_0);
    Assert.assertEquals(task_0, ((Map) response.getEntity()).get("payload"));

    // Task not present in taskStorage - should fail
    response = overlordResource.getTaskPayload("whatever");
    Assert.assertEquals(404, response.getStatus());

    // Task status of the submitted task should be running
    response = overlordResource.getTaskStatus(taskId_0);
    Assert.assertEquals(taskId_0, ((Map) response.getEntity()).get("task"));
    Assert.assertEquals(
        TaskStatus.running(taskId_0).getStatusCode(),
        ((TaskStatus) ((Map) response.getEntity()).get("status")).getStatusCode()
    );

    // Simulate completion of task_0
    taskCompletionCountDownLatches[Integer.parseInt(taskId_0)].countDown();
    // Wait for taskQueue to handle success status of task_0
    waitForTaskStatus(taskId_0, TaskStatus.Status.SUCCESS);

    // Manually insert task in taskStorage
    // Verifies sync from storage
    final String taskId_1 = "1";
    NoopTask task_1 = new NoopTask(taskId_1, 0, 0, null, null, null);
    taskStorage.insert(task_1, TaskStatus.running(taskId_1));
    // Wait for task runner to run task_1
    runTaskCountDownLatches[Integer.parseInt(taskId_1)].await();

    response = overlordResource.getRunningTasks(req);
    // 1 task that was manually inserted should be in running state
    Assert.assertEquals(1, (((List) response.getEntity()).size()));
    final OverlordResource.TaskResponseObject taskResponseObject = ((List<OverlordResource.TaskResponseObject>) response
        .getEntity()).get(0);
    Assert.assertEquals(taskId_1, taskResponseObject.toJson().get("id"));
    Assert.assertEquals(TASK_LOCATION, taskResponseObject.toJson().get("location"));

    // Simulate completion of task_1
    taskCompletionCountDownLatches[Integer.parseInt(taskId_1)].countDown();
    // Wait for taskQueue to handle success status of task_1
    waitForTaskStatus(taskId_1, TaskStatus.Status.SUCCESS);

    // should return number of tasks which are not in running state
    response = overlordResource.getCompleteTasks(req);
    Assert.assertEquals(2, (((List) response.getEntity()).size()));
    taskMaster.stop();
    Assert.assertFalse(taskMaster.isLeading());
    EasyMock.verify(taskLockbox, taskActionClientFactory);
  }

  /* Wait until the task with given taskId has the given Task Status
   * These method will not timeout until the condition is met so calling method should ensure timeout
   * This method also assumes that the task with given taskId is present
   * */
  private void waitForTaskStatus(String taskId, TaskStatus.Status status) throws InterruptedException
  {
    while (true) {
      Response response = overlordResource.getTaskStatus(taskId);
      if (status.equals(((TaskStatus) ((Map) response.getEntity()).get("status")).getStatusCode())) {
        break;
      }
      Thread.sleep(10);
    }
  }

  @After
  public void tearDown() throws Exception
  {
    tearDownServerAndCurator();
  }

  public static class MockTaskRunner implements TaskRunner
  {
    private CountDownLatch[] completionLatches;
    private CountDownLatch[] runLatches;
    private ConcurrentHashMap<String, TaskRunnerWorkItem> taskRunnerWorkItems;
    private List<String> runningTasks;

    public MockTaskRunner(CountDownLatch[] runLatches, CountDownLatch[] completionLatches)
    {
      this.runLatches = runLatches;
      this.completionLatches = completionLatches;
      this.taskRunnerWorkItems = new ConcurrentHashMap<>();
      this.runningTasks = new ArrayList<>();
    }

    @Override
    public List<Pair<Task, ListenableFuture<TaskStatus>>> restore()
    {
      return ImmutableList.of();
    }

    @Override
    public void registerListener(TaskRunnerListener listener, Executor executor)
    {
      // Overlord doesn't call this method
      throw new UnsupportedOperationException();
    }

    @Override
    public void stop()
    {
      // Do nothing
    }

    @Override
    public synchronized ListenableFuture<TaskStatus> run(final Task task)
    {
      final String taskId = task.getId();
      ListenableFuture<TaskStatus> future = MoreExecutors.listeningDecorator(
          Execs.singleThreaded(
              "noop_test_task_exec_%s"
          )
      ).submit(
          new Callable<TaskStatus>()
          {
            @Override
            public TaskStatus call() throws Exception
            {
              // adding of task to list of runningTasks should be done before count down as
              // getRunningTasks may not include the task for which latch has been counted down
              // Count down to let know that task is actually running
              // this is equivalent of getting process holder to run task in ForkingTaskRunner
              runningTasks.add(taskId);
              if (runLatches != null) {
                runLatches[Integer.parseInt(taskId)].countDown();
              }
              // Wait for completion count down
              if (completionLatches != null) {
                completionLatches[Integer.parseInt(taskId)].await();
              }
              taskRunnerWorkItems.remove(taskId);
              runningTasks.remove(taskId);
              return TaskStatus.success(taskId);
            }
          }
      );
      TaskRunnerWorkItem taskRunnerWorkItem = new TaskRunnerWorkItem(taskId, future)
      {
        @Override
        public TaskLocation getLocation()
        {
          return TASK_LOCATION;
        }
      };
      taskRunnerWorkItems.put(taskId, taskRunnerWorkItem);
      return future;
    }

    @Override
    public void shutdown(String taskid) {}

    @Override
    public synchronized Collection<? extends TaskRunnerWorkItem> getRunningTasks()
    {
      return Lists.transform(
          runningTasks,
          new Function<String, TaskRunnerWorkItem>()
          {
            @Nullable
            @Override
            public TaskRunnerWorkItem apply(String input)
            {
              return taskRunnerWorkItems.get(input);
            }
          }
      );
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getPendingTasks()
    {
      return ImmutableList.of();
    }

    @Override
    public Collection<? extends TaskRunnerWorkItem> getKnownTasks()
    {
      return taskRunnerWorkItems.values();
    }

    @Override
    public Optional<ScalingStats> getScalingStats()
    {
      return Optional.absent();
    }

    @Override
    public void start()
    {
      //Do nothing
    }
  }
}
