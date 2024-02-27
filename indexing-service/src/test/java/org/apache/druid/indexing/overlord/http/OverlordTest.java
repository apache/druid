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

package org.apache.druid.indexing.overlord.http;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryOneTime;
import org.apache.curator.test.TestingServer;
import org.apache.curator.test.Timing;
import org.apache.druid.audit.AuditManager;
import org.apache.druid.curator.PotentiallyGzippedCompressionProvider;
import org.apache.druid.curator.discovery.LatchableServiceAnnouncer;
import org.apache.druid.discovery.DruidLeaderSelector;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.actions.SegmentAllocationQueue;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskStorageConfig;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.HeapMemoryTaskStorage;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageAdapter;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.TaskLockbox;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.indexing.overlord.TaskRunner;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskRunnerListener;
import org.apache.druid.indexing.overlord.TaskRunnerWorkItem;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.TaskStorageQueryAdapter;
import org.apache.druid.indexing.overlord.WorkerTaskRunnerQueryAdapter;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.config.DefaultTaskConfig;
import org.apache.druid.indexing.overlord.config.TaskLockConfig;
import org.apache.druid.indexing.overlord.config.TaskQueueConfig;
import org.apache.druid.indexing.overlord.duty.OverlordDutyExecutor;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.indexing.test.TestIndexerMetadataStorageCoordinator;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.coordinator.CoordinatorOverlordServiceConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthTestUtils;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.utils.CloseableUtils;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;

public class OverlordTest
{
  private static final TaskLocation TASK_LOCATION = TaskLocation.create("dummy", 1000, -1);

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
  private Map<String, CountDownLatch> taskCompletionCountDownLatches;
  private Map<String, CountDownLatch> runTaskCountDownLatches;
  private HttpServletRequest req;
  private SupervisorManager supervisorManager;

  // Bad task's id must be lexicographically greater than the good task's
  private final String goodTaskId = "aaa";
  private final String badTaskId = "zzz";

  private Task task0;
  private Task task1;
  private String taskId0;
  private String taskId1;

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
    CloseableUtils.closeAndWrapExceptions(curator);
    CloseableUtils.closeAndWrapExceptions(server);
  }

  @Before
  public void setUp() throws Exception
  {
    req = EasyMock.createMock(HttpServletRequest.class);
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_AUTHOR)).andReturn("author").once();
    EasyMock.expect(req.getHeader(AuditManager.X_DRUID_COMMENT)).andReturn("comment").once();
    EasyMock.expect(req.getRemoteAddr()).andReturn("127.0.0.1").once();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHENTICATION_RESULT)).andReturn(
        new AuthenticationResult("druid", "druid", null, null)
    ).anyTimes();
    EasyMock.expect(req.getMethod()).andReturn("GET").anyTimes();
    EasyMock.expect(req.getRequestURI()).andReturn("/request/uri").anyTimes();
    EasyMock.expect(req.getQueryString()).andReturn("query=string").anyTimes();

    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_ALLOW_UNSECURED_PATH)).andReturn(null).anyTimes();
    EasyMock.expect(req.getAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED)).andReturn(null).anyTimes();
    req.setAttribute(AuthConfig.DRUID_AUTHORIZATION_CHECKED, true);
    EasyMock.expectLastCall().anyTimes();
    supervisorManager = EasyMock.createMock(SupervisorManager.class);

    taskActionClientFactory = EasyMock.createStrictMock(TaskActionClientFactory.class);
    EasyMock.expect(taskActionClientFactory.create(EasyMock.anyObject()))
            .andReturn(null).anyTimes();
    EasyMock.replay(taskActionClientFactory, req);

    taskStorage = new HeapMemoryTaskStorage(new TaskStorageConfig(null));

    IndexerMetadataStorageCoordinator mdc = new TestIndexerMetadataStorageCoordinator();

    taskLockbox = new TaskLockbox(taskStorage, mdc);

    task0 = NoopTask.create();
    taskId0 = task0.getId();

    task1 = NoopTask.create();
    taskId1 = task1.getId();

    runTaskCountDownLatches = new HashMap<>();
    runTaskCountDownLatches.put(taskId0, new CountDownLatch(1));
    runTaskCountDownLatches.put(taskId1, new CountDownLatch(1));
    taskCompletionCountDownLatches = new HashMap<>();
    taskCompletionCountDownLatches.put(taskId0, new CountDownLatch(1));
    taskCompletionCountDownLatches.put(taskId1, new CountDownLatch(1));
    announcementLatch = new CountDownLatch(1);
    setupServerAndCurator();
    curator.start();
    curator.blockUntilConnected();
    druidNode = new DruidNode("hey", "what", false, 1234, null, true, false);
    ServiceEmitter serviceEmitter = new NoopServiceEmitter();

    // Add two tasks with conflicting locks
    // The bad task (The one with a lexicographically larger name) must be failed
    Task badTask = new NoopTask(badTaskId, badTaskId, "datasource", 10_000, 0, null);
    TaskLock badLock = new TimeChunkLock(null, badTaskId, "datasource", Intervals.ETERNITY, "version1", 50);
    Task goodTask = new NoopTask(goodTaskId, goodTaskId, "datasource", 0, 0, null);
    TaskLock goodLock = new TimeChunkLock(null, goodTaskId, "datasource", Intervals.ETERNITY, "version0", 50);
    taskStorage.insert(goodTask, TaskStatus.running(goodTaskId));
    taskStorage.insert(badTask, TaskStatus.running(badTaskId));
    taskStorage.addLock(badTaskId, badLock);
    taskStorage.addLock(goodTaskId, goodLock);
    runTaskCountDownLatches.put(badTaskId, new CountDownLatch(1));
    runTaskCountDownLatches.put(goodTaskId, new CountDownLatch(1));
    taskCompletionCountDownLatches.put(badTaskId, new CountDownLatch(1));
    taskCompletionCountDownLatches.put(goodTaskId, new CountDownLatch(1));

    TaskRunnerFactory<MockTaskRunner> taskRunnerFactory = new TaskRunnerFactory<MockTaskRunner>()
    {
      private MockTaskRunner runner;

      @Override
      public MockTaskRunner build()
      {
        runner = new MockTaskRunner(runTaskCountDownLatches, taskCompletionCountDownLatches);
        return runner;
      }

      @Override
      public MockTaskRunner get()
      {
        return runner;
      }
    };


    taskRunnerFactory.build().run(badTask);
    taskRunnerFactory.build().run(goodTask);

    taskMaster = new TaskMaster(
        new TaskLockConfig(),
        new TaskQueueConfig(null, new Period(1), null, new Period(10), null),
        new DefaultTaskConfig(),
        taskLockbox,
        taskStorage,
        taskActionClientFactory,
        druidNode,
        taskRunnerFactory,
        new LatchableServiceAnnouncer(announcementLatch, null),
        new CoordinatorOverlordServiceConfig(null, null),
        serviceEmitter,
        supervisorManager,
        EasyMock.createNiceMock(OverlordDutyExecutor.class),
        new TestDruidLeaderSelector(),
        EasyMock.createNiceMock(SegmentAllocationQueue.class)
    );
    EmittingLogger.registerEmitter(serviceEmitter);
  }

  @Test(timeout = 60_000L)
  public void testOverlordRun() throws Exception
  {
    // basic task master lifecycle test
    taskMaster.start();
    announcementLatch.await();
    while (!taskMaster.isLeader()) {
      // I believe the control will never reach here and thread will never sleep but just to be on safe side
      Thread.sleep(10);
    }
    Assert.assertEquals(taskMaster.getCurrentLeader(), druidNode.getHostAndPort());
    Assert.assertEquals(Optional.absent(), taskMaster.getRedirectLocation());

    final TaskStorageQueryAdapter taskStorageQueryAdapter = new TaskStorageQueryAdapter(taskStorage, taskLockbox, taskMaster);
    final WorkerTaskRunnerQueryAdapter workerTaskRunnerQueryAdapter = new WorkerTaskRunnerQueryAdapter(taskMaster, null);
    // Test Overlord resource stuff
    AuditManager auditManager = EasyMock.createNiceMock(AuditManager.class);
    overlordResource = new OverlordResource(
        taskMaster,
        taskStorageQueryAdapter,
        new IndexerMetadataStorageAdapter(taskStorageQueryAdapter, null),
        null,
        null,
        auditManager,
        AuthTestUtils.TEST_AUTHORIZER_MAPPER,
        workerTaskRunnerQueryAdapter,
        null,
        new AuthConfig()
    );
    Response response = overlordResource.getLeader();
    Assert.assertEquals(druidNode.getHostAndPort(), response.getEntity());

    // BadTask must fail due to null task lock
    waitForTaskStatus(badTaskId, TaskState.FAILED);

    // GoodTask must successfully run
    taskCompletionCountDownLatches.get(goodTaskId).countDown();
    waitForTaskStatus(goodTaskId, TaskState.SUCCESS);

    response = overlordResource.taskPost(task0, req);
    Assert.assertEquals(200, response.getStatus());
    Assert.assertEquals(ImmutableMap.of("task", taskId0), response.getEntity());

    // Duplicate task - should fail
    response = overlordResource.taskPost(task0, req);
    Assert.assertEquals(400, response.getStatus());

    // Task payload for task_0 should be present in taskStorage
    response = overlordResource.getTaskPayload(taskId0);
    Assert.assertEquals(task0, ((TaskPayloadResponse) response.getEntity()).getPayload());

    // Task not present in taskStorage - should fail
    response = overlordResource.getTaskPayload("whatever");
    Assert.assertEquals(404, response.getStatus());

    // Task status of the submitted task should be running
    response = overlordResource.getTaskStatus(taskId0);
    Assert.assertEquals(taskId0, ((TaskStatusResponse) response.getEntity()).getTask());
    Assert.assertEquals(
        TaskStatus.running(taskId0).getStatusCode(),
        ((TaskStatusResponse) response.getEntity()).getStatus().getStatusCode()
    );

    // Simulate completion of task_0
    taskCompletionCountDownLatches.get(taskId0).countDown();
    // Wait for taskQueue to handle success status of task_0
    waitForTaskStatus(taskId0, TaskState.SUCCESS);

    // Manually insert task in taskStorage
    // Verifies sync from storage
    taskStorage.insert(task1, TaskStatus.running(taskId1));
    // Wait for task runner to run task_1
    runTaskCountDownLatches.get(taskId1).await();

    response = overlordResource.getRunningTasks(null, req);
    // 1 task that was manually inserted should be in running state
    Assert.assertEquals(1, (((List) response.getEntity()).size()));
    final TaskStatusPlus taskResponseObject = ((List<TaskStatusPlus>) response
        .getEntity()).get(0);
    Assert.assertEquals(taskId1, taskResponseObject.getId());
    Assert.assertEquals(TASK_LOCATION, taskResponseObject.getLocation());

    // Simulate completion of task_1
    taskCompletionCountDownLatches.get(taskId1).countDown();
    // Wait for taskQueue to handle success status of task_1
    waitForTaskStatus(taskId1, TaskState.SUCCESS);

    // should return number of tasks which are not in running state
    response = overlordResource.getCompleteTasks(null, req);
    Assert.assertEquals(4, (((List) response.getEntity()).size()));

    response = overlordResource.getCompleteTasks(1, req);
    Assert.assertEquals(1, (((List) response.getEntity()).size()));
    Assert.assertEquals(1, taskMaster.getStats().rowCount());

    taskMaster.stop();
    Assert.assertFalse(taskMaster.isLeader());
    Assert.assertEquals(0, taskMaster.getStats().rowCount());

    EasyMock.verify(taskActionClientFactory);
  }

  /* Wait until the task with given taskId has the given Task Status
   * These method will not timeout until the condition is met so calling method should ensure timeout
   * This method also assumes that the task with given taskId is present
   * */
  private void waitForTaskStatus(String taskId, TaskState status) throws InterruptedException
  {
    while (true) {
      Response response = overlordResource.getTaskStatus(taskId);
      if (status.equals(((TaskStatusResponse) response.getEntity()).getStatus().getStatusCode())) {
        break;
      }
      Thread.sleep(10);
    }
  }

  @After
  public void tearDown()
  {
    tearDownServerAndCurator();
  }

  public static class MockTaskRunner implements TaskRunner
  {
    private Map<String, CountDownLatch> completionLatches;
    private Map<String, CountDownLatch> runLatches;
    private ConcurrentHashMap<String, TaskRunnerWorkItem> taskRunnerWorkItems;
    private List<String> runningTasks;

    public MockTaskRunner(Map<String, CountDownLatch> runLatches, Map<String, CountDownLatch> completionLatches)
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
    public void unregisterListener(String listenerId)
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
                runLatches.get(taskId).countDown();
              }
              // Wait for completion count down
              if (completionLatches != null) {
                completionLatches.get(taskId).await();
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

        @Override
        public String getTaskType()
        {
          return task.getType();
        }

        @Override
        public String getDataSource()
        {
          return task.getDataSource();
        }

      };
      taskRunnerWorkItems.put(taskId, taskRunnerWorkItem);
      return future;
    }

    @Override
    public void shutdown(String taskid, String reason)
    {
      runningTasks.remove(taskid);
    }

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
    public Collection<TaskRunnerWorkItem> getPendingTasks()
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
    public Map<String, Long> getTotalTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getIdleTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getUsedTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getLazyTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Map<String, Long> getBlacklistedTaskSlotCount()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void start()
    {
      //Do nothing
    }
  }

  private static class TestDruidLeaderSelector implements DruidLeaderSelector
  {
    private volatile Listener listener;
    private volatile String leader;

    @Override
    public String getCurrentLeader()
    {
      return leader;
    }

    @Override
    public boolean isLeader()
    {
      return leader != null;
    }

    @Override
    public int localTerm()
    {
      return 0;
    }

    @Override
    public void registerListener(Listener listener)
    {
      this.listener = listener;

      leader = "what:1234";
      listener.becomeLeader();
    }

    @Override
    public void unregisterListener()
    {
      leader = null;
      listener.stopBeingLeader();
    }
  }
}
