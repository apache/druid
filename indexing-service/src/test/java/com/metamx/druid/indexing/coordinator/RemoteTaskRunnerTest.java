package com.metamx.druid.indexing.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.metamx.common.ISE;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.curator.PotentiallyGzippedCompressionProvider;
import com.metamx.druid.indexing.TestTask;
import com.metamx.druid.indexing.common.RetryPolicyFactory;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.TaskToolboxFactory;
import com.metamx.druid.indexing.common.config.IndexerZkConfig;
import com.metamx.druid.indexing.common.config.RetryPolicyConfig;
import com.metamx.druid.indexing.common.config.TaskConfig;
import com.metamx.druid.indexing.coordinator.config.RemoteTaskRunnerConfig;
import com.metamx.druid.indexing.coordinator.setup.WorkerSetupData;
import com.metamx.druid.indexing.worker.Worker;
import com.metamx.druid.indexing.worker.WorkerCuratorCoordinator;
import com.metamx.druid.indexing.worker.WorkerTaskMonitor;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.service.ServiceEmitter;
import org.apache.commons.lang.mutable.MutableBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.CreateMode;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static junit.framework.Assert.fail;

/**
 */
public class RemoteTaskRunnerTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private static final String basePath = "/test/druid/indexer";
  private static final String announcementsPath = String.format("%s/announcements", basePath);
  private static final String tasksPath = String.format("%s/tasks", basePath);
  private static final String statusPath = String.format("%s/status", basePath);

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private PathChildrenCache pathChildrenCache;
  private RemoteTaskRunner remoteTaskRunner;
  private WorkerTaskMonitor workerTaskMonitor;

  private ScheduledExecutorService scheduledExec;

  private TestTask task1;

  private Worker worker1;

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

    cf.create().creatingParentsIfNeeded().forPath(announcementsPath);
    cf.create().forPath(tasksPath);
    cf.create().forPath(String.format("%s/worker1", tasksPath));
    cf.create().forPath(statusPath);
    cf.create().forPath(String.format("%s/worker1", statusPath));

    pathChildrenCache = new PathChildrenCache(cf, announcementsPath, true);

    worker1 = new Worker(
        "worker1",
        "localhost",
        3,
        "0"
    );

    task1 = new TestTask(
        "task1",
        "dummyDs",
        Lists.<DataSegment>newArrayList(
            new DataSegment(
                "dummyDs",
                new Interval(new DateTime(), new DateTime()),
                new DateTime().toString(),
                null,
                null,
                null,
                null,
                0,
                0
            )
        ),
        Lists.<AggregatorFactory>newArrayList(),
        TaskStatus.success("task1")
    );

    makeRemoteTaskRunner();
    makeTaskMonitor();
  }

  @After
  public void tearDown() throws Exception
  {
    testingCluster.stop();
    remoteTaskRunner.stop();
    workerTaskMonitor.stop();
  }

  @Test
  public void testRunNoExistingTask() throws Exception
  {
    remoteTaskRunner.run(task1);
  }

  @Test
  public void testExceptionThrownWithExistingTask() throws Exception
  {
    remoteTaskRunner.run(
        new TestTask(
            task1.getId(),
            task1.getDataSource(),
            task1.getSegments(),
            Lists.<AggregatorFactory>newArrayList(),
            TaskStatus.running(task1.getId())
        )
    );
    try {
      remoteTaskRunner.run(task1);
      fail("ISE expected");
    }
    catch (ISE expected) {
    }
  }

  @Test
  public void testRunTooMuchZKData() throws Exception
  {
    ServiceEmitter emitter = EasyMock.createMock(ServiceEmitter.class);
    EmittingLogger.registerEmitter(emitter);
    EasyMock.replay(emitter);
    remoteTaskRunner.run(
        new TestTask(
            new String(new char[5000]),
            "dummyDs",
            Lists.<DataSegment>newArrayList(
                new DataSegment(
                    "dummyDs",
                    new Interval(new DateTime(), new DateTime()),
                    new DateTime().toString(),
                    null,
                    null,
                    null,
                    null,
                    0,
                    0
                )
            ),
            Lists.<AggregatorFactory>newArrayList(),
            TaskStatus.success("foo")
        )
    );
    EasyMock.verify(emitter);
  }

  @Test
  public void testRunWithCallback() throws Exception
  {
    final MutableBoolean callbackCalled = new MutableBoolean(false);

    Futures.addCallback(
        remoteTaskRunner.run(
            new TestTask(
                task1.getId(),
                task1.getDataSource(),
                task1.getSegments(),
                Lists.<AggregatorFactory>newArrayList(),
                TaskStatus.running(task1.getId())
            )
        ), new FutureCallback<TaskStatus>()
    {
      @Override
      public void onSuccess(TaskStatus taskStatus)
      {
        callbackCalled.setValue(true);
      }

      @Override
      public void onFailure(Throwable throwable)
      {
        // neg
      }
    }
    );

    // Really don't like this way of waiting for the task to appear
    int count = 0;
    while (remoteTaskRunner.findWorkerRunningTask(task1.getId()) == null) {
      Thread.sleep(500);
      if (count > 10) {
        throw new ISE("WTF?! Task still not announced in ZK?");
      }
      count++;
    }

    Assert.assertTrue(remoteTaskRunner.getRunningTasks().size() == 1);

    // Complete the task
    cf.setData().forPath(
        String.format("%s/worker1/task1", statusPath),
        jsonMapper.writeValueAsBytes(TaskStatus.success(task1.getId()))
    );

    // Really don't like this way of waiting for the task to disappear
    count = 0;
    while (remoteTaskRunner.findWorkerRunningTask(task1.getId()) != null) {
      Thread.sleep(500);
      if (count > 10) {
        throw new ISE("WTF?! Task still exists in ZK?");
      }
      count++;
    }

    Assert.assertTrue("TaskCallback was not called!", callbackCalled.booleanValue());
  }


  @Test
  public void testRunSameAvailabilityGroup() throws Exception
  {
    TestRealtimeTask theTask = new TestRealtimeTask("rt1", "rt1", "foo", TaskStatus.running("rt1"));
    remoteTaskRunner.run(theTask);
    remoteTaskRunner.run(
        new TestRealtimeTask("rt2", "rt1", "foo", TaskStatus.running("rt2"))
    );
    remoteTaskRunner.run(
        new TestRealtimeTask("rt3", "rt2", "foo", TaskStatus.running("rt3"))
    );

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();
    while (remoteTaskRunner.getRunningTasks().isEmpty()) {
      Thread.sleep(100);
      if (stopwatch.elapsed(TimeUnit.MILLISECONDS) > 1000) {
        throw new ISE("Cannot find running task");
      }
    }

    Assert.assertTrue(remoteTaskRunner.getRunningTasks().size() == 2);
    Assert.assertTrue(remoteTaskRunner.getPendingTasks().size() == 1);
    Assert.assertTrue(remoteTaskRunner.getPendingTasks().iterator().next().getTask().getId().equals("rt2"));
  }


  private void makeTaskMonitor() throws Exception
  {
    WorkerCuratorCoordinator workerCuratorCoordinator = new WorkerCuratorCoordinator(
        jsonMapper,
        new IndexerZkConfig()
        {
          @Override
          public String getIndexerAnnouncementPath()
          {
            return announcementsPath;
          }

          @Override
          public String getIndexerTaskPath()
          {
            return tasksPath;
          }

          @Override
          public String getIndexerStatusPath()
          {
            return statusPath;
          }

          @Override
          public String getZkBasePath()
          {
            throw new UnsupportedOperationException();
          }

          @Override
          public long getMaxNumBytes()
          {
            return 1000;
          }
        },
        cf,
        worker1
    );
    workerCuratorCoordinator.start();

    workerTaskMonitor = new WorkerTaskMonitor(
        jsonMapper,
        new PathChildrenCache(cf, String.format("%s/worker1", tasksPath), true),
        cf,
        workerCuratorCoordinator,
        new ThreadPoolTaskRunner(
            new TaskToolboxFactory(
                new TaskConfig()
                {
                  @Override
                  public File getBaseTaskDir()
                  {
                    try {
                      return File.createTempFile("billy", "yay");
                    }
                    catch (Exception e) {
                      throw Throwables.propagate(e);
                    }
                  }

                  @Override
                  public int getDefaultRowFlushBoundary()
                  {
                    return 0;
                  }

                  @Override
                  public String getHadoopWorkingPath()
                  {
                    return null;
                  }
                }, null, null, null, null, null, null, null, null, jsonMapper
            ), Executors.newSingleThreadExecutor()
        ),
        Executors.newSingleThreadExecutor()
    );
    jsonMapper.registerSubtypes(new NamedType(TestTask.class, "test"));
    jsonMapper.registerSubtypes(new NamedType(TestRealtimeTask.class, "test_realtime"));
    workerTaskMonitor.start();
  }

  private void makeRemoteTaskRunner() throws Exception
  {
    scheduledExec = EasyMock.createMock(ScheduledExecutorService.class);

    remoteTaskRunner = new RemoteTaskRunner(
        jsonMapper,
        new TestRemoteTaskRunnerConfig(),
        cf,
        pathChildrenCache,
        scheduledExec,
        new RetryPolicyFactory(new TestRetryPolicyConfig()),
        new AtomicReference<WorkerSetupData>(new WorkerSetupData("0", 0, 1, null, null)),
        null
    );

    // Create a single worker and wait for things for be ready
    remoteTaskRunner.start();
    cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
        String.format("%s/worker1", announcementsPath),
        jsonMapper.writeValueAsBytes(worker1)
    );
    int count = 0;
    while (remoteTaskRunner.getWorkers().size() == 0) {
      Thread.sleep(500);
      if (count > 10) {
        throw new ISE("WTF?! Still can't find worker!");
      }
      count++;
    }
  }

  private static class TestRetryPolicyConfig extends RetryPolicyConfig
  {
    @Override
    public Duration getRetryMinDuration()
    {
      return null;
    }

    @Override
    public Duration getRetryMaxDuration()
    {
      return null;
    }

    @Override
    public long getMaxRetryCount()
    {
      return 0;
    }
  }

  private static class TestRemoteTaskRunnerConfig extends RemoteTaskRunnerConfig
  {
    @Override
    public String getIndexerAnnouncementPath()
    {
      return announcementsPath;
    }

    @Override
    public String getIndexerTaskPath()
    {
      return tasksPath;
    }

    @Override
    public String getIndexerStatusPath()
    {
      return statusPath;
    }

    @Override
    public String getZkBasePath()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public Duration getTaskAssignmentTimeoutDuration()
    {
      return new Duration(60000);
    }

    @Override
    public long getMaxNumBytes()
    {
      return 1000;
    }
  }
}
