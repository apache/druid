package com.metamx.druid.merger.coordinator;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.ISE;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.common.config.TaskConfig;
import com.metamx.druid.merger.common.task.DefaultMergeTask;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.RemoteTaskRunnerConfig;
import com.metamx.druid.merger.coordinator.config.RetryPolicyConfig;
import com.metamx.druid.merger.coordinator.scaling.AutoScalingData;
import com.metamx.druid.merger.coordinator.scaling.ScalingStrategy;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupData;
import com.metamx.druid.merger.coordinator.setup.WorkerSetupManager;
import com.metamx.druid.merger.worker.TaskMonitor;
import com.metamx.druid.merger.worker.Worker;
import com.metamx.druid.merger.worker.WorkerCuratorCoordinator;
import com.netflix.curator.framework.CuratorFramework;
import com.netflix.curator.framework.CuratorFrameworkFactory;
import com.netflix.curator.framework.recipes.cache.PathChildrenCache;
import com.netflix.curator.retry.ExponentialBackoffRetry;
import com.netflix.curator.test.TestingCluster;
import org.apache.commons.lang.mutable.MutableBoolean;
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
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

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
  private TaskMonitor taskMonitor;
  private WorkerSetupManager workerSetupManager;

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
                null,
                0
            )
        ), Lists.<AggregatorFactory>newArrayList()
    );

    makeRemoteTaskRunner();
    makeTaskMonitor();
  }

  @After
  public void tearDown() throws Exception
  {
    testingCluster.stop();
    remoteTaskRunner.stop();
    taskMonitor.stop();
  }

  @Test
  public void testRunNoExistingTask() throws Exception
  {
    remoteTaskRunner.run(
        task1,
        null
    );
  }

  @Test
  public void testAlreadyExecutedTask() throws Exception
  {
    final CountDownLatch latch = new CountDownLatch(1);
    remoteTaskRunner.run(
        new TestTask(task1){
          @Override
          public TaskStatus run(TaskToolbox toolbox) throws Exception
          {
            latch.await();
            return super.run(toolbox);
          }
        },
        null
    );
    try {
      remoteTaskRunner.run(task1, null);
      latch.countDown();
      fail("ISE expected");
    }
    catch (ISE expected) {
      latch.countDown();
    }
  }

  @Test
  public void testRunTooMuchZKData() throws Exception
  {
    boolean exceptionOccurred = false;
    try {
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
                      null,
                      0
                  )
              ), Lists.<AggregatorFactory>newArrayList()
          ),
          null
      );
    }
    catch (IllegalStateException e) {
      exceptionOccurred = true;
    }
    Assert.assertTrue(exceptionOccurred);
  }

  @Test
  public void testRunWithExistingCompletedTask() throws Exception
  {
    cf.create().creatingParentsIfNeeded().forPath(
        String.format("%s/worker1/task1", statusPath),
        jsonMapper.writeValueAsBytes(
            TaskStatus.success("task1")
        )
    );

    // Really don't like this way of waiting for the task to appear
    int count = 0;
    while (!remoteTaskRunner.isTaskRunning("task1")) {
      Thread.sleep(500);
      if (count > 10) {
        throw new ISE("WTF?! Task still not announced in ZK?");
      }
      count++;
    }

    final MutableBoolean callbackCalled = new MutableBoolean(false);
    remoteTaskRunner.run(
        task1,
        new TaskCallback()
        {
          @Override
          public void notify(TaskStatus status)
          {
            callbackCalled.setValue(true);
          }
        }
    );

    Assert.assertTrue("TaskCallback was not called!", callbackCalled.booleanValue());
  }

  private void makeTaskMonitor() throws Exception
  {
    WorkerCuratorCoordinator workerCuratorCoordinator = new WorkerCuratorCoordinator(
        jsonMapper,
        new IndexerZkConfig()
        {
          @Override
          public String getAnnouncementPath()
          {
            return announcementsPath;
          }

          @Override
          public String getTaskPath()
          {
            return tasksPath;
          }

          @Override
          public String getStatusPath()
          {
            return statusPath;
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

    taskMonitor = new TaskMonitor(
        new PathChildrenCache(cf, String.format("%s/worker1", tasksPath), true),
        cf,
        workerCuratorCoordinator,
        new TaskToolbox(
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
            }, null, null, null, null, null, jsonMapper
        ),
        Executors.newSingleThreadExecutor()
    );
    jsonMapper.registerSubtypes(new NamedType(TestTask.class, "test"));
    taskMonitor.start();
  }

  private void makeRemoteTaskRunner() throws Exception
  {
    scheduledExec = EasyMock.createMock(ScheduledExecutorService.class);
    workerSetupManager = EasyMock.createMock(WorkerSetupManager.class);

    EasyMock.expect(workerSetupManager.getWorkerSetupData()).andReturn(
        new WorkerSetupData(
            "0",
            0,
            null,
            null
        )
    );
    EasyMock.replay(workerSetupManager);

    remoteTaskRunner = new RemoteTaskRunner(
        jsonMapper,
        new TestRemoteTaskRunnerConfig(),
        cf,
        pathChildrenCache,
        scheduledExec,
        new RetryPolicyFactory(new TestRetryPolicyConfig()),
        new TestScalingStrategy(),
        workerSetupManager
    );

    // Create a single worker and wait for things for be ready
    remoteTaskRunner.start();
    cf.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL).forPath(
        String.format("%s/worker1", announcementsPath),
        jsonMapper.writeValueAsBytes(worker1)
    );
    int count = 0;
    while (remoteTaskRunner.getNumWorkers() == 0) {
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

  private static class TestScalingStrategy<T> implements ScalingStrategy<T>
  {
    @Override
    public AutoScalingData provision()
    {
      return null;
    }

    @Override
    public AutoScalingData terminate(List<String> nodeIds)
    {
      return null;
    }

    @Override
    public List<String> ipLookup(List<String> ips)
    {
      return ips;
    }
  }

  private static class TestRemoteTaskRunnerConfig extends RemoteTaskRunnerConfig
  {
    @Override
    public Duration getTerminateResourcesDuration()
    {
      return null;
    }

    @Override
    public DateTime getTerminateResourcesOriginDateTime()
    {
      return null;
    }

    @Override
    public int getMaxWorkerIdleTimeMillisBeforeDeletion()
    {
      return 0;
    }

    @Override
    public Duration getMaxScalingDuration()
    {
      return null;
    }

    @Override
    public String getAnnouncementPath()
    {
      return announcementsPath;
    }

    @Override
    public String getTaskPath()
    {
      return tasksPath;
    }

    @Override
    public String getStatusPath()
    {
      return statusPath;
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

  @JsonTypeName("test")
  private static class TestTask extends DefaultMergeTask
  {
    private final String id;
    private final String dataSource;
    private final List<DataSegment> segments;
    private final List<AggregatorFactory> aggregators;

    @JsonCreator
    public TestTask(
        @JsonProperty("id") String id,
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("segments") List<DataSegment> segments,
        @JsonProperty("aggregations") List<AggregatorFactory> aggregators
    )
    {
      super(dataSource, segments, aggregators);

      this.id = id;
      this.dataSource = dataSource;
      this.segments = segments;
      this.aggregators = aggregators;
    }

    public TestTask(TestTask task)
    {
      this(task.id, task.dataSource, task.segments, task.aggregators);
    }

    @Override
    @JsonProperty
    public String getId()
    {
      return id;
    }

    @Override
    public String getType()
    {
      return "test";
    }

    @Override
    public TaskStatus run(TaskToolbox toolbox) throws Exception
    {
      return TaskStatus.success("task1");
    }
  }
}
