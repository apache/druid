package com.metamx.druid.merger.coordinator;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.metamx.common.ISE;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.common.config.IndexerZkConfig;
import com.metamx.druid.merger.common.task.DefaultMergeTask;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
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
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.annotate.JsonTypeName;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.jsontype.NamedType;
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

  private Task task1;

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
        new TaskContext(new DateTime().toString(), Sets.<DataSegment>newHashSet()),
        null
    );
  }

  @Test
  public void testAlreadyExecutedTask() throws Exception
  {
    remoteTaskRunner.run(task1, new TaskContext(new DateTime().toString(), Sets.<DataSegment>newHashSet()), null);
    try {
      remoteTaskRunner.run(task1, new TaskContext(new DateTime().toString(), Sets.<DataSegment>newHashSet()), null);
      fail("ISE expected");
    }
    catch (ISE expected) {

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
                      0
                  )
              ), Lists.<AggregatorFactory>newArrayList()
          ),
          new TaskContext(new DateTime().toString(), Sets.<DataSegment>newHashSet()),
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
            TaskStatus.success(
                "task1",
                Lists.<DataSegment>newArrayList()
            )
        )
    );

    // Really don't like this way of waiting for the task to appear
    while (remoteTaskRunner.getNumWorkers() == 0) {
      Thread.sleep(500);
    }

    final MutableBoolean callbackCalled = new MutableBoolean(false);
    remoteTaskRunner.run(
        task1,
        null,
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
            new IndexerCoordinatorConfig()
            {
              @Override
              public String getServerName()
              {
                return "worker1";
              }

              @Override
              public String getLeaderLatchPath()
              {
                return null;
              }

              @Override
              public int getNumLocalThreads()
              {
                return 1;
              }

              @Override
              public String getRunnerImpl()
              {
                return null;
              }

              @Override
              public String getStorageImpl()
              {
                return null;
              }

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
              public boolean isWhitelistEnabled()
              {
                return false;
              }

              @Override
              public String getWhitelistDatasourcesString()
              {
                return null;
              }

              @Override
              public long getRowFlushBoundary()
              {
                return 0;
              }


              @Override
              public String getStrategyImpl()
              {
                return null;
              }
            }, null, null, null, jsonMapper
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
    while (remoteTaskRunner.getNumWorkers() == 0) {
      Thread.sleep(500);
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

    public TestTask(
        @JsonProperty("id") String id,
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("segments") List<DataSegment> segments,
        @JsonProperty("aggregations") List<AggregatorFactory> aggregators
    )
    {
      super(dataSource, segments, aggregators);

      this.id = id;
    }

    @Override
    @JsonProperty
    public String getId()
    {
      return id;
    }

    @Override
    public Type getType()
    {
      return Type.TEST;
    }

    @Override
    public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception
    {
      return TaskStatus.success("task1", Lists.<DataSegment>newArrayList());
    }
  }
}
