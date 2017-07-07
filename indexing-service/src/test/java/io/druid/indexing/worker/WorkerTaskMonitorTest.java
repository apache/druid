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

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.indexing.common.IndexingServiceCondition;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestRealtimeTask;
import io.druid.indexing.common.TestTasks;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.actions.TaskActionClient;
import io.druid.indexing.common.actions.TaskActionClientFactory;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.overlord.TestRemoteTaskRunnerConfig;
import io.druid.indexing.overlord.ThreadPoolTaskRunner;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMergerV9;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.SegmentLoaderLocalCacheManager;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.segment.realtime.plumber.SegmentHandoffNotifierFactory;
import io.druid.server.DruidNode;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.server.initialization.ServerConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.server.metrics.NoopServiceEmitter;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 */
public class WorkerTaskMonitorTest
{
  private static final Joiner joiner = Joiner.on("/");
  private static final String basePath = "/test/druid";
  private static final String tasksPath = StringUtils.format("%s/indexer/tasks/worker", basePath);
  private static final String statusPath = StringUtils.format("%s/indexer/status/worker", basePath);
  private static final DruidNode DUMMY_NODE = new DruidNode("dummy", "dummy", 9000, null, new ServerConfig());

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private WorkerCuratorCoordinator workerCuratorCoordinator;
  private WorkerTaskMonitor workerTaskMonitor;

  private Task task;

  private Worker worker;
  private ObjectMapper jsonMapper;
  private IndexMergerV9 indexMergerV9;
  private IndexIO indexIO;

  public WorkerTaskMonitorTest()
  {
    TestUtils testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    indexMergerV9 = testUtils.getTestIndexMergerV9();
    indexIO = testUtils.getTestIndexIO();
  }

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
    cf.blockUntilConnected();
    cf.create().creatingParentsIfNeeded().forPath(basePath);

    worker = new Worker(
        "http",
        "worker",
        "localhost",
        3,
        "0"
    );

    workerCuratorCoordinator = new WorkerCuratorCoordinator(
        jsonMapper,
        new IndexerZkConfig(
            new ZkPathsConfig()
            {
              @Override
              public String getBase()
              {
                return basePath;
              }
            }, null, null, null, null, null
        ),
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        cf,
        worker
    );
    workerCuratorCoordinator.start();


    // Start a task monitor
    workerTaskMonitor = createTaskMonitor();
    TestTasks.registerSubtypes(jsonMapper);
    jsonMapper.registerSubtypes(new NamedType(TestRealtimeTask.class, "test_realtime"));
    workerTaskMonitor.start();

    task = TestTasks.immediateSuccess("test");
  }

  private WorkerTaskMonitor createTaskMonitor()
  {
    final TaskConfig taskConfig = new TaskConfig(
        Files.createTempDir().toString(),
        null,
        null,
        0,
        null,
        false,
        null,
        null
    );
    TaskActionClientFactory taskActionClientFactory = EasyMock.createNiceMock(TaskActionClientFactory.class);
    TaskActionClient taskActionClient = EasyMock.createNiceMock(TaskActionClient.class);
    EasyMock.expect(taskActionClientFactory.create(EasyMock.<Task>anyObject())).andReturn(taskActionClient).anyTimes();
    SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    EasyMock.replay(taskActionClientFactory, taskActionClient, notifierFactory);
    return new WorkerTaskMonitor(
        jsonMapper,
        cf,
        workerCuratorCoordinator,
        new ThreadPoolTaskRunner(
            new TaskToolboxFactory(
                taskConfig,
                taskActionClientFactory,
                null, null, null, null, null, null, null, notifierFactory, null, null, null, new SegmentLoaderFactory(
                new SegmentLoaderLocalCacheManager(
                    null,
                    new SegmentLoaderConfig()
                    {
                      @Override
                      public List<StorageLocationConfig> getLocations()
                      {
                        return Lists.newArrayList();
                      }
                    }
                    , jsonMapper
                )
            ),
                jsonMapper,
                indexIO,
                null,
                null,
                indexMergerV9
            ),
            taskConfig,
            new NoopServiceEmitter(),
            DUMMY_NODE
        )
    );
  }

  @After
  public void tearDown() throws Exception
  {
    workerCuratorCoordinator.stop();
    workerTaskMonitor.stop();
    cf.close();
    testingCluster.stop();
  }

  @Test(timeout = 30_000L)
  public void testRunTask() throws Exception
  {
    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  return cf.checkExists().forPath(joiner.join(tasksPath, task.getId())) == null;
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );

    cf.create()
      .creatingParentsIfNeeded()
      .forPath(joiner.join(tasksPath, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  final byte[] bytes = cf.getData().forPath(joiner.join(statusPath, task.getId()));
                  final TaskAnnouncement announcement = jsonMapper.readValue(
                      bytes,
                      TaskAnnouncement.class
                  );
                  return announcement.getTaskStatus().isComplete();
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );

    TaskAnnouncement taskAnnouncement = jsonMapper.readValue(
        cf.getData().forPath(joiner.join(statusPath, task.getId())), TaskAnnouncement.class
    );

    Assert.assertEquals(task.getId(), taskAnnouncement.getTaskStatus().getId());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, taskAnnouncement.getTaskStatus().getStatusCode());
  }

  @Test(timeout = 30_000L)
  public void testGetAnnouncements() throws Exception
  {
    cf.create()
      .creatingParentsIfNeeded()
      .forPath(joiner.join(tasksPath, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  final byte[] bytes = cf.getData().forPath(joiner.join(statusPath, task.getId()));
                  final TaskAnnouncement announcement = jsonMapper.readValue(
                      bytes,
                      TaskAnnouncement.class
                  );
                  return announcement.getTaskStatus().isComplete();
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );

    List<TaskAnnouncement> announcements = workerCuratorCoordinator.getAnnouncements();
    Assert.assertEquals(1, announcements.size());
    Assert.assertEquals(task.getId(), announcements.get(0).getTaskStatus().getId());
    Assert.assertEquals(TaskStatus.Status.SUCCESS, announcements.get(0).getTaskStatus().getStatusCode());
    Assert.assertEquals(DUMMY_NODE.getHost(), announcements.get(0).getTaskLocation().getHost());
    Assert.assertEquals(DUMMY_NODE.getPlaintextPort(), announcements.get(0).getTaskLocation().getPort());
  }

  @Test(timeout = 30_000L)
  public void testRestartCleansOldStatus() throws Exception
  {
    task = TestTasks.unending("test");

    cf.create()
      .creatingParentsIfNeeded()
      .forPath(joiner.join(tasksPath, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  return cf.checkExists().forPath(joiner.join(statusPath, task.getId())) != null;
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );
    // simulate node restart
    workerTaskMonitor.stop();
    workerTaskMonitor = createTaskMonitor();
    workerTaskMonitor.start();
    List<TaskAnnouncement> announcements = workerCuratorCoordinator.getAnnouncements();
    Assert.assertEquals(1, announcements.size());
    Assert.assertEquals(task.getId(), announcements.get(0).getTaskStatus().getId());
    Assert.assertEquals(TaskStatus.Status.FAILED, announcements.get(0).getTaskStatus().getStatusCode());
  }

  @Test(timeout = 30_000L)
  public void testStatusAnnouncementsArePersistent() throws Exception
  {
    cf.create()
      .creatingParentsIfNeeded()
      .forPath(joiner.join(tasksPath, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  return cf.checkExists().forPath(joiner.join(statusPath, task.getId())) != null;
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );
    // ephemeral owner is 0 is created node is PERSISTENT
    Assert.assertEquals(0, cf.checkExists().forPath(joiner.join(statusPath, task.getId())).getEphemeralOwner());

  }
}
