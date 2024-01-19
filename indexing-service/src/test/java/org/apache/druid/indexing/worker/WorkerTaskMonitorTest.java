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

package org.apache.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Joiner;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.apache.druid.client.coordinator.NoopCoordinatorClient;
import org.apache.druid.client.indexing.NoopOverlordClient;
import org.apache.druid.curator.PotentiallyGzippedCompressionProvider;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.common.IndexingServiceCondition;
import org.apache.druid.indexing.common.SegmentCacheManagerFactory;
import org.apache.druid.indexing.common.TaskToolboxFactory;
import org.apache.druid.indexing.common.TestIndexTask;
import org.apache.druid.indexing.common.TestTasks;
import org.apache.druid.indexing.common.TestUtils;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.common.config.TaskConfigBuilder;
import org.apache.druid.indexing.common.task.NoopTestTaskReportFileWriter;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TestAppenderatorsManager;
import org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner;
import org.apache.druid.indexing.overlord.TestRemoteTaskRunnerConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexMergerV9Factory;
import org.apache.druid.segment.handoff.SegmentHandoffNotifierFactory;
import org.apache.druid.segment.join.NoopJoinableFactory;
import org.apache.druid.segment.metadata.CentralizedDatasourceSchemaConfig;
import org.apache.druid.segment.realtime.firehose.NoopChatHandlerProvider;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.initialization.IndexerZkConfig;
import org.apache.druid.server.initialization.ServerConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.server.metrics.NoopServiceEmitter;
import org.apache.druid.server.security.AuthTestUtils;
import org.easymock.EasyMock;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class WorkerTaskMonitorTest
{
  private static final Joiner JOINER = Joiner.on("/");
  private static final String BASE_PATH = "/test/druid";
  private static final String TASKS_PATH = StringUtils.format("%s/indexer/tasks/worker", BASE_PATH);
  private static final String STATUS_PATH = StringUtils.format("%s/indexer/status/worker", BASE_PATH);
  private static final DruidNode DUMMY_NODE = new DruidNode("dummy", "dummy", false, 9000, null, true, false);

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private WorkerCuratorCoordinator workerCuratorCoordinator;
  private WorkerTaskMonitor workerTaskMonitor;

  private Task task;

  private Worker worker;
  private final TestUtils testUtils;
  private ObjectMapper jsonMapper;
  private IndexMergerV9Factory indexMergerV9Factory;
  private IndexIO indexIO;

  public WorkerTaskMonitorTest()
  {
    testUtils = new TestUtils();
    jsonMapper = testUtils.getTestObjectMapper();
    indexMergerV9Factory = testUtils.getIndexMergerV9Factory();
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
    cf.create().creatingParentsIfNeeded().forPath(BASE_PATH);

    worker = new Worker(
        "http",
        "worker",
        "localhost",
        3,
        "0",
        WorkerConfig.DEFAULT_CATEGORY
    );

    workerCuratorCoordinator = new WorkerCuratorCoordinator(
        jsonMapper,
        new IndexerZkConfig(
            new ZkPathsConfig()
            {
              @Override
              public String getBase()
              {
                return BASE_PATH;
              }
            }, null, null, null, null
        ),
        new TestRemoteTaskRunnerConfig(new Period("PT1S")),
        cf,
        worker
    );
    workerCuratorCoordinator.start();


    // Start a task monitor
    workerTaskMonitor = createTaskMonitor();
    TestTasks.registerSubtypes(jsonMapper);
    jsonMapper.registerSubtypes(new NamedType(TestIndexTask.class, "test_index"));
    workerTaskMonitor.start();

    task = TestTasks.immediateSuccess("test");
  }

  private WorkerTaskMonitor createTaskMonitor()
  {
    final TaskConfig taskConfig = new TaskConfigBuilder()
        .setBaseDir(FileUtils.createTempDir().toString())
        .setDefaultRowFlushBoundary(0)
        .setBatchProcessingMode(TaskConfig.BATCH_PROCESSING_MODE_DEFAULT.name())
        .build();

    TaskActionClientFactory taskActionClientFactory = EasyMock.createNiceMock(TaskActionClientFactory.class);
    TaskActionClient taskActionClient = EasyMock.createNiceMock(TaskActionClient.class);
    EasyMock.expect(taskActionClientFactory.create(EasyMock.anyObject())).andReturn(taskActionClient).anyTimes();
    SegmentHandoffNotifierFactory notifierFactory = EasyMock.createNiceMock(SegmentHandoffNotifierFactory.class);
    EasyMock.replay(taskActionClientFactory, taskActionClient, notifierFactory);
    return new WorkerTaskMonitor(
        jsonMapper,
        new SingleTaskBackgroundRunner(
            new TaskToolboxFactory(
                null,
                taskConfig,
                null,
                taskActionClientFactory,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                notifierFactory,
                null,
                null,
                NoopJoinableFactory.INSTANCE,
                null,
                new SegmentCacheManagerFactory(jsonMapper),
                jsonMapper,
                indexIO,
                null,
                null,
                null,
                indexMergerV9Factory,
                null,
                null,
                null,
                null,
                new NoopTestTaskReportFileWriter(),
                null,
                AuthTestUtils.TEST_AUTHORIZER_MAPPER,
                new NoopChatHandlerProvider(),
                testUtils.getRowIngestionMetersFactory(),
                new TestAppenderatorsManager(),
                new NoopOverlordClient(),
                new NoopCoordinatorClient(),
                null,
                null,
                null,
                "1",
                CentralizedDatasourceSchemaConfig.create()
            ),
            taskConfig,
            new NoopServiceEmitter(),
            DUMMY_NODE,
            new ServerConfig()
        ),
        taskConfig,
        cf,
        workerCuratorCoordinator,
        EasyMock.createNiceMock(OverlordClient.class)
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

  @Test(timeout = 60_000L)
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
                  return cf.checkExists().forPath(JOINER.join(TASKS_PATH, task.getId())) == null;
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
      .forPath(JOINER.join(TASKS_PATH, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  final byte[] bytes = cf.getData().forPath(JOINER.join(STATUS_PATH, task.getId()));
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
        cf.getData().forPath(JOINER.join(STATUS_PATH, task.getId())), TaskAnnouncement.class
    );

    Assert.assertEquals(task.getId(), taskAnnouncement.getTaskStatus().getId());
    Assert.assertEquals(TaskState.SUCCESS, taskAnnouncement.getTaskStatus().getStatusCode());
  }

  @Test(timeout = 60_000L)
  public void testGetAnnouncements() throws Exception
  {
    cf.create()
      .creatingParentsIfNeeded()
      .forPath(JOINER.join(TASKS_PATH, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  final byte[] bytes = cf.getData().forPath(JOINER.join(STATUS_PATH, task.getId()));
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
    Assert.assertEquals(TaskState.SUCCESS, announcements.get(0).getTaskStatus().getStatusCode());
    Assert.assertEquals(DUMMY_NODE.getHost(), announcements.get(0).getTaskLocation().getHost());
    Assert.assertEquals(DUMMY_NODE.getPlaintextPort(), announcements.get(0).getTaskLocation().getPort());
  }

  @Test(timeout = 60_000L)
  public void testRestartCleansOldStatus() throws Exception
  {
    task = TestTasks.unending("test");

    cf.create()
      .creatingParentsIfNeeded()
      .forPath(JOINER.join(TASKS_PATH, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  return cf.checkExists().forPath(JOINER.join(STATUS_PATH, task.getId())) != null;
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
    Assert.assertEquals(TaskState.FAILED, announcements.get(0).getTaskStatus().getStatusCode());
    Assert.assertEquals(
        "Canceled as unknown task. See middleManager or indexer logs for more details.",
        announcements.get(0).getTaskStatus().getErrorMsg()
    );
  }

  @Test(timeout = 60_000L)
  public void testStatusAnnouncementsArePersistent() throws Exception
  {
    cf.create()
      .creatingParentsIfNeeded()
      .forPath(JOINER.join(TASKS_PATH, task.getId()), jsonMapper.writeValueAsBytes(task));

    Assert.assertTrue(
        TestUtils.conditionValid(
            new IndexingServiceCondition()
            {
              @Override
              public boolean isValid()
              {
                try {
                  return cf.checkExists().forPath(JOINER.join(STATUS_PATH, task.getId())) != null;
                }
                catch (Exception e) {
                  return false;
                }
              }
            }
        )
    );
    // ephemeral owner is 0 is created node is PERSISTENT
    Assert.assertEquals(0, cf.checkExists().forPath(JOINER.join(STATUS_PATH, task.getId())).getEphemeralOwner());

  }
}
