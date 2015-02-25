/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.worker;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.curator.PotentiallyGzippedCompressionProvider;
import io.druid.indexing.common.IndexingServiceCondition;
import io.druid.indexing.common.SegmentLoaderFactory;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolboxFactory;
import io.druid.indexing.common.TestMergeTask;
import io.druid.indexing.common.TestRealtimeTask;
import io.druid.indexing.common.TestUtils;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.TestRemoteTaskRunnerConfig;
import io.druid.indexing.overlord.ThreadPoolTaskRunner;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.LocalDataSegmentPuller;
import io.druid.segment.loading.OmniSegmentLoader;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.StorageLocationConfig;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.server.initialization.ZkPathsConfig;
import junit.framework.Assert;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingCluster;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 */
public class WorkerTaskMonitorTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  private static final Joiner joiner = Joiner.on("/");
  private static final String basePath = "/test/druid";
  private static final String tasksPath = String.format("%s/indexer/tasks/worker", basePath);
  private static final String statusPath = String.format("%s/indexer/status/worker", basePath);

  private TestingCluster testingCluster;
  private CuratorFramework cf;
  private WorkerCuratorCoordinator workerCuratorCoordinator;
  private WorkerTaskMonitor workerTaskMonitor;

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

    worker = new Worker(
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
        },null,null,null,null,null),
        new TestRemoteTaskRunnerConfig(),
        cf,
        worker
    );
    workerCuratorCoordinator.start();

    final File tmp = Files.createTempDir();

    // Start a task monitor
    workerTaskMonitor = new WorkerTaskMonitor(
        jsonMapper,
        cf,
        workerCuratorCoordinator,
        new ThreadPoolTaskRunner(
            new TaskToolboxFactory(
                new TaskConfig(tmp.toString(), null, null, 0, null),
                null, null, null, null, null, null, null, null, null, null, null, new SegmentLoaderFactory(
                new OmniSegmentLoader(
                    ImmutableMap.<String, DataSegmentPuller>of(
                        "local",
                        new LocalDataSegmentPuller()
                    ),
                    null,
                    new SegmentLoaderConfig()
                    {
                      @Override
                      public List<StorageLocationConfig> getLocations()
                      {
                        return Lists.newArrayList();
                      }
                    }
                )
            ), jsonMapper
            ),
            null
        ),
        new WorkerConfig().setCapacity(1)
    );
    jsonMapper.registerSubtypes(new NamedType(TestMergeTask.class, "test"));
    jsonMapper.registerSubtypes(new NamedType(TestRealtimeTask.class, "test_realtime"));
    workerTaskMonitor.start();

    task = TestMergeTask.createDummyTask("test");
  }

  @After
  public void tearDown() throws Exception
  {
    workerTaskMonitor.stop();
    cf.close();
    testingCluster.stop();
  }

  @Test
  public void testRunTask() throws Exception
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
                  return cf.checkExists().forPath(joiner.join(tasksPath, task.getId())) == null;
                }
                catch (Exception e) {
                  return false;
                }
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

    TaskAnnouncement taskAnnouncement = jsonMapper.readValue(
        cf.getData().forPath(joiner.join(statusPath, task.getId())), TaskAnnouncement.class
    );

    Assert.assertEquals(task.getId(), taskAnnouncement.getTaskStatus().getId());
    Assert.assertEquals(TaskStatus.Status.RUNNING, taskAnnouncement.getTaskStatus().getStatusCode());
  }
}
