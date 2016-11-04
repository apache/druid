/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.routing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.metamx.http.client.HttpClient;
import io.druid.concurrent.Execs;
import io.druid.indexing.overlord.RemoteTaskRunner;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerResourceManagementConfig;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.server.initialization.IndexerZkConfig;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.concurrent.ScheduledExecutorService;

@RunWith(EasyMockRunner.class)
public class RemoteTaskRunnerTierFactoryTest
{
  @Mock(type = MockType.STRICT)
  PendingTaskBasedWorkerResourceManagementConfig config;

  @Mock(type = MockType.STRICT)
  WorkerBehaviorConfig workerBehaviorConfig;

  @Mock(type = MockType.STRICT)
  CuratorFramework curator;

  @Mock(type = MockType.STRICT)
  HttpClient httpClient;

  @Mock(type = MockType.STRICT)
  ResourceManagementSchedulerConfig resourceManagementSchedulerConfig;

  final ScheduledExecutorService scheduledExecutorService = Execs.scheduledSingleThreaded("TestThread");
  final ScheduledExecutorFactory executorFactory = new ScheduledExecutorFactory()
  {
    @Override
    public ScheduledExecutorService create(int corePoolSize, String nameFormat)
    {
      return scheduledExecutorService;
    }
  };
  IndexerZkConfig zkPaths = new IndexerZkConfig(new ZkPathsConfig(), null, null, null, null, null);
  final ObjectMapper jsonMapper = new DefaultObjectMapper();
  final RemoteTaskRunnerConfig remoteTaskRunnerConfig = new RemoteTaskRunnerConfig();

  @Test
  public void testBuild() throws Exception
  {
    final RemoteTaskRunnerTierFactory factory = new RemoteTaskRunnerTierFactory(
        remoteTaskRunnerConfig,
        config,
        Suppliers.ofInstance(workerBehaviorConfig),
        curator,
        zkPaths,
        jsonMapper,
        httpClient,
        executorFactory,
        resourceManagementSchedulerConfig
    );

    EasyMock.expect(resourceManagementSchedulerConfig.isDoAutoscale()).andReturn(false).times(2);
    EasyMock.replay(resourceManagementSchedulerConfig);
    final TaskRunner runner = factory.build();
    Assert.assertTrue(runner instanceof RemoteTaskRunner);
    final RemoteTaskRunner rtr = (RemoteTaskRunner) runner;
    Assert.assertEquals(ImmutableList.of(), rtr.getKnownTasks());
    Assert.assertEquals(ImmutableList.of(), rtr.getLazyWorkers());
    Assert.assertEquals(ImmutableList.of(), rtr.getPendingTaskPayloads());
    Assert.assertEquals(ImmutableList.of(), rtr.getPendingTasks());
    Assert.assertEquals(ImmutableList.of(), rtr.getRunningTasks());
    Assert.assertEquals(ImmutableList.of(), rtr.getWorkers());
  }

  @Test
  public void testGetRemoteTaskRunnerConfig() throws Exception
  {
    final RemoteTaskRunnerTierFactory factory = new RemoteTaskRunnerTierFactory(
        remoteTaskRunnerConfig,
        config,
        Suppliers.ofInstance(workerBehaviorConfig),
        curator,
        zkPaths,
        jsonMapper,
        httpClient,
        executorFactory,
        resourceManagementSchedulerConfig
    );
    Assert.assertEquals(remoteTaskRunnerConfig, factory.getRemoteTaskRunnerConfig());
  }

  @Test
  public void testGetZkPaths() throws Exception
  {
    final RemoteTaskRunnerTierFactory factory = new RemoteTaskRunnerTierFactory(
        remoteTaskRunnerConfig,
        config,
        Suppliers.ofInstance(workerBehaviorConfig),
        curator,
        zkPaths,
        jsonMapper,
        httpClient,
        executorFactory,
        resourceManagementSchedulerConfig
    );
    Assert.assertEquals(zkPaths, factory.getZkPaths());
  }

  @Test
  public void testGetPendingConfig() throws Exception
  {
    final RemoteTaskRunnerTierFactory factory = new RemoteTaskRunnerTierFactory(
        remoteTaskRunnerConfig,
        config,
        Suppliers.ofInstance(workerBehaviorConfig),
        curator,
        zkPaths,
        jsonMapper,
        httpClient,
        executorFactory,
        resourceManagementSchedulerConfig
    );
    Assert.assertEquals(config, factory.getPendingTaskBasedWorkerResourceManagementConfig());
  }

  @Test
  public void testGetResourceManagementSchedulerConfig() throws Exception
  {
    final RemoteTaskRunnerTierFactory factory = new RemoteTaskRunnerTierFactory(
        remoteTaskRunnerConfig,
        config,
        Suppliers.ofInstance(workerBehaviorConfig),
        curator,
        zkPaths,
        jsonMapper,
        httpClient,
        executorFactory,
        resourceManagementSchedulerConfig
    );
    Assert.assertEquals(remoteTaskRunnerConfig, factory.getRemoteTaskRunnerConfig());
  }

  @After
  public void tearDown()
  {
    scheduledExecutorService.shutdownNow();
  }
}
