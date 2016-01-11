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
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.http.client.HttpClient;
import io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerResourceManagementConfig;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

public class RemoteTaskRunnerTierFactoryTest
{
  final RemoteTaskRunnerConfig remoteTaskRunnerConfig = EasyMock.createStrictMock(RemoteTaskRunnerConfig.class);
  final PendingTaskBasedWorkerResourceManagementConfig config = EasyMock.createStrictMock(
      PendingTaskBasedWorkerResourceManagementConfig.class);
  final Supplier<WorkerBehaviorConfig> workerConfigRef = Suppliers.ofInstance(EasyMock.createStrictMock(
      WorkerBehaviorConfig.class));
  final CuratorFramework curator = EasyMock.createStrictMock(CuratorFramework.class);
  final IndexerZkConfig zkPaths = EasyMock.createStrictMock(IndexerZkConfig.class);
  final ObjectMapper jsonMapper = new DefaultObjectMapper();
  final HttpClient httpClient = EasyMock.createStrictMock(HttpClient.class);
  final ScheduledExecutorFactory executorFactory = EasyMock.createStrictMock(ScheduledExecutorFactory.class);
  final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig =
      EasyMock.createStrictMock(ResourceManagementSchedulerConfig.class);
  final RemoteTaskRunnerTierFactory factory = new RemoteTaskRunnerTierFactory(
      remoteTaskRunnerConfig,
      config,
      workerConfigRef,
      curator,
      zkPaths,
      jsonMapper,
      httpClient,
      executorFactory,
      resourceManagementSchedulerConfig
  );

  @Test
  public void testBuild() throws Exception
  {

  }

  @Test
  public void testGetRemoteTaskRunnerConfig() throws Exception
  {
    Assert.assertEquals(remoteTaskRunnerConfig, factory.getRemoteTaskRunnerConfig());
  }

  @Test
  public void testGetZkPaths() throws Exception
  {
    Assert.assertEquals(zkPaths, factory.getZkPaths());
  }

  @Test
  public void testGetPendingConfig() throws Exception
  {
    Assert.assertEquals(config, factory.getPendingTaskBasedWorkerResourceManagementConfig());
  }

  @Test
  public void testGetResourceManagementSchedulerConfig() throws Exception
  {
    Assert.assertEquals(remoteTaskRunnerConfig, factory.getRemoteTaskRunnerConfig());
  }
}
