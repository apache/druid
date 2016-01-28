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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import io.druid.curator.cache.SimplePathChildrenCacheFactory;
import io.druid.guice.annotations.Global;
import io.druid.indexing.overlord.autoscaling.NoopResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.overlord.autoscaling.ResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.SimpleResourceManagementConfig;
import io.druid.indexing.overlord.autoscaling.SimpleResourceManagementStrategy;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class RemoteTaskRunnerFactory implements TaskRunnerFactory<RemoteTaskRunner>
{
  private static final Logger LOG = new Logger(RemoteTaskRunnerFactory.class);
  private final CuratorFramework curator;
  private final RemoteTaskRunnerConfig remoteTaskRunnerConfig;
  private final IndexerZkConfig zkPaths;
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final ScheduledExecutorService cleanupExec;
  private final SimpleResourceManagementConfig config;
  private final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig;
  private final ScheduledExecutorService resourceManagementExec;

  @Inject
  public RemoteTaskRunnerFactory(
      final CuratorFramework curator,
      final RemoteTaskRunnerConfig remoteTaskRunnerConfig,
      final IndexerZkConfig zkPaths,
      final ObjectMapper jsonMapper,
      @Global final HttpClient httpClient,
      final Supplier<WorkerBehaviorConfig> workerConfigRef,
      final ScheduledExecutorFactory factory,
      final SimpleResourceManagementConfig config,
      final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig
  )
  {
    this.curator = curator;
    this.remoteTaskRunnerConfig = remoteTaskRunnerConfig;
    this.zkPaths = zkPaths;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.workerConfigRef = workerConfigRef;
    this.cleanupExec = factory.create(1, "RemoteTaskRunner-Scheduled-Cleanup--%d");
    this.config = config;
    this.resourceManagementSchedulerConfig = resourceManagementSchedulerConfig;
    this.resourceManagementExec = factory.create(1, "RemoteTaskRunner-ResourceManagement--%d");
  }

  @Override
  public RemoteTaskRunner build()
  {
    final ResourceManagementStrategy<RemoteTaskRunner> resourceManagementStrategy;
    if (resourceManagementSchedulerConfig.isDoAutoscale()) {
      resourceManagementStrategy = new SimpleResourceManagementStrategy(
          config,
          workerConfigRef,
          resourceManagementSchedulerConfig,
          resourceManagementExec
      );
    } else {
      resourceManagementStrategy = new NoopResourceManagementStrategy<>();
    }
    return new RemoteTaskRunner(
        jsonMapper,
        remoteTaskRunnerConfig,
        zkPaths,
        curator,
        new SimplePathChildrenCacheFactory
            .Builder()
            .withCompressed(true)
            .build(),
        httpClient,
        workerConfigRef,
        cleanupExec,
        resourceManagementStrategy
    );
  }
}
