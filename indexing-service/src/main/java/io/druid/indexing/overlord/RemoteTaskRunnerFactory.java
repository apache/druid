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
import com.metamx.http.client.HttpClient;
import io.druid.curator.cache.SimplePathChildrenCacheFactory;
import io.druid.guice.annotations.Global;
import io.druid.indexing.overlord.autoscaling.NoopResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.overlord.autoscaling.ResourceManagementStrategy;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;

/**
 */
public class RemoteTaskRunnerFactory implements TaskRunnerFactory<RemoteTaskRunner>
{
  public static final String TYPE_NAME = "remote";
  private final CuratorFramework curator;
  private final RemoteTaskRunnerConfig remoteTaskRunnerConfig;
  private final IndexerZkConfig zkPaths;
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig;
  private final ResourceManagementStrategy resourceManagementStrategy;
  private final ScheduledExecutorFactory factory;

  @Inject
  public RemoteTaskRunnerFactory(
      final CuratorFramework curator,
      final RemoteTaskRunnerConfig remoteTaskRunnerConfig,
      final IndexerZkConfig zkPaths,
      final ObjectMapper jsonMapper,
      @Global final HttpClient httpClient,
      final Supplier<WorkerBehaviorConfig> workerConfigRef,
      final ScheduledExecutorFactory factory,
      final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig,
      final ResourceManagementStrategy resourceManagementStrategy
  )
  {
    this.curator = curator;
    this.remoteTaskRunnerConfig = remoteTaskRunnerConfig;
    this.zkPaths = zkPaths;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.workerConfigRef = workerConfigRef;
    this.resourceManagementSchedulerConfig = resourceManagementSchedulerConfig;
    this.resourceManagementStrategy = resourceManagementStrategy;
    this.factory = factory;
  }

  @Override
  public RemoteTaskRunner build()
  {
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
        factory.create(1, "RemoteTaskRunner-Scheduled-Cleanup--%d"),
        resourceManagementSchedulerConfig.isDoAutoscale()
        ? resourceManagementStrategy
        : new NoopResourceManagementStrategy<>()
    );
  }
}
