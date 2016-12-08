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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.metamx.http.client.HttpClient;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Json;
import io.druid.indexing.overlord.RemoteTaskRunnerFactory;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.autoscaling.NoopResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerResourceManagementConfig;
import io.druid.indexing.overlord.autoscaling.PendingTaskBasedWorkerResourceManagementStrategy;
import io.druid.indexing.overlord.autoscaling.ResourceManagementSchedulerConfig;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.java.util.common.concurrent.ScheduledExecutorFactory;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;

public class RemoteTaskRunnerTierFactory implements TierTaskRunnerFactory
{
  final CuratorFramework curator;
  final RemoteTaskRunnerConfig remoteTaskRunnerConfig;
  final IndexerZkConfig zkPaths;
  final ObjectMapper jsonMapper;
  final HttpClient httpClient;
  final Supplier<WorkerBehaviorConfig> workerConfigRef;
  final ScheduledExecutorFactory factory;
  final PendingTaskBasedWorkerResourceManagementConfig pendingTaskBasedWorkerResourceManagementConfig;
  final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig;

  @JsonCreator
  public RemoteTaskRunnerTierFactory(
      @JsonProperty
      final RemoteTaskRunnerConfig remoteTaskRunnerConfig,
      @JsonProperty
      final PendingTaskBasedWorkerResourceManagementConfig pendingTaskBasedWorkerResourceManagementConfig,
      // This is part of why this is not compatible with the tiering methodology.
      @JacksonInject
      final Supplier<WorkerBehaviorConfig> workerConfigRef,
      @JacksonInject
      final CuratorFramework curator,
      @JacksonInject
      final IndexerZkConfig zkPaths,
      @JacksonInject
      @Json
      final ObjectMapper jsonMapper,
      @JacksonInject
      @Global final HttpClient httpClient,
      @JacksonInject
      final ScheduledExecutorFactory factory,
      @JacksonInject
      final ResourceManagementSchedulerConfig resourceManagementSchedulerConfig
  )
  {
    this.remoteTaskRunnerConfig = remoteTaskRunnerConfig;
    this.workerConfigRef = workerConfigRef;
    this.pendingTaskBasedWorkerResourceManagementConfig = pendingTaskBasedWorkerResourceManagementConfig;
    this.curator = curator;
    this.zkPaths = zkPaths;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.factory = factory;
    this.resourceManagementSchedulerConfig = resourceManagementSchedulerConfig;
  }

  @Override
  public TaskRunner build()
  {
    return new RemoteTaskRunnerFactory(
        curator,
        remoteTaskRunnerConfig,
        zkPaths,
        jsonMapper,
        httpClient,
        workerConfigRef,
        factory,
        resourceManagementSchedulerConfig,
        resourceManagementSchedulerConfig.isDoAutoscale() ? new PendingTaskBasedWorkerResourceManagementStrategy(
            pendingTaskBasedWorkerResourceManagementConfig,
            workerConfigRef,
            resourceManagementSchedulerConfig,
            factory
        ) : new NoopResourceManagementStrategy()
    ).build();
  }

  @JsonProperty
  public RemoteTaskRunnerConfig getRemoteTaskRunnerConfig()
  {
    return remoteTaskRunnerConfig;
  }

  @JsonProperty
  public IndexerZkConfig getZkPaths()
  {
    return zkPaths;
  }

  @JsonProperty
  public PendingTaskBasedWorkerResourceManagementConfig getPendingTaskBasedWorkerResourceManagementConfig()
  {
    return pendingTaskBasedWorkerResourceManagementConfig;
  }

  @JsonProperty
  public ResourceManagementSchedulerConfig getResourceManagementSchedulerConfig()
  {
    return resourceManagementSchedulerConfig;
  }
}
