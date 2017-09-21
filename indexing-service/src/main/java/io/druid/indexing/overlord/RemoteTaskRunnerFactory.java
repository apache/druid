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
import io.druid.curator.cache.PathChildrenCacheFactory;
import io.druid.guice.annotations.EscalatedGlobal;
import io.druid.indexing.overlord.autoscaling.NoopProvisioningStrategy;
import io.druid.indexing.overlord.autoscaling.ProvisioningSchedulerConfig;
import io.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
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
  private final ProvisioningSchedulerConfig provisioningSchedulerConfig;
  private final ProvisioningStrategy provisioningStrategy;

  @Inject
  public RemoteTaskRunnerFactory(
      final CuratorFramework curator,
      final RemoteTaskRunnerConfig remoteTaskRunnerConfig,
      final IndexerZkConfig zkPaths,
      final ObjectMapper jsonMapper,
      @EscalatedGlobal final HttpClient httpClient,
      final Supplier<WorkerBehaviorConfig> workerConfigRef,
      final ProvisioningSchedulerConfig provisioningSchedulerConfig,
      final ProvisioningStrategy provisioningStrategy
  )
  {
    this.curator = curator;
    this.remoteTaskRunnerConfig = remoteTaskRunnerConfig;
    this.zkPaths = zkPaths;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.workerConfigRef = workerConfigRef;
    this.provisioningSchedulerConfig = provisioningSchedulerConfig;
    this.provisioningStrategy = provisioningStrategy;
  }

  @Override
  public RemoteTaskRunner build()
  {
    return new RemoteTaskRunner(
        jsonMapper,
        remoteTaskRunnerConfig,
        zkPaths,
        curator,
        new PathChildrenCacheFactory.Builder().withCompressed(true),
        httpClient,
        workerConfigRef,
        provisioningSchedulerConfig.isDoAutoscale() ? provisioningStrategy : new NoopProvisioningStrategy<>()
    );
  }
}
