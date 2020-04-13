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

package org.apache.druid.indexing.overlord.hrtr;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.discovery.DruidNodeDiscoveryProvider;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.indexing.overlord.TaskRunnerFactory;
import org.apache.druid.indexing.overlord.TaskStorage;
import org.apache.druid.indexing.overlord.autoscaling.NoopProvisioningStrategy;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningSchedulerConfig;
import org.apache.druid.indexing.overlord.autoscaling.ProvisioningStrategy;
import org.apache.druid.indexing.overlord.config.HttpRemoteTaskRunnerConfig;
import org.apache.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.initialization.IndexerZkConfig;

/**
 */
public class HttpRemoteTaskRunnerFactory implements TaskRunnerFactory<HttpRemoteTaskRunner>
{
  public static final String TYPE_NAME = "httpRemote";

  private final ObjectMapper smileMapper;
  private final HttpRemoteTaskRunnerConfig httpRemoteTaskRunnerConfig;
  private final HttpClient httpClient;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final ProvisioningSchedulerConfig provisioningSchedulerConfig;
  private final ProvisioningStrategy provisioningStrategy;
  private final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider;
  private final TaskStorage taskStorage;

  // ZK_CLEANUP_TODO : Remove these when RemoteTaskRunner and WorkerTaskMonitor are removed.
  private final CuratorFramework cf;
  private final IndexerZkConfig indexerZkConfig;

  @Inject
  public HttpRemoteTaskRunnerFactory(
      @Smile final ObjectMapper smileMapper,
      final HttpRemoteTaskRunnerConfig httpRemoteTaskRunnerConfig,
      @EscalatedGlobal final HttpClient httpClient,
      final Supplier<WorkerBehaviorConfig> workerConfigRef,
      final ProvisioningSchedulerConfig provisioningSchedulerConfig,
      final ProvisioningStrategy provisioningStrategy,
      final DruidNodeDiscoveryProvider druidNodeDiscoveryProvider,
      final TaskStorage taskStorage,
      final CuratorFramework cf,
      final IndexerZkConfig indexerZkConfig
  )
  {
    this.smileMapper = smileMapper;
    this.httpRemoteTaskRunnerConfig = httpRemoteTaskRunnerConfig;
    this.httpClient = httpClient;
    this.workerConfigRef = workerConfigRef;
    this.provisioningSchedulerConfig = provisioningSchedulerConfig;
    this.provisioningStrategy = provisioningStrategy;
    this.druidNodeDiscoveryProvider = druidNodeDiscoveryProvider;
    this.taskStorage = taskStorage;
    this.cf = cf;
    this.indexerZkConfig = indexerZkConfig;
  }

  @Override
  public HttpRemoteTaskRunner build()
  {
    return new HttpRemoteTaskRunner(
        smileMapper,
        httpRemoteTaskRunnerConfig,
        httpClient,
        workerConfigRef,
        provisioningSchedulerConfig.isDoAutoscale() ? provisioningStrategy : new NoopProvisioningStrategy<>(),
        druidNodeDiscoveryProvider,
        taskStorage,
        cf,
        indexerZkConfig
    );
  }
}
