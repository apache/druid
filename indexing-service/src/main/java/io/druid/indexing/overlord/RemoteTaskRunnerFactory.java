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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.common.concurrent.ScheduledExecutorFactory;
import com.metamx.http.client.HttpClient;
import io.druid.curator.cache.SimplePathChildrenCacheFactory;
import io.druid.guice.annotations.Global;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.server.initialization.IndexerZkConfig;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ScheduledExecutorService;

/**
 */
public class RemoteTaskRunnerFactory implements TaskRunnerFactory
{
  private final CuratorFramework curator;
  private final RemoteTaskRunnerConfig remoteTaskRunnerConfig;
  private final IndexerZkConfig zkPaths;
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final Supplier<WorkerBehaviorConfig> workerConfigRef;
  private final ScheduledExecutorService cleanupExec;

  @Inject
  public RemoteTaskRunnerFactory(
      final CuratorFramework curator,
      final RemoteTaskRunnerConfig remoteTaskRunnerConfig,
      final IndexerZkConfig zkPaths,
      final ObjectMapper jsonMapper,
      @Global final HttpClient httpClient,
      final Supplier<WorkerBehaviorConfig> workerConfigRef,
      ScheduledExecutorFactory factory
  )
  {
    this.curator = curator;
    this.remoteTaskRunnerConfig = remoteTaskRunnerConfig;
    this.zkPaths = zkPaths;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.workerConfigRef = workerConfigRef;
    this.cleanupExec = factory.create(1,"RemoteTaskRunner-Scheduled-Cleanup--%d");
  }

  @Override
  public TaskRunner build()
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
        cleanupExec
    );
  }
}
