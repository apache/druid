/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import io.druid.curator.cache.SimplePathChildrenCacheFactory;
import io.druid.guice.annotations.Global;
import io.druid.indexing.overlord.config.RemoteTaskRunnerConfig;
import io.druid.indexing.overlord.setup.FillCapacityWorkerSelectStrategy;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerSelectStrategy;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.framework.CuratorFramework;

/**
 */
public class RemoteTaskRunnerFactory implements TaskRunnerFactory
{
  private final CuratorFramework curator;
  private final RemoteTaskRunnerConfig remoteTaskRunnerConfig;
  private final ZkPathsConfig zkPaths;
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final WorkerSelectStrategy strategy;

  @Inject
  public RemoteTaskRunnerFactory(
      final CuratorFramework curator,
      final RemoteTaskRunnerConfig remoteTaskRunnerConfig,
      final ZkPathsConfig zkPaths,
      final ObjectMapper jsonMapper,
      @Global final HttpClient httpClient,
      final Supplier<WorkerBehaviorConfig> workerBehaviourConfigSupplier
  )
  {
    this.curator = curator;
    this.remoteTaskRunnerConfig = remoteTaskRunnerConfig;
    this.zkPaths = zkPaths;
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    if (workerBehaviourConfigSupplier != null) {
      // Backwards compatibility
      final WorkerBehaviorConfig workerBehaviorConfig = workerBehaviourConfigSupplier.get();
      if (workerBehaviorConfig != null) {
        this.strategy = workerBehaviorConfig.getSelectStrategy();
      } else {
        this.strategy = new FillCapacityWorkerSelectStrategy();
      }
    } else {
      this.strategy = new FillCapacityWorkerSelectStrategy();
    }
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
            .withCompressed(remoteTaskRunnerConfig.isCompressZnodes())
            .build(),
        httpClient,
        strategy
    );
  }
}
