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

package org.apache.druid.server.coordinator.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.inject.Provider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.coordinator.DruidCoordinatorConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides LoadQueuePeons
 */
public class LoadQueueTaskMaster
{
  private static final Logger log = new Logger(LoadQueueTaskMaster.class);

  private final Provider<CuratorFramework> curatorFrameworkProvider;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService peonExec;
  private final ExecutorService callbackExec;
  private final DruidCoordinatorConfig config;
  private final HttpClient httpClient;
  private final ZkPathsConfig zkPaths;
  private final boolean httpLoading;

  private final ConcurrentHashMap<String, LoadQueuePeon> loadManagementPeons = new ConcurrentHashMap<>();

  public LoadQueueTaskMaster(
      Provider<CuratorFramework> curatorFrameworkProvider,
      ObjectMapper jsonMapper,
      ScheduledExecutorService peonExec,
      ExecutorService callbackExec,
      DruidCoordinatorConfig config,
      HttpClient httpClient,
      ZkPathsConfig zkPaths
  )
  {
    this.curatorFrameworkProvider = curatorFrameworkProvider;
    this.jsonMapper = jsonMapper;
    this.peonExec = peonExec;
    this.callbackExec = callbackExec;
    this.config = config;
    this.httpClient = httpClient;
    this.zkPaths = zkPaths;
    this.httpLoading = "http".equalsIgnoreCase(config.getLoadQueuePeonType());
  }

  private LoadQueuePeon createPeon(ImmutableDruidServer server)
  {
    if (httpLoading) {
      return new HttpLoadQueuePeon(server.getURL(), jsonMapper, httpClient, config, peonExec, callbackExec);
    } else {
      return new CuratorLoadQueuePeon(
          curatorFrameworkProvider.get(),
          ZKPaths.makePath(zkPaths.getLoadQueuePath(), server.getName()),
          jsonMapper,
          peonExec,
          callbackExec,
          config
      );
    }
  }

  public Map<String, LoadQueuePeon> getAllPeons()
  {
    return loadManagementPeons;
  }

  public LoadQueuePeon getPeonForServer(ImmutableDruidServer server)
  {
    return loadManagementPeons.get(server.getName());
  }

  public void startPeonsForNewServers(List<ImmutableDruidServer> currentServers)
  {
    for (ImmutableDruidServer server : currentServers) {
      loadManagementPeons.computeIfAbsent(server.getName(), serverName -> {
        LoadQueuePeon loadQueuePeon = createPeon(server);
        loadQueuePeon.start();
        log.debug("Created LoadQueuePeon for server[%s].", server.getName());
        return loadQueuePeon;
      });
    }
  }

  public void stopPeonsForDisappearedServers(List<ImmutableDruidServer> servers)
  {
    final Set<String> disappearedServers = Sets.newHashSet(loadManagementPeons.keySet());
    for (ImmutableDruidServer server : servers) {
      disappearedServers.remove(server.getName());
    }
    for (String name : disappearedServers) {
      log.debug("Removing LoadQueuePeon for disappeared server[%s].", name);
      LoadQueuePeon peon = loadManagementPeons.remove(name);
      peon.stop();
    }
  }

  public void stopAndRemoveAllPeons()
  {
    for (String server : loadManagementPeons.keySet()) {
      LoadQueuePeon peon = loadManagementPeons.remove(server);
      peon.stop();
    }
    loadManagementPeons.clear();
  }

  public boolean isHttpLoading()
  {
    return httpLoading;
  }
}
