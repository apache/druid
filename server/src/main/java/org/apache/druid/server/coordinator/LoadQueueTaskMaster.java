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

package org.apache.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.client.ImmutableDruidServer;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.initialization.ZkPathsConfig;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides LoadQueuePeons
 */
public class LoadQueueTaskMaster
{
  private final CuratorFramework curator;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService peonExec;
  private final ExecutorService callbackExec;
  private final DruidCoordinatorConfig config;
  private final HttpClient httpClient;
  private final ZkPathsConfig zkPaths;

  public LoadQueueTaskMaster(
      CuratorFramework curator,
      ObjectMapper jsonMapper,
      ScheduledExecutorService peonExec,
      ExecutorService callbackExec,
      DruidCoordinatorConfig config,
      HttpClient httpClient,
      ZkPathsConfig zkPaths
  )
  {
    this.curator = curator;
    this.jsonMapper = jsonMapper;
    this.peonExec = peonExec;
    this.callbackExec = callbackExec;
    this.config = config;
    this.httpClient = httpClient;
    this.zkPaths = zkPaths;
  }

  public LoadQueuePeon giveMePeon(ImmutableDruidServer server)
  {
    if ("http".equalsIgnoreCase(config.getLoadQueuePeonType())) {
      return new HttpLoadQueuePeon(server.getURL(), jsonMapper, httpClient, config, peonExec, callbackExec);
    } else {
      return new CuratorLoadQueuePeon(
          curator,
          ZKPaths.makePath(zkPaths.getLoadQueuePath(), server.getName()),
          jsonMapper,
          peonExec,
          callbackExec,
          config
      );
    }
  }
}
