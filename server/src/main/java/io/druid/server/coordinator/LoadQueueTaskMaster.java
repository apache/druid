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

package io.druid.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.curator.framework.CuratorFramework;

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

  @Inject
  public LoadQueueTaskMaster(
      CuratorFramework curator,
      ObjectMapper jsonMapper,
      ScheduledExecutorService peonExec,
      ExecutorService callbackExec,
      DruidCoordinatorConfig config
  )
  {
    this.curator = curator;
    this.jsonMapper = jsonMapper;
    this.peonExec = peonExec;
    this.callbackExec = callbackExec;
    this.config = config;
  }

  public LoadQueuePeon giveMePeon(String basePath)
  {
    return new LoadQueuePeon(curator, basePath, jsonMapper, peonExec, callbackExec, config);
  }
}
