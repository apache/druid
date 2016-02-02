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
import com.metamx.http.client.HttpClient;
import io.druid.client.ImmutableDruidServer;
import org.apache.curator.framework.CuratorFramework;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Provides HttpLoadQueuePeons
 */
public class HttpLoadQueueTaskMaster implements LoadQueueTaskMaster
{
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService peonExec;
  private final ExecutorService callbackExec;
  private final DruidCoordinatorConfig config;

  @Inject
  public HttpLoadQueueTaskMaster(
      HttpClient httpClient,
      ObjectMapper jsonMapper,
      ScheduledExecutorService peonExec,
      ExecutorService callbackExec,
      DruidCoordinatorConfig config
  )
  {
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
    this.peonExec = peonExec;
    this.callbackExec = callbackExec;
    this.config = config;
  }

  public LoadQueuePeon giveMePeon(ImmutableDruidServer druidServer)
  {
    String baseUrl = String.format("http://%s/druid/historical/v1", druidServer.getHost());
    return new HttpLoadQueuePeon(httpClient,baseUrl, jsonMapper, peonExec, callbackExec, config);
  }
}
