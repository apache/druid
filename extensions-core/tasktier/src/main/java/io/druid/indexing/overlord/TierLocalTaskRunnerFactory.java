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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import io.druid.curator.discovery.ServiceAnnouncer;
import io.druid.guice.annotations.Global;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.config.TierLocalTaskRunnerConfig;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogPusher;

import java.util.Properties;

public class TierLocalTaskRunnerFactory implements TaskRunnerFactory
{
  final TierLocalTaskRunnerConfig config;
  final TaskConfig taskConfig;
  final WorkerConfig workerConfig;
  final Properties props;
  final TaskLogPusher taskLogPusher;
  final ObjectMapper jsonMapper;
  final
  @Self
  DruidNode node;
  final
  @Global
  HttpClient httpClient;
  final ServiceAnnouncer serviceAnnouncer;

  @Inject
  public TierLocalTaskRunnerFactory(
      final TierLocalTaskRunnerConfig config,
      final TaskConfig taskConfig,
      final WorkerConfig workerConfig,
      final Properties props,
      final TaskLogPusher taskLogPusher,
      final ObjectMapper jsonMapper,
      final @Self DruidNode node,
      final @Global HttpClient httpClient,
      final ServiceAnnouncer serviceAnnouncer
  )
  {
    this.config = config;
    this.taskConfig = taskConfig;
    this.workerConfig = workerConfig;
    this.props = props;
    this.taskLogPusher = taskLogPusher;
    this.jsonMapper = jsonMapper;
    this.node = node;
    this.httpClient = httpClient;
    this.serviceAnnouncer = serviceAnnouncer;
  }

  @Override
  public TaskRunner build()
  {
    return new TierLocalTaskRunner(
        config,
        taskConfig,
        workerConfig,
        props,
        taskLogPusher,
        jsonMapper,
        node,
        httpClient,
        serviceAnnouncer
    );
  }
}
