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
import com.google.inject.Inject;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.server.DruidNode;
import io.druid.server.initialization.ServerConfig;
import io.druid.tasklogs.TaskLogPusher;

import java.util.Properties;

/**
 */
public class ForkingTaskRunnerFactory implements TaskRunnerFactory<ForkingTaskRunner>
{
  private final ForkingTaskRunnerConfig config;
  private final TaskConfig taskConfig;
  private final WorkerConfig workerConfig;
  private final Properties props;
  private final ObjectMapper jsonMapper;
  private final TaskLogPusher persistentTaskLogs;
  private final DruidNode node;
  private final ServerConfig serverConfig;

  @Inject
  public ForkingTaskRunnerFactory(
      final ForkingTaskRunnerConfig config,
      final TaskConfig taskConfig,
      final WorkerConfig workerConfig,
      final Properties props,
      final ObjectMapper jsonMapper,
      final TaskLogPusher persistentTaskLogs,
      @Self DruidNode node,
      ServerConfig serverConfig
  )
  {
    this.config = config;
    this.taskConfig = taskConfig;
    this.workerConfig = workerConfig;
    this.props = props;
    this.jsonMapper = jsonMapper;
    this.persistentTaskLogs = persistentTaskLogs;
    this.node = node;
    this.serverConfig = serverConfig;
  }

  @Override
  public ForkingTaskRunner build()
  {
    return new ForkingTaskRunner(config, taskConfig, workerConfig, props, persistentTaskLogs, jsonMapper, node, serverConfig);
  }
}
