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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.indexing.common.config.TaskConfig;
import org.apache.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import org.apache.druid.indexing.worker.config.WorkerConfig;
import org.apache.druid.server.DruidNode;
import org.apache.druid.tasklogs.TaskLogPusher;

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

  @Inject
  public ForkingTaskRunnerFactory(
      final ForkingTaskRunnerConfig config,
      final TaskConfig taskConfig,
      final WorkerConfig workerConfig,
      final Properties props,
      final ObjectMapper jsonMapper,
      final TaskLogPusher persistentTaskLogs,
      @Self DruidNode node
  )
  {
    this.config = config;
    this.taskConfig = taskConfig;
    this.workerConfig = workerConfig;
    this.props = props;
    this.jsonMapper = jsonMapper;
    this.persistentTaskLogs = persistentTaskLogs;
    this.node = node;
  }

  @Override
  public ForkingTaskRunner build()
  {
    return new ForkingTaskRunner(config, taskConfig, workerConfig, props, persistentTaskLogs, jsonMapper, node);
  }
}
