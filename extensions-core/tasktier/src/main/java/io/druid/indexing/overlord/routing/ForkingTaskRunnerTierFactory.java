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

package io.druid.indexing.overlord.routing;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.guice.annotations.Self;
import io.druid.indexing.common.config.TaskConfig;
import io.druid.indexing.overlord.ForkingTaskRunnerFactory;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.config.ForkingTaskRunnerConfig;
import io.druid.indexing.worker.config.WorkerConfig;
import io.druid.server.DruidNode;
import io.druid.tasklogs.TaskLogPusher;

import java.util.Properties;

public class ForkingTaskRunnerTierFactory implements TierTaskRunnerFactory
{
  private final ForkingTaskRunnerConfig forkingTaskRunnerConfig;
  private final TaskConfig taskConfig;
  private final WorkerConfig workerConfig;
  private final Properties properties;
  private final ObjectMapper jsonMapper;
  private final TaskLogPusher persistentTaskLogs;
  private final DruidNode node;

  @JsonCreator
  public ForkingTaskRunnerTierFactory(
      @JsonProperty
      final ForkingTaskRunnerConfig forkingTaskRunnerConfig,
      @JsonProperty
      final TaskConfig taskConfig,
      @JsonProperty
      final WorkerConfig workerConfig,
      @JsonProperty
      final Properties properties,
      @JacksonInject
      final ObjectMapper jsonMapper,
      @JacksonInject
      final TaskLogPusher persistentTaskLogs,
      @JacksonInject
      @Self DruidNode node
  )
  {
    this.forkingTaskRunnerConfig = forkingTaskRunnerConfig;
    this.taskConfig = taskConfig;
    this.workerConfig = workerConfig;
    this.properties = properties;
    this.jsonMapper = jsonMapper;
    this.persistentTaskLogs = persistentTaskLogs;
    this.node = node;
  }

  @Override
  public TaskRunner build()
  {
    return new ForkingTaskRunnerFactory(
        forkingTaskRunnerConfig,
        taskConfig,
        workerConfig,
        properties,
        jsonMapper,
        persistentTaskLogs,
        node
    ).build();
  }

  @JsonProperty
  public ForkingTaskRunnerConfig getForkingTaskRunnerConfig()
  {
    return forkingTaskRunnerConfig;
  }

  @JsonProperty
  public TaskConfig getTaskConfig()
  {
    return taskConfig;
  }

  @JsonProperty
  public WorkerConfig getWorkerConfig()
  {
    return workerConfig;
  }

  @JsonProperty
  public Properties getProperties()
  {
    return properties;
  }
}
