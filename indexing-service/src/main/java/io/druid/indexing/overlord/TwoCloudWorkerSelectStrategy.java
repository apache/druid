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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableMap;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskLabels;
import io.druid.indexing.overlord.config.WorkerTaskRunnerConfig;
import io.druid.indexing.overlord.setup.TwoCloudConfig;
import io.druid.indexing.overlord.setup.WorkerBehaviorConfig;
import io.druid.indexing.overlord.setup.WorkerSelectStrategy;

import java.util.Map;

public class TwoCloudWorkerSelectStrategy implements WorkerSelectStrategy
{
  private final TwoCloudConfig twoCloudConfig;

  @JsonCreator
  public TwoCloudWorkerSelectStrategy(
      @JsonProperty("twoCloudConfig") TwoCloudConfig twoCloudConfig
  )
  {
    this.twoCloudConfig = twoCloudConfig;
  }

  @JsonProperty
  public TwoCloudConfig getTwoCloudConfig()
  {
    return twoCloudConfig;
  }

  @Override
  public Optional<ImmutableWorkerInfo> findWorkerForTask(
      WorkerTaskRunnerConfig config, ImmutableMap<String, ImmutableWorkerInfo> zkWorkers, Task task
  )
  {
    String taskLabel = TaskLabels.getTaskLabel(task);
    String ipFilter = twoCloudConfig.getIpFilter(taskLabel);
    WorkerBehaviorConfig workerBehaviorConfig = twoCloudConfig.getWorkerBehaviorConfig(taskLabel);
    WorkerSelectStrategy workerSelectStrategy = workerBehaviorConfig.getSelectStrategy();
    return workerSelectStrategy.findWorkerForTask(config, filterWorkers(ipFilter, zkWorkers), task);
  }

  private ImmutableMap<String, ImmutableWorkerInfo> filterWorkers(
      String ipFilter,
      ImmutableMap<String, ImmutableWorkerInfo> zkWorkers
  )
  {
    ImmutableMap.Builder<String, ImmutableWorkerInfo> filtered = ImmutableMap.builder();
    for (Map.Entry<String, ImmutableWorkerInfo> e : zkWorkers.entrySet()) {
      if (e.getValue().getWorker().getIp().startsWith(ipFilter)) {
        filtered.put(e);
      }
    }
    return filtered.build();
  }
}
