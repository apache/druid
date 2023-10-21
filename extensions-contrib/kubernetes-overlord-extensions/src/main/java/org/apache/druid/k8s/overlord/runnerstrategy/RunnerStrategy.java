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

package org.apache.druid.k8s.overlord.runnerstrategy;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerFactory;

/**
 * Strategy interface for selecting the appropriate runner based on the task spec or specific context conditions.
 *
 * <p>This interface is part of a strategy pattern and is implemented by different classes that handle
 * the logic of selecting a runner based on various criteria. Each task submitted to the system
 * will pass through the strategy implementation to determine the correct runner for execution.
 *
 * <p>The strategy uses {@link RunnerType} as a standardized way of referring to and managing different types of runners.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = KubernetesRunnerStrategy.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "k8s", value = KubernetesRunnerStrategy.class),
    @JsonSubTypes.Type(name = "worker", value = WorkerRunnerStrategy.class),
    @JsonSubTypes.Type(name = "taskType", value = TaskTypeRunnerStrategy.class)
})
public interface RunnerStrategy
{
  /**
   * Enumerates the available runner types, each associated with a specific method of task execution.
   * These runner types are used by the strategies to make decisions and by the system to route tasks appropriately.
   */
  enum RunnerType
  {
    KUBERNETES_RUNNER_TYPE(KubernetesTaskRunnerFactory.TYPE_NAME),
    WORKER_REMOTE_RUNNER_TYPE(RemoteTaskRunnerFactory.TYPE_NAME),
    WORKER_HTTPREMOTE_RUNNER_TYPE(HttpRemoteTaskRunnerFactory.TYPE_NAME);

    private final String type;

    RunnerType(String type)
    {
      this.type = type;
    }

    public String getType()
    {
      return type;
    }
  }

  /**
   * Analyzes the task and determines the appropriate runner type for executing it.
   *
   * @param task The task that needs to be executed.
   * @return The runner type deemed most suitable for executing the task.
   */
  RunnerType getRunnerTypeForTask(Task task);

  /**
   * Provides the worker type being used in the strategy, if applicable. This default method returns null,
   * and it should be overridden by strategies that use specific worker types.
   *
   * @return The type of the worker associated with the strategy, or null if no specific worker type is used.
   */
  default String getWorkerType()
  {
    return null;
  }
}
