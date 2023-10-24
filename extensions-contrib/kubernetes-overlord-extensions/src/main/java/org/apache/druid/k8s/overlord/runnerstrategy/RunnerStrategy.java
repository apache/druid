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
import org.apache.druid.k8s.overlord.KubernetesTaskRunnerFactory;

/**
 * Strategy interface for selecting the appropriate runner type based on the task spec or specific context conditions.
 *
 * <p>This interface is part of a strategy pattern and is implemented by different classes that handle
 * the logic of selecting a runner type based on various criteria. Each task submitted to the system
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
  String WORKER_NAME = "worker";

  /**
   * Enumerates the available runner types, each associated with a specific method of task execution.
   * These runner types are used by the strategies to make decisions and by the system to route tasks appropriately.
   */
  enum RunnerType
  {
    KUBERNETES_RUNNER_TYPE(KubernetesTaskRunnerFactory.TYPE_NAME),
    WORKER_RUNNER_TYPE(WORKER_NAME);

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
}
