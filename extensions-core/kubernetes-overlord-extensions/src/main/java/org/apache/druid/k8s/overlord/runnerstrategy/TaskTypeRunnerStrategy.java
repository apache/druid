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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.task.Task;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Implementation of {@link RunnerStrategy} that allows dynamic selection of runner type based on task type.
 *
 * <p>This strategy checks each task's type against a set of overrides to determine the appropriate runner type.
 * If no override is specified for a task's type, it uses a default runner.
 *
 * <p>Runner types are determined based on configurations provided at construction, including default runner
 * type and specific overrides per task type. This strategy is designed for environments where tasks may require
 * different execution environments (e.g., Kubernetes or worker nodes).
 */
public class TaskTypeRunnerStrategy implements RunnerStrategy
{
  @Nullable
  private final Map<String, String> overrides;
  private final RunnerStrategy kubernetesRunnerStrategy = new KubernetesRunnerStrategy();
  private WorkerRunnerStrategy workerRunnerStrategy;
  private final RunnerStrategy defaultRunnerStrategy;
  private final String defaultRunner;

  @JsonCreator
  public TaskTypeRunnerStrategy(
      @JsonProperty("default") String defaultRunner,
      @JsonProperty("overrides") @Nullable Map<String, String> overrides
  )
  {
    Preconditions.checkNotNull(defaultRunner);
    workerRunnerStrategy = new WorkerRunnerStrategy();
    defaultRunnerStrategy = RunnerType.WORKER_RUNNER_TYPE.getType().equals(defaultRunner) ?
                            workerRunnerStrategy : kubernetesRunnerStrategy;
    validate(overrides);
    this.defaultRunner = defaultRunner;
    this.overrides = overrides;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<String, String> getOverrides()
  {
    return overrides;
  }

  @JsonProperty
  public String getDefault()
  {
    return defaultRunner;
  }

  @Override
  public RunnerType getRunnerTypeForTask(Task task)
  {
    String runnerType = null;
    if (overrides != null) {
      runnerType = overrides.get(task.getType());
    }

    RunnerStrategy runnerStrategy = getRunnerSelectStrategy(runnerType);
    return runnerStrategy.getRunnerTypeForTask(task);
  }

  private RunnerStrategy getRunnerSelectStrategy(String runnerType)
  {
    if (runnerType == null) {
      return defaultRunnerStrategy;
    }

    if (WORKER_NAME.equals(runnerType)) {
      return workerRunnerStrategy;
    } else {
      return kubernetesRunnerStrategy;
    }
  }

  private void validate(Map<String, String> overrides)
  {
    if (overrides == null) {
      return;
    }

    boolean hasValidRunnerType =
        overrides.values().stream().allMatch(v -> RunnerType.WORKER_RUNNER_TYPE.getType().equals(v)
                                                  || RunnerType.KUBERNETES_RUNNER_TYPE.getType().equals(v));
    Preconditions.checkArgument(
        hasValidRunnerType,
        "Invalid config in 'overrides'. Each runner type must be either '%s' or '%s'.",
        RunnerType.WORKER_RUNNER_TYPE.getType(),
        RunnerType.KUBERNETES_RUNNER_TYPE.getType()
    );
  }

  @Override
  public String toString()
  {
    return "TaskTypeRunnerStrategy{" +
           "default=" + defaultRunner +
           ", overrides=" + overrides +
           '}';
  }
}
