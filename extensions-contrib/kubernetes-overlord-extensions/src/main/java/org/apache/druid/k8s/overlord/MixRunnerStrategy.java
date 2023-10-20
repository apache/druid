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

package org.apache.druid.k8s.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.task.Task;

import javax.annotation.Nullable;
import java.util.Map;

public class MixRunnerStrategy implements RunnerStrategy
{
  @Nullable
  private final Map<String, String> overrides;
  private final RunnerStrategy kubernetesRunnerStrategy = new KubernetesRunnerStrategy();
  private WorkerRunnerStrategy workerRunnerStrategy = null;
  private final RunnerStrategy defaultRunnerStrategy;
  private final String defaultRunner;

  @JsonCreator
  public MixRunnerStrategy(
      @JsonProperty("default") String defaultRunner,
      @JsonProperty("workerType") String workerType,
      @JsonProperty("overrides") @Nullable Map<String, String> overrides
  )
  {
    Preconditions.checkNotNull(defaultRunner);
    workerRunnerStrategy = new WorkerRunnerStrategy(workerType);
    defaultRunnerStrategy = RunnerType.KUBERNETES_RUNNER_TYPE.getType().equals(defaultRunner) ?
                            kubernetesRunnerStrategy : workerRunnerStrategy;

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
    return runnerStrategy == null ? null : runnerStrategy.getRunnerTypeForTask(task);
  }

  public String getWorkerType()
  {
    return workerRunnerStrategy == null ? null : workerRunnerStrategy.getWorkerType();
  }

  private RunnerStrategy getRunnerSelectStrategy(String runnerType)
  {
    if (runnerType == null) {
      return defaultRunnerStrategy;
    }

    if (RunnerType.KUBERNETES_RUNNER_TYPE.getType().equals(runnerType)) {
      return kubernetesRunnerStrategy;
    } else if ("worker".equals(runnerType)) {
      return workerRunnerStrategy;
    } else {
      // means wrong configuration
      return null;
    }
  }

  @Override
  public String toString()
  {
    return "MixRunnerSelectStrategy{" +
           "default=" + defaultRunner +
           ", overrides=" + overrides +
           '}';
  }
}
