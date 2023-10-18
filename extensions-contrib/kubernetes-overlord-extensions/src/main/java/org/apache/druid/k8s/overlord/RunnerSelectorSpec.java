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

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class RunnerSelectorSpec
{
  @Nullable
  private final Map<String, String> overrides;
  private final String defaultRunner;

  @JsonCreator
  public RunnerSelectorSpec(
      @JsonProperty("default") String defaultRunner,
      @JsonProperty("overrides") @Nullable Map<String, String> overrides
  )
  {
    Preconditions.checkNotNull(defaultRunner);
    Preconditions.checkArgument(
        KubernetesRunnerSelectStrategy.KUBERNETES_RUNNER_TYPE.equals(defaultRunner)
        || KubernetesRunnerSelectStrategy.WORKER_RUNNER_TYPE.equals(defaultRunner),
        "runnerSelectorSpec default must be set to one of (%s, %s)",
        KubernetesRunnerSelectStrategy.KUBERNETES_RUNNER_TYPE,
        KubernetesRunnerSelectStrategy.WORKER_RUNNER_TYPE
    );
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
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final RunnerSelectorSpec that = (RunnerSelectorSpec) o;
    return Objects.equals(defaultRunner, that.defaultRunner) &&
           Objects.equals(overrides, that.overrides);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(defaultRunner, overrides);
  }

  @Override
  public String toString()
  {
    return "RunnerSelectorSpec{" +
           "default=" + defaultRunner +
           ", overrides=" + overrides +
           '}';
  }
}
