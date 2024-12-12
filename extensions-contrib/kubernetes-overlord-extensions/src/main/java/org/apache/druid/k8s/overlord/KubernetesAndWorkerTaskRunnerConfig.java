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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;

import javax.annotation.Nullable;

public class KubernetesAndWorkerTaskRunnerConfig
{

  private final String RUNNER_STRATEGY_TYPE = "runnerStrategy.type";
  private final String RUNNER_STRATEGY_WORKER_TYPE = "runnerStrategy.workerType";
  /**
   * Select which runner type a task would run on, options are k8s or worker.
   */
  @JsonProperty(RUNNER_STRATEGY_TYPE)
  private String runnerStrategy;

  @JsonProperty(RUNNER_STRATEGY_WORKER_TYPE)
  private String workerType;

  @JsonCreator
  public KubernetesAndWorkerTaskRunnerConfig(
      @JsonProperty(RUNNER_STRATEGY_TYPE) @Nullable String runnerStrategy,
      @JsonProperty(RUNNER_STRATEGY_WORKER_TYPE) @Nullable String workerType
  )
  {
    this.runnerStrategy = ObjectUtils.defaultIfNull(runnerStrategy, KubernetesTaskRunnerFactory.TYPE_NAME);
    this.workerType = ObjectUtils.defaultIfNull(workerType, HttpRemoteTaskRunnerFactory.TYPE_NAME);
    Preconditions.checkArgument(
        this.workerType.equals(HttpRemoteTaskRunnerFactory.TYPE_NAME) ||
        this.workerType.equals(RemoteTaskRunnerFactory.TYPE_NAME),
        "workerType must be set to one of (%s, %s)",
        HttpRemoteTaskRunnerFactory.TYPE_NAME,
        RemoteTaskRunnerFactory.TYPE_NAME
    );
  }

  @JsonProperty(RUNNER_STRATEGY_TYPE)
  public String getRunnerStrategy()
  {
    return runnerStrategy;
  }

  @JsonProperty(RUNNER_STRATEGY_WORKER_TYPE)
  public String getWorkerType()
  {
    return workerType;
  }

}
