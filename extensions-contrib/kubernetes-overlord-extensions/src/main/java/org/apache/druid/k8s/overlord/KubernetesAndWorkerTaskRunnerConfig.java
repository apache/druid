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

import javax.validation.constraints.NotNull;

public class KubernetesAndWorkerTaskRunnerConfig
{
  private static final String DEFAULT_WORKER_TASK_RUNNER_TYPE = HttpRemoteTaskRunnerFactory.TYPE_NAME;
  /**
   * Select which worker task runner to use in addition to the Kubernetes runner, options are httpRemote or remote.
   */
  @JsonProperty
  @NotNull
  private final String workerTaskRunnerType;

  /**
   * Whether or not to send tasks to the worker task runner instead of the Kubernetes runner.
   */
  @JsonProperty
  @NotNull
  private final boolean sendAllTasksToWorkerTaskRunner;

  @JsonCreator
  public KubernetesAndWorkerTaskRunnerConfig(
      @JsonProperty("workerTaskRunnerType") String workerTaskRunnerType,
      @JsonProperty("sendAllTasksToWorkerTaskRunner") Boolean sendAllTasksToWorkerTaskRunner
  )
  {
    this.workerTaskRunnerType = ObjectUtils.defaultIfNull(
        workerTaskRunnerType,
        DEFAULT_WORKER_TASK_RUNNER_TYPE
    );
    Preconditions.checkArgument(
        this.workerTaskRunnerType.equals(HttpRemoteTaskRunnerFactory.TYPE_NAME) ||
        this.workerTaskRunnerType.equals(RemoteTaskRunnerFactory.TYPE_NAME),
        "workerTaskRunnerType must be set to one of (%s, %s)",
        HttpRemoteTaskRunnerFactory.TYPE_NAME,
        RemoteTaskRunnerFactory.TYPE_NAME
    );
    this.sendAllTasksToWorkerTaskRunner = ObjectUtils.defaultIfNull(
        sendAllTasksToWorkerTaskRunner,
        false
    );
  }

  public String getWorkerTaskRunnerType()
  {
    return workerTaskRunnerType;
  }

  public boolean isSendAllTasksToWorkerTaskRunner()
  {
    return sendAllTasksToWorkerTaskRunner;
  }

}
