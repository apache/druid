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
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.RemoteTaskRunnerFactory;
import org.apache.druid.indexing.overlord.hrtr.HttpRemoteTaskRunnerFactory;

public class WorkerRunnerStrategy implements RunnerStrategy
{
  private static final String DEFAULT_WORKER_TASK_RUNNER_TYPE = HttpRemoteTaskRunnerFactory.TYPE_NAME;
  private final String workerType;

  @JsonCreator
  public WorkerRunnerStrategy(@JsonProperty("workerType") String workerType)
  {
    this.workerType = ObjectUtils.defaultIfNull(
        workerType,
        DEFAULT_WORKER_TASK_RUNNER_TYPE
    );
    Preconditions.checkArgument(
        this.workerType.equals(HttpRemoteTaskRunnerFactory.TYPE_NAME) ||
        this.workerType.equals(RemoteTaskRunnerFactory.TYPE_NAME),
        "workerType must be set to one of (%s, %s)",
        HttpRemoteTaskRunnerFactory.TYPE_NAME,
        RemoteTaskRunnerFactory.TYPE_NAME
    );
  }

  @Override
  public RunnerType getRunnerTypeForTask(Task task)
  {
    return DEFAULT_WORKER_TASK_RUNNER_TYPE.equals(workerType)
           ? RunnerType.WORKER_HTTPREMOTE_RUNNER_TYPE
           : RunnerType.WORKER_REMOTE_RUNNER_TYPE;
  }

  public String getWorkerType()
  {
    return workerType;
  }
}
