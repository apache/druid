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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.druid.indexing.common.task.Task;

/**
 * Implementation of {@link RunnerStrategy} that determines the runner type based on a predefined worker type.
 *
 * <p>This strategy uses the worker type to select the appropriate runner for executing tasks. It supports
 * different runner types, specifically those defined in {@link RunnerType}.
 *
 * <p>The worker type must be one of the supported {@link RunnerType}s, and if not explicitly provided,
 * it defaults to {@link RunnerType#WORKER_HTTPREMOTE_RUNNER_TYPE}.
 */
public class WorkerRunnerStrategy implements RunnerStrategy
{
  private static final String DEFAULT_WORKER_TASK_RUNNER_TYPE = RunnerType.WORKER_HTTPREMOTE_RUNNER_TYPE.getType();
  private final String workerType;

  @JsonCreator
  public WorkerRunnerStrategy(@JsonProperty("workerType") String workerType)
  {
    this.workerType = ObjectUtils.defaultIfNull(
        workerType,
        DEFAULT_WORKER_TASK_RUNNER_TYPE
    );
    Preconditions.checkArgument(
        this.workerType.equals(RunnerType.WORKER_HTTPREMOTE_RUNNER_TYPE.getType()) ||
        this.workerType.equals(RunnerType.WORKER_REMOTE_RUNNER_TYPE.getType()),
        "workerType must be set to one of (%s, %s)",
        RunnerType.WORKER_HTTPREMOTE_RUNNER_TYPE.getType(),
        RunnerType.WORKER_REMOTE_RUNNER_TYPE.getType()
    );
  }

  @Override
  public RunnerType getRunnerTypeForTask(Task task)
  {
    return RunnerType.WORKER_HTTPREMOTE_RUNNER_TYPE.getType().equals(workerType)
           ? RunnerType.WORKER_HTTPREMOTE_RUNNER_TYPE
           : RunnerType.WORKER_REMOTE_RUNNER_TYPE;
  }

  @Override
  public String getWorkerType()
  {
    return workerType;
  }
}
