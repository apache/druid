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
import org.apache.druid.indexing.common.task.Task;

/**
 * Implementation of {@link RunnerStrategy} that always selects the Worker runner type for tasks.
 *
 * <p>This strategy is specific for tasks that are intended to be executed in a Worker environment.
 * Regardless of task specifics, this strategy always returns {@link RunnerType#WORKER_RUNNER_TYPE}.
 */
public class WorkerRunnerStrategy implements RunnerStrategy
{
  @JsonCreator
  public WorkerRunnerStrategy()
  {
  }

  @Override
  public RunnerType getRunnerTypeForTask(Task task)
  {
    return RunnerType.WORKER_RUNNER_TYPE;
  }
}
