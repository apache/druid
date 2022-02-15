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

package org.apache.druid.indexing.overlord.config;

import com.fasterxml.jackson.annotation.JsonProperty;

public class WorkerTaskRunnerConfig
{
  @JsonProperty
  private String minWorkerVersion = "0";

  @JsonProperty
  private double parallelIndexTaskSlotRatio = 1;

  public String getMinWorkerVersion()
  {
    return minWorkerVersion;
  }

  /**
   * The number of task slots that a parallel indexing task can take is restricted using this config as a multiplier
   *
   * A value of 1 means no restriction on the number of slots ParallelIndexSupervisorTasks can occupy (default behaviour)
   * A value of 0 means ParallelIndexSupervisorTasks can occupy no slots.
   * Deadlocks can occur if the all task slots are occupied by ParallelIndexSupervisorTasks,
   * as no subtask would ever get a slot. Set this config to a value < 1 to prevent deadlocks.
   *
   * @return ratio of task slots available to a parallel indexing task at a worker level
   */
  public double getParallelIndexTaskSlotRatio()
  {
    return parallelIndexTaskSlotRatio;
  }
}
