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

import java.util.HashMap;
import java.util.Map;

public class WorkerTaskRunnerConfig
{
  @JsonProperty
  private String minWorkerVersion = "0";

  @JsonProperty
  private double parallelIndexTaskSlotRatio = 1;

  @JsonProperty
  private Map<String, Integer> taskSlotLimits = new HashMap<>();
  @JsonProperty
  private Map<String, Double> taskSlotRatios = new HashMap<>();

  public String getMinWorkerVersion()
  {
    return minWorkerVersion;
  }

  /**
   * The number of task slots that a parallel indexing task can take is restricted using this config as a multiplier
   * <p>
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

  /**
   * Returns a map where each key is a task type (`String`), and the value is an `Integer`
   * representing the absolute limit on the number of task slots that tasks of this type can occupy.
   * <p>
   * This absolute limit specifies the maximum number of task slots available to a specific task type.
   * <p>
   * If both an absolute limit and a ratio (from {@link #getTaskSlotRatios()}) are specified for the same task type,
   * the effective limit will be the smaller of the two.
   *
   * @return A map of task types with their corresponding absolute slot limits.
   */
  public Map<String, Integer> getTaskSlotLimits()
  {
    return taskSlotLimits;
  }

  /**
   * Returns a map where each key is a task type (`String`), and the value is a `Double` which should be in the
   * range [0, 1], representing the ratio of available task slots that tasks of this type can occupy.
   * <p>
   * This ratio defines the proportion of total task slots a task type can use, calculated as `ratio * totalSlots`.
   * <p>
   * If both a ratio and an absolute limit (from {@link #getTaskSlotLimits()}) are specified for the same task type,
   * the effective limit will be the smaller of the two.
   *
   * @return A map of task types with their corresponding slot ratios.
   */
  public Map<String, Double> getTaskSlotRatios()
  {
    return taskSlotRatios;
  }
}
