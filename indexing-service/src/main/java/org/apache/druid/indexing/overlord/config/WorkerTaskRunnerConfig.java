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
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.druid.indexing.overlord.util.TaskSlotLimitsDeserializer;

import java.util.HashMap;
import java.util.Map;

public class WorkerTaskRunnerConfig
{
  @JsonProperty
  private String minWorkerVersion = "0";

  @JsonProperty
  private double parallelIndexTaskSlotRatio = 1;

  @JsonProperty
  @JsonDeserialize(using = TaskSlotLimitsDeserializer.class)
  private Map<String, Number> taskSlotLimits = new HashMap<>();

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

  /**
   * The `taskSlotLimits` configuration is a map where each key is a task type,
   * and the corresponding value represents the limit on the number of task slots
   * that a task of that type can occupy on a worker.
   * <p>
   * The key is a `String` that specifies the task type.
   * The value can either be a Double or Integer:
   * <p>
   * 1. A `Double` in the range [0, 1], representing a ratio of the available task slots
   * that tasks of this type can occupy. For example, a value of 0.5 means that tasks
   * of this type can occupy up to 50% of the task slots on a worker.
   * A value of 0 means that tasks of this type can occupy no slots (i.e., they are effectively disabled).
   * A value of 1 means no restriction, allowing tasks of this type to occupy all available slots.
   * <p>
   * 2. An `Integer` that is greater than or equal to 0, representing an absolute limit
   * on the number of task slots that tasks of this type can occupy. For example, a value of 5
   * means that tasks of this type can occupy up to 5 task slots on a worker.
   * <p>
   * If a task type is not present in the `taskSlotLimits` map, there is no restriction
   * on the number of task slots it can occupy, meaning it can use all available slots.
   * <p>
   * Example:
   * <p>
   * taskSlotLimits = {
   * "index_parallel": 0.5,  // 'index_parallel' can occupy up to 50% of task slots
   * "query_controller": 3     // 'query_controller' can occupy up to 3 task slots
   * }
   * <p>
   * This configuration allows for granular control over the allocation of task slots
   * based on the specific needs of different task types, helping to prevent any one type
   * of task from monopolizing worker resources and reducing the risk of deadlocks.
   *
   * @return A map where the key is the task type (`String`), and the value is either a `Double` (0 to 1)
   * representing the ratio of task slots available for that type, or an `Integer` (>= 0)
   * representing the absolute limit of task slots for that type. If a task type is absent,
   * it is not limited in terms of the number of task slots it can occupy.
   */
  public Map<String, Number> getTaskSlotLimitsLimits()
  {
    return taskSlotLimits;
  }
}
