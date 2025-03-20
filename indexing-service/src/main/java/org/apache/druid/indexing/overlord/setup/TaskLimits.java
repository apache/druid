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

package org.apache.druid.indexing.overlord.setup;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.common.config.Configs;
import org.apache.druid.error.InvalidInput;
import org.apache.druid.indexing.common.task.Task;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * Defines global limits for task execution using absolute slot counts and proportional ratios.
 *
 * <p>Task count limits ({@code maxSlotCountByType}) define the maximum number of slots per task type.
 * Task ratios ({@code maxSlotRatioByType}) define the proportion of total slots a task type can use.
 * If both are set for a task type, the lower limit applies.</p>
 *
 * <p>Example:
 * {@code maxSlotCountByType = {"index_parallel": 3, "query_controller": 5}}
 * {@code maxSlotRatioByType = {"index_parallel": 0.5, "query_controller": 0.25}}</p>
 */
public class TaskLimits
{
  public static final TaskLimits EMPTY = new TaskLimits();
  private final Map<String, Integer> maxSlotCountByType;
  private final Map<String, Double> maxSlotRatioByType;

  private TaskLimits()
  {
    this(Map.of(), Map.of());
  }

  @JsonCreator
  public TaskLimits(
      @JsonProperty("maxSlotCountByType") @Nullable Map<String, Integer> maxSlotCountByType,
      @JsonProperty("maxSlotRatioByType") @Nullable Map<String, Double> maxSlotRatioByType
  )
  {
    validateLimits(maxSlotCountByType, maxSlotRatioByType);
    this.maxSlotCountByType = Configs.valueOrDefault(maxSlotCountByType, Collections.emptyMap());
    this.maxSlotRatioByType = Configs.valueOrDefault(maxSlotRatioByType, Collections.emptyMap());
  }

  /**
   * Determines whether the given task can be executed based on task limits and available capacity.
   *
   * @param task             The task to check.
   * @param currentSlotsUsed The current capacity used by tasks of the same type.
   * @param totalCapacity    The total available capacity across all workers.
   * @return {@code true} if the task meets the defined limits and capacity constraints; {@code false} otherwise.
   */
  public boolean canRunTask(Task task, Integer currentSlotsUsed, Integer totalCapacity)
  {
    if (maxSlotRatioByType.isEmpty() && maxSlotCountByType.isEmpty()) {
      return true;
    }
    return meetsTaskLimit(
        task,
        currentSlotsUsed,
        totalCapacity
    );
  }

  private boolean meetsTaskLimit(
      Task task,
      Integer currentSlotsUsed,
      Integer totalCapacity
  )
  {
    final Integer limit = getLimitForTask(task.getType(), totalCapacity);

    if (limit == null) {
      return true; // No limit specified, so task can run
    }

    int requiredCapacity = task.getTaskResource().getRequiredCapacity();

    return hasCapacityBasedOnLimit(limit, currentSlotsUsed, requiredCapacity);
  }

  private Integer getLimitForTask(
      String taskType,
      Integer totalCapacity
  )
  {
    Integer absoluteLimit = maxSlotCountByType.get(taskType);
    Double ratioLimit = maxSlotRatioByType.get(taskType);

    if (absoluteLimit == null && ratioLimit == null) {
      return null;
    }

    if (ratioLimit != null) {
      int ratioBasedLimit = calculateTaskCapacityFromRatio(ratioLimit, totalCapacity);
      if (absoluteLimit != null) {
        return Math.min(absoluteLimit, ratioBasedLimit);
      } else {
        return ratioBasedLimit;
      }
    } else {
      return absoluteLimit;
    }
  }

  private boolean hasCapacityBasedOnLimit(int limit, int currentCapacityUsed, int requiredCapacity)
  {
    return limit - currentCapacityUsed >= requiredCapacity;
  }

  private int calculateTaskCapacityFromRatio(double taskSlotRatio, int totalCapacity)
  {
    int workerParallelIndexCapacity = (int) Math.floor(taskSlotRatio * totalCapacity);
    return Math.max(1, Math.min(workerParallelIndexCapacity, totalCapacity));
  }

  private void validateLimits(Map<String, Integer> taskCountLimits, Map<String, Double> taskRatios)
  {
    if (taskCountLimits != null && taskCountLimits.values().stream().anyMatch(val -> val < 0)) {
      throw InvalidInput.exception(
          "Max task slot count limit for any type must be greater than zero. Found[%s].",
          taskCountLimits
      );
    } else if (taskRatios != null && taskRatios.values().stream().anyMatch(val -> val < 0 || val > 1)) {
      throw InvalidInput.exception(
          "Max task slot ratios should be in the interval of [0, 1]. Found[%s].",
          taskCountLimits
      );
    }
  }

  @JsonProperty
  public Map<String, Integer> getMaxSlotCountByType()
  {
    return maxSlotCountByType;
  }

  @JsonProperty
  public Map<String, Double> getMaxSlotRatioByType()
  {
    return maxSlotRatioByType;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaskLimits that = (TaskLimits) o;
    return Objects.equals(maxSlotCountByType, that.maxSlotCountByType) && Objects.equals(
        maxSlotRatioByType,
        that.maxSlotRatioByType
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(maxSlotCountByType, maxSlotRatioByType);
  }

  @Override
  public String toString()
  {
    return "TaskLimits{" +
           "maxSlotCountByType=" + maxSlotCountByType +
           ", maxSlotRatioByType=" + maxSlotRatioByType +
           '}';
  }
}
