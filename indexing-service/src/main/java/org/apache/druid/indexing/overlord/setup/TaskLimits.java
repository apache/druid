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

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;

public class TaskLimits
{
  private final Map<String, Integer> taskLimits;
  private final Map<String, Double> taskRatios;

  public TaskLimits()
  {
    this(Collections.emptyMap(), Collections.emptyMap());
  }

  @JsonCreator
  public TaskLimits(
      @JsonProperty("taskLimits") @Nullable Map<String, Integer> taskLimits,
      @JsonProperty("taskRatios") @Nullable Map<String, Double> taskRatios
  )
  {
    validateLimits(taskLimits, taskRatios);
    this.taskLimits = Configs.valueOrDefault(taskLimits, Collections.emptyMap());
    this.taskRatios = Configs.valueOrDefault(taskRatios, Collections.emptyMap());
  }

  private void validateLimits(Map<String, Integer> taskLimits, Map<String, Double> taskRatios)
  {
    if (taskLimits != null && taskLimits.values().stream().anyMatch(val -> val < 0)) {
      throw new IllegalArgumentException("Task limits should be bigger than 0");
    } else if (taskRatios != null && taskRatios.values().stream().anyMatch(val -> val < 0 || val > 1)) {
      throw new IllegalArgumentException("Task ratios should be in the interval of [0, 1]");
    }
  }

  @JsonProperty
  public Map<String, Integer> getTaskLimits()
  {
    return taskLimits;
  }

  @JsonProperty
  public Map<String, Double> getTaskRatios()
  {
    return taskRatios;
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
    return Objects.equals(taskLimits, that.taskLimits) && Objects.equals(taskRatios, that.taskRatios);
  }

  @Override
  public int hashCode()
  {
    int result = taskLimits != null ? taskLimits.hashCode() : 0;
    result = 31 * result + (taskRatios != null ? taskRatios.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "TaskLimits{" +
           "taskLimits=" + taskLimits +
           ", taskRatios=" + taskRatios +
           '}';
  }
}
