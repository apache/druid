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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents the configuration for a task lane within Druid's task management system.
 * Task lanes are a mechanism for categorizing tasks into different lanes based on their
 * labels, allowing for more nuanced control over task execution priorities and resource
 * allocation. Each task lane configuration defines a set of task labels, a capacity ratio,
 * and a policy that governs how tasks within the lane should be managed.
 *
 * The {@code labels} field is a comma-separated list of task labels that identify the tasks
 * which belong to this lane. Task labels are used to group tasks with similar characteristics
 * or requirements, enabling targeted scheduling and execution strategies.
 *
 * The {@code capacityRatio} field specifies the fraction of the total available task execution
 * capacity that should be allocated to this lane. This allows administrators to reserve or
 * limit resources for specific types of tasks, ensuring that critical tasks have sufficient
 * resources while preventing any single task type from monopolizing the execution environment.
 *
 * The {@code policy} field defines how the specified capacity ratio should be applied. It supports
 * values such as "MAX", indicating that tasks in this lane can use up to the specified capacity
 * ratio of resources, and "RESERVE", designating that a certain portion of resources is reserved
 * exclusively for tasks in this lane.
 *
 * Example configuration:
 * <pre>
 *   {"labels":"kill,compact","capacityRatio":"0.1","policy":"MAX"}
 *   {"labels":"msq_export_durable_storage","capacityRatio":"0.2","policy":"MAX"}
 *   {"labels":"index_parallel,msq_index_batch","capacityRatio":"0.6","policy":"RESERVE"}
 * </pre>
 *
 * This configuration demonstrates how different task lanes can be established for various task
 * types, such as administrative tasks (kill, compact), data export tasks (msq_export_durable_storage),
 * and ingestion tasks (index_parallel, msq_index_batch), each with its own resource allocation
 * and policy settings.
 */
public class TaskLaneConfig
{
  @JsonProperty
  private final String labels;
  @JsonProperty
  private final double capacityRatio;
  @JsonProperty
  private final TaskLaneCapacityPolicy policy;

  @JsonCreator
  public TaskLaneConfig(
      @JsonProperty("labels") String labels,
      @JsonProperty("capacityRatio") double capacityRatio,
      @JsonProperty("policy") TaskLaneCapacityPolicy policy
  )
  {
    this.labels = labels;
    this.capacityRatio = capacityRatio;
    this.policy = policy;
  }

  @JsonProperty
  public String getLabels()
  {
    return labels;
  }

  @JsonProperty
  public double getCapacityRatio()
  {
    return capacityRatio;
  }

  @JsonProperty
  public TaskLaneCapacityPolicy getPolicy()
  {
    return policy;
  }

  public Set<String> getLabelSet()
  {
    return Arrays.stream(getLabels().split(",\\s*"))
                 .map(String::trim)
                 .filter(label -> !label.isEmpty())
                 .collect(Collectors.toSet());
  }

  @Override
  public String toString()
  {
    return "TaskLaneConfig{" +
           "labels=" + labels +
           ", capacityRatio=" + capacityRatio +
           ", policy=" + policy +
           '}';
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
    TaskLaneConfig that = (TaskLaneConfig) o;
    return Double.compare(capacityRatio, that.capacityRatio) == 0 && Objects.equals(
        labels,
        that.labels
    ) && Objects.equals(policy, that.policy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(labels, capacityRatio, policy);
  }
}
