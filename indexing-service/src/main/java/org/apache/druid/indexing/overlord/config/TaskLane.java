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

import java.util.Set;

/**
 * The TaskLane class represents a task lane in a task scheduling environment.
 * Task lanes are logical divisions of task execution resources, allowing for more granular control over
 * how tasks are allocated and executed.
 * Each TaskLane is associated with a set of task labels, a capacity ratio, and a capacity policy.
 * The task labels define the type of tasks that can be executed in the lane.
 * The capacity ratio specifies the proportion of the total capacity that this lane is allowed to utilize.
 * The capacity policy determines how the lane's capacity is managed.
 */
public class TaskLane
{
  private Set<String> taskLabels;
  private double capacityRatio;
  private TaskLaneCapacityPolicy policy;

  public TaskLane(Set<String> taskLabels, double capacityRatio, TaskLaneCapacityPolicy policy)
  {
    this.taskLabels = taskLabels;
    this.capacityRatio = capacityRatio;
    this.policy = policy;
  }

  public Set<String> getTaskLabels()
  {
    return taskLabels;
  }

  public double getCapacityRatio()
  {
    return capacityRatio;
  }

  public TaskLaneCapacityPolicy getPolicy()
  {
    return policy;
  }
}
