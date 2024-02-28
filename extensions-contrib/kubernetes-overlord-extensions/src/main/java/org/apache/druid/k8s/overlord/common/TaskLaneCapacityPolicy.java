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

package org.apache.druid.k8s.overlord.common;

/**
 * Enumerates the policies for managing task lane capacities in a task scheduling environment.
 * Task lanes are logical divisions of task execution resources, allowing for more granular control over how tasks are allocated and executed.
 * This enumeration defines the strategies that can be applied to these lanes in terms of their capacity management.
 */
public enum TaskLaneCapacityPolicy
{
  /**
   * MAX policy indicates that the task lane should be allowed to utilize up to a maximum specified capacity.
   * This policy is suitable for tasks that can be executed concurrently without exceeding a predefined portion of the available resources.
   * It ensures that the lane does not monopolize the system's resources, maintaining a balance among different types of tasks.
   */
  MAX,

  /**
   * RESERVE policy is designed to reserve a specific portion of the system's capacity exclusively for tasks in this lane.
   * This ensures that, regardless of the system's overall load, a predetermined amount of resources is always available for these tasks.
   * It is particularly useful for critical tasks that must have guaranteed resources to prevent delays or failures due to resource contention.
   */
  RESERVE
}
