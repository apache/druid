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

import org.apache.druid.indexing.overlord.config.TaskLane;
import org.apache.druid.indexing.overlord.config.TaskLaneCapacityPolicy;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class TaskLaneRegistry
{
  private final Map<String, TaskLane> taskLabelToLaneMap;
  private final int capacity;
  private int totalReservedTaskSlots;
  private final Set<String> reservedTaskLabels;

  public TaskLaneRegistry(Map<String, TaskLane> taskLabelToLaneMap, int capacity)
  {
    this.taskLabelToLaneMap = taskLabelToLaneMap;
    this.capacity = capacity;
    totalReservedTaskSlots =
        taskLabelToLaneMap.values().stream()
                        .filter(lane -> lane.getPolicy() == TaskLaneCapacityPolicy.RESERVE)
                        .mapToInt(lane -> (int) (capacity * lane.getCapacityRatio()))
                        .sum();
    reservedTaskLabels =
        taskLabelToLaneMap.values()
                        .stream()
                        .filter(lane -> lane.getPolicy() == TaskLaneCapacityPolicy.RESERVE)
                        .flatMap(lane -> lane.getTaskLabels().stream())
                        .collect(Collectors.toSet());
  }

  public TaskLane getTaskLane(String taskLabel)
  {
    return getTaskLabelToLaneMap().get(taskLabel);
  }

  public int getAllowedTaskSlotsCount(String taskLabel)
  {
    TaskLane taskLane = getTaskLane(taskLabel);
    if (taskLane == null) {
      // return max value when no config TaskLane
      return Integer.MAX_VALUE;
    }

    return (int) (taskLane.getCapacityRatio() * capacity);
  }

  public Map<String, TaskLane> getTaskLabelToLaneMap()
  {
    return taskLabelToLaneMap;
  }

  public int getTotalReservedTaskSlots()
  {
    return totalReservedTaskSlots;
  }

  public Set<String> getReservedTaskLabels()
  {
    return reservedTaskLabels;
  }
}
