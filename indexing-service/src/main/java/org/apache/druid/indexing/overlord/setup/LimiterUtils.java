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

import org.apache.druid.indexing.common.task.Task;

import java.util.Map;

public class LimiterUtils
{

  public static boolean canRunTask(Task task, TaskLimits taskLimits, Integer currentSlotsUsed, Integer totalCapacity)
  {
    return meetsTaskLimit(
        task,
        taskLimits.getTaskLimits(),
        taskLimits.getTaskRatios(),
        currentSlotsUsed,
        totalCapacity
    );
  }

  private static boolean meetsTaskLimit(
      Task task,
      Map<String, Integer> limitsMap,
      Map<String, Double> ratiosMap,
      Integer currentSlotsUsed,
      Integer totalCapacity
  )
  {
    final Integer limit = getLimitForTask(task.getType(), limitsMap, ratiosMap, totalCapacity);

    if (limit == null) {
      return true; // No limit specified, so task can run
    }

    int requiredCapacity = task.getTaskResource().getRequiredCapacity();

    return hasCapacityBasedOnLimit(limit, currentSlotsUsed, requiredCapacity);
  }

  private static Integer getLimitForTask(
      String taskType,
      Map<String, Integer> limitsMap,
      Map<String, Double> ratiosMap,
      Integer totalCapacity
  )
  {
    Integer absoluteLimit = limitsMap.get(taskType);
    Double ratioLimit = ratiosMap.get(taskType);

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

  private static boolean hasCapacityBasedOnLimit(int limit, int currentCapacityUsed, int requiredCapacity)
  {
    return limit - currentCapacityUsed >= requiredCapacity;
  }

  private static int calculateTaskCapacityFromRatio(double taskSlotRatio, int totalCapacity)
  {
    int workerParallelIndexCapacity = (int) Math.floor(taskSlotRatio * totalCapacity);
    return Math.max(1, Math.min(workerParallelIndexCapacity, totalCapacity));
  }
}
