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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.indexing.worker.Worker;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;


import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * A snapshot of a Worker and its current state i.e tasks assigned to that worker.
 */
@PublicApi
public class ImmutableWorkerInfo
{
  private static final Logger logger = new Logger(ImmutableWorkerInfo.class);
  private final Worker worker;
  private final int currCapacityUsed;
  private final Map<String, Integer> typeSpecificCapacityMap;
  private final ImmutableSet<String> availabilityGroups;
  private final ImmutableSet<String> runningTasks;
  private final DateTime lastCompletedTaskTime;

  @Nullable
  private final DateTime blacklistedUntil;

  @JsonCreator
  public ImmutableWorkerInfo(
      @JsonProperty("worker") Worker worker,
      @JsonProperty("currCapacityUsed") int currCapacityUsed,
      @JsonProperty("currTypeSpecificCapacityUsed") Map<String, Integer> typeSpecificCapacityMap,
      @JsonProperty("availabilityGroups") Set<String> availabilityGroups,
      @JsonProperty("runningTasks") Collection<String> runningTasks,
      @JsonProperty("lastCompletedTaskTime") DateTime lastCompletedTaskTime,
      @JsonProperty("blacklistedUntil") @Nullable DateTime blacklistedUntil
  )
  {
    this.worker = worker;
    this.currCapacityUsed = currCapacityUsed;
    this.typeSpecificCapacityMap = typeSpecificCapacityMap;
    this.availabilityGroups = ImmutableSet.copyOf(availabilityGroups);
    this.runningTasks = ImmutableSet.copyOf(runningTasks);
    this.lastCompletedTaskTime = lastCompletedTaskTime;
    this.blacklistedUntil = blacklistedUntil;
  }

  public ImmutableWorkerInfo(
      Worker worker,
      int currCapacityUsed,
      Map<String, Integer> typeSpecificCapacityMap,
      Set<String> availabilityGroups,
      Collection<String> runningTasks,
      DateTime lastCompletedTaskTime
  )
  {
    this(worker, currCapacityUsed, typeSpecificCapacityMap, availabilityGroups,
         runningTasks, lastCompletedTaskTime, null
    );
  }

  public ImmutableWorkerInfo(
      Worker worker,
      int currCapacityUsed,
      Set<String> availabilityGroups,
      Collection<String> runningTasks,
      DateTime lastCompletedTaskTime
  )
  {
    this(
        worker,
        currCapacityUsed,
        Collections.emptyMap(),
        availabilityGroups,
        runningTasks,
        lastCompletedTaskTime,
        null
    );
  }

  /**
   * Helper used by {@link ZkWorker} and {@link org.apache.druid.indexing.overlord.hrtr.WorkerHolder}.
   */
  public static ImmutableWorkerInfo fromWorkerAnnouncements(
      final Worker worker,
      final Map<String, TaskAnnouncement> announcements,
      final DateTime lastCompletedTaskTime,
      @Nullable final DateTime blacklistedUntil
  )
  {
    int currCapacityUsed = 0;
    Map<String, Integer> typeSpecificCapacityMap = new HashMap<>();
    ImmutableSet.Builder<String> taskIds = ImmutableSet.builder();
    ImmutableSet.Builder<String> availabilityGroups = ImmutableSet.builder();

    for (final Map.Entry<String, TaskAnnouncement> entry : announcements.entrySet()) {
      final TaskAnnouncement announcement = entry.getValue();

      if (announcement.getStatus().isRunnable()) {
        final String taskId = entry.getKey();
        final TaskResource taskResource = announcement.getTaskResource();
        final int requiredCapacity = taskResource.getRequiredCapacity();

        currCapacityUsed += requiredCapacity;

        typeSpecificCapacityMap.merge(announcement.getTaskType(), 1, Integer::sum);

        taskIds.add(taskId);
        availabilityGroups.add(taskResource.getAvailabilityGroup());
      }
    }

    return new ImmutableWorkerInfo(
        worker,
        currCapacityUsed,
        typeSpecificCapacityMap,
        availabilityGroups.build(),
        taskIds.build(),
        lastCompletedTaskTime,
        blacklistedUntil
    );
  }

  @JsonProperty("worker")
  public Worker getWorker()
  {
    return worker;
  }

  @JsonProperty("currCapacityUsed")
  public int getCurrCapacityUsed()
  {
    return currCapacityUsed;
  }

  @JsonProperty("currTypeSpecificCapacityUsed")
  public Map<String, Integer> getTypeSpecificCapacityMap()
  {
    return typeSpecificCapacityMap;
  }

  @JsonProperty("availabilityGroups")
  public Set<String> getAvailabilityGroups()
  {
    return availabilityGroups;
  }

  public int getAvailableCapacity()
  {
    return getWorker().getCapacity() - getCurrCapacityUsed();
  }

  @JsonProperty("runningTasks")
  public Set<String> getRunningTasks()
  {
    return runningTasks;
  }

  @JsonProperty("lastCompletedTaskTime")
  public DateTime getLastCompletedTaskTime()
  {
    return lastCompletedTaskTime;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DateTime getBlacklistedUntil()
  {
    return blacklistedUntil;
  }

  public boolean isValidVersion(String minVersion)
  {
    return worker.getVersion().compareTo(minVersion) >= 0;
  }

  public boolean canRunTask(Task task, Map<String, Number> taskLimits)
  {
    return hasSufficientWorkerCapacity(task)
           && canRunTaskBasedOnCustomLimit(task, taskLimits)
           && isAvailabilityGroupAvailable(task);
  }

  private boolean hasSufficientWorkerCapacity(Task task)
  {
    int capacityRemaining = worker.getCapacity() - getCurrCapacityUsed();
    int requiredCapacity = task.getTaskResource().getRequiredCapacity();
    boolean hasCapacity = capacityRemaining >= requiredCapacity;

    if (!hasCapacity) {
      logger.info("Insufficient worker capacity for task '%s'. Required: %s, Available: %s",
                  task.getId(), requiredCapacity, capacityRemaining
      );
    }

    return hasCapacity;
  }

  private boolean isAvailabilityGroupAvailable(Task task)
  {
    boolean isAvailable = !getAvailabilityGroups().contains(task.getTaskResource().getAvailabilityGroup());

    if (!isAvailable) {
      logger.info("Availability group %s is not available for task '%s'.",
                  task.getTaskResource().getAvailabilityGroup(), task.getId()
      );
    }

    return isAvailable;
  }

  private boolean canRunTaskBasedOnCustomLimit(Task task, Map<String, Number> limitsMap)
  {
    Number limit = limitsMap.get(task.getType());

    if (limit == null) {
      return true; // No limit specified, so task can run
    }

    int currentCapacityUsed = getTypeSpecificCapacityMap().getOrDefault(task.getType(), 0);
    int requiredCapacity = task.getTaskResource().getRequiredCapacity();

    boolean canRun;
    if (limit instanceof Double) {
      canRun = hasCapacityBasedOnRatio(limit.doubleValue(), currentCapacityUsed, requiredCapacity);
      if (!canRun) {
        logger.info("Task '%s' of type '%s' cannot run due to ratio limit. Current: %s, Required: %s, Limit: %s",
                    task.getId(), task.getType(), currentCapacityUsed, requiredCapacity, limit.doubleValue()
        );
      }
    } else {
      canRun = hasCapacityBasedOnLimit(limit.intValue(), currentCapacityUsed, requiredCapacity);
      if (!canRun) {
        logger.info("Task '%s' of type '%s' cannot run due to limit. Current: %s, Required: %s, Limit: %s",
                    task.getId(), task.getType(), currentCapacityUsed, requiredCapacity, limit.intValue()
        );
      }
    }
    return canRun;
  }

  private boolean hasCapacityBasedOnRatio(double taskSlotRatio, int currentCapacityUsed, int requiredCapacity)
  {
    int maxCapacityFromRatio = calculateTaskCapacityFromRatio(taskSlotRatio);
    return maxCapacityFromRatio - currentCapacityUsed >= requiredCapacity;
  }

  private boolean hasCapacityBasedOnLimit(int limit, int currentCapacityUsed, int requiredCapacity)
  {
    return limit - currentCapacityUsed >= requiredCapacity;
  }

  private int calculateTaskCapacityFromRatio(double taskSlotRatio)
  {
    int totalCapacity = worker.getCapacity();
    int workerParallelIndexCapacity = (int) Math.floor(taskSlotRatio * totalCapacity);
    return Math.max(1, Math.min(workerParallelIndexCapacity, totalCapacity));
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

    ImmutableWorkerInfo that = (ImmutableWorkerInfo) o;

    if (currCapacityUsed != that.currCapacityUsed) {
      return false;
    }
    if (!typeSpecificCapacityMap.equals(that.typeSpecificCapacityMap)) {
      return false;
    }
    if (!worker.equals(that.worker)) {
      return false;
    }
    if (!availabilityGroups.equals(that.availabilityGroups)) {
      return false;
    }
    if (!runningTasks.equals(that.runningTasks)) {
      return false;
    }
    if (!lastCompletedTaskTime.equals(that.lastCompletedTaskTime)) {
      return false;
    }
    return !(blacklistedUntil != null
             ? !blacklistedUntil.equals(that.blacklistedUntil)
             : that.blacklistedUntil != null);
  }

  public Map<String, Integer> incrementTypeSpecificCapacity(String type, int capacityToAdd)
  {
    Map<String, Integer> result = new HashMap<>(typeSpecificCapacityMap);
    if (result.containsKey(type)) {
      result.put(type, result.get(type) + capacityToAdd);
    } else {
      result.put(type, capacityToAdd);
    }
    return result;
  }

  @Override
  public int hashCode()
  {
    int result = worker.hashCode();
    result = 31 * result + currCapacityUsed;
    result = 31 * result + typeSpecificCapacityMap.hashCode();
    result = 31 * result + availabilityGroups.hashCode();
    result = 31 * result + runningTasks.hashCode();
    result = 31 * result + lastCompletedTaskTime.hashCode();
    result = 31 * result + (blacklistedUntil != null ? blacklistedUntil.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "ImmutableWorkerInfo{" +
           "worker=" + worker +
           ", currCapacityUsed=" + currCapacityUsed +
           ", currTypeSpecificCapacityUsed=" + typeSpecificCapacityMap +
           ", availabilityGroups=" + availabilityGroups +
           ", runningTasks=" + runningTasks +
           ", lastCompletedTaskTime=" + lastCompletedTaskTime +
           ", blacklistedUntil=" + blacklistedUntil +
           '}';
  }
}
