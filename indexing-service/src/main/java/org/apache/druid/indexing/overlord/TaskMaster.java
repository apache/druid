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

import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.indexing.common.actions.TaskActionClient;
import org.apache.druid.indexing.common.actions.TaskActionClientFactory;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.autoscaling.ScalingStats;
import org.apache.druid.indexing.overlord.supervisor.SupervisorManager;
import org.apache.druid.server.coordinator.stats.CoordinatorRunStats;
import org.apache.druid.server.metrics.TaskCountStatsProvider;
import org.apache.druid.server.metrics.TaskSlotCountStatsProvider;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Encapsulates various Overlord classes that allow querying and updating the
 * current state of the Overlord leader.
 */
public class TaskMaster implements TaskCountStatsProvider, TaskSlotCountStatsProvider
{
  private final TaskActionClientFactory taskActionClientFactory;
  private final SupervisorManager supervisorManager;
  private volatile TaskRunner taskRunner;
  private volatile TaskQueue taskQueue;

  private final AtomicBoolean isLeader = new AtomicBoolean(false);

  @Inject
  public TaskMaster(
      TaskActionClientFactory taskActionClientFactory,
      SupervisorManager supervisorManager
  )
  {
    this.taskActionClientFactory = taskActionClientFactory;
    this.supervisorManager = supervisorManager;
  }

  public void becomeLeader(TaskRunner taskRunner, TaskQueue taskQueue)
  {
    this.taskRunner = taskRunner;
    this.taskQueue = taskQueue;
    isLeader.set(true);
  }

  public void stopBeingLeader()
  {
    isLeader.set(false);
    this.taskQueue = null;
    this.taskRunner = null;
  }

  private boolean isLeader()
  {
    return isLeader.get();
  }

  public Optional<TaskRunner> getTaskRunner()
  {
    if (isLeader()) {
      return Optional.of(taskRunner);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskQueue> getTaskQueue()
  {
    if (isLeader()) {
      return Optional.of(taskQueue);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskActionClient> getTaskActionClient(Task task)
  {
    if (isLeader()) {
      return Optional.of(taskActionClientFactory.create(task));
    } else {
      return Optional.absent();
    }
  }

  public Optional<ScalingStats> getScalingStats()
  {
    if (isLeader()) {
      return taskRunner.getScalingStats();
    } else {
      return Optional.absent();
    }
  }

  public Optional<SupervisorManager> getSupervisorManager()
  {
    if (isLeader()) {
      return Optional.of(supervisorManager);
    } else {
      return Optional.absent();
    }
  }

  @Override
  public Map<String, Long> getSuccessfulTaskCount()
  {
    Optional<TaskQueue> taskQueue = getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get().getSuccessfulTaskCount();
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Long> getFailedTaskCount()
  {
    Optional<TaskQueue> taskQueue = getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get().getFailedTaskCount();
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Long> getRunningTaskCount()
  {
    Optional<TaskQueue> taskQueue = getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get().getRunningTaskCount();
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Long> getPendingTaskCount()
  {
    Optional<TaskQueue> taskQueue = getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get().getPendingTaskCount();
    } else {
      return null;
    }
  }

  @Override
  public Map<String, Long> getWaitingTaskCount()
  {
    Optional<TaskQueue> taskQueue = getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get().getWaitingTaskCount();
    } else {
      return null;
    }
  }

  @Override
  public CoordinatorRunStats getStats()
  {
    Optional<TaskQueue> taskQueue = getTaskQueue();
    if (taskQueue.isPresent()) {
      return taskQueue.get().getQueueStats();
    } else {
      return CoordinatorRunStats.empty();
    }
  }

  @Override
  @Nullable
  public Map<String, Long> getTotalTaskSlotCount()
  {
    Optional<TaskRunner> taskRunner = getTaskRunner();
    if (taskRunner.isPresent()) {
      return taskRunner.get().getTotalTaskSlotCount();
    } else {
      return null;
    }
  }

  @Override
  @Nullable
  public Map<String, Long> getIdleTaskSlotCount()
  {
    Optional<TaskRunner> taskRunner = getTaskRunner();
    if (taskRunner.isPresent()) {
      return taskRunner.get().getIdleTaskSlotCount();
    } else {
      return null;
    }
  }

  @Override
  @Nullable
  public Map<String, Long> getUsedTaskSlotCount()
  {
    Optional<TaskRunner> taskRunner = getTaskRunner();
    if (taskRunner.isPresent()) {
      return taskRunner.get().getUsedTaskSlotCount();
    } else {
      return null;
    }
  }

  @Override
  @Nullable
  public Map<String, Long> getLazyTaskSlotCount()
  {
    Optional<TaskRunner> taskRunner = getTaskRunner();
    if (taskRunner.isPresent()) {
      return taskRunner.get().getLazyTaskSlotCount();
    } else {
      return null;
    }
  }

  @Override
  @Nullable
  public Map<String, Long> getBlacklistedTaskSlotCount()
  {
    Optional<TaskRunner> taskRunner = getTaskRunner();
    if (taskRunner.isPresent()) {
      return taskRunner.get().getBlacklistedTaskSlotCount();
    } else {
      return null;
    }
  }
}
