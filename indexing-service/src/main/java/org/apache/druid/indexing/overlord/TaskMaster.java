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
import java.util.concurrent.atomic.AtomicReference;

/**
 * Encapsulates various Overlord classes that allow querying and updating the
 * current state of the Overlord leader.
 */
public class TaskMaster implements TaskCountStatsProvider, TaskSlotCountStatsProvider
{
  enum LeadershipState
  {
    NOT_LEADER,

    /**
     * Leader of essential services only: task queue, task action client, and task runner. We enter this state after
     * the queue and runner are initialized, but before the supervisor manager is not yet initialized.
     */
    HALF_LEADER,

    /**
     * Leader of all services. We enter this state after the supervisor manager is initialized.
     */
    FULL_LEADER
  }

  private final TaskActionClientFactory taskActionClientFactory;
  private final SupervisorManager supervisorManager;
  private volatile TaskRunner taskRunner;
  private volatile TaskQueue taskQueue;

  private final AtomicReference<LeadershipState> leadershipState = new AtomicReference<>(LeadershipState.NOT_LEADER);

  @Inject
  public TaskMaster(
      TaskActionClientFactory taskActionClientFactory,
      SupervisorManager supervisorManager
  )
  {
    this.taskActionClientFactory = taskActionClientFactory;
    this.supervisorManager = supervisorManager;
  }

  /**
   * Enter state {@link LeadershipState#HALF_LEADER}, from any state.
   */
  public void becomeHalfLeader(TaskRunner taskRunner, TaskQueue taskQueue)
  {
    this.taskRunner = taskRunner;
    this.taskQueue = taskQueue;
    leadershipState.set(LeadershipState.HALF_LEADER);
  }

  /**
   * Enter state {@link LeadershipState#HALF_LEADER}, from {@link LeadershipState#FULL_LEADER}.
   */
  public void downgradeToHalfLeader()
  {
    leadershipState.compareAndSet(LeadershipState.FULL_LEADER, LeadershipState.HALF_LEADER);
  }

  /**
   * Enter state {@link LeadershipState#FULL_LEADER}.
   */
  public void becomeFullLeader()
  {
    leadershipState.set(LeadershipState.FULL_LEADER);
  }

  /**
   * Enter state {@link LeadershipState#NOT_LEADER}.
   */
  public void stopBeingLeader()
  {
    leadershipState.set(LeadershipState.NOT_LEADER);
    this.taskQueue = null;
    this.taskRunner = null;
  }

  public Optional<TaskRunner> getTaskRunner()
  {
    if (isHalfOrFullLeader()) {
      return Optional.of(taskRunner);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskQueue> getTaskQueue()
  {
    if (isHalfOrFullLeader()) {
      return Optional.of(taskQueue);
    } else {
      return Optional.absent();
    }
  }

  public Optional<TaskActionClient> getTaskActionClient(Task task)
  {
    if (isHalfOrFullLeader()) {
      return Optional.of(taskActionClientFactory.create(task));
    } else {
      return Optional.absent();
    }
  }

  public Optional<ScalingStats> getScalingStats()
  {
    if (isHalfOrFullLeader()) {
      return taskRunner.getScalingStats();
    } else {
      return Optional.absent();
    }
  }

  public Optional<SupervisorManager> getSupervisorManager()
  {
    if (isFullLeader()) {
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

  public boolean isHalfOrFullLeader()
  {
    final LeadershipState state = leadershipState.get();
    return state == LeadershipState.HALF_LEADER || state == LeadershipState.FULL_LEADER;
  }

  public boolean isFullLeader()
  {
    return leadershipState.get() == LeadershipState.FULL_LEADER;
  }
}
