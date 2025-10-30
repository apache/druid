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

package org.apache.druid.server.compaction;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.client.indexing.ClientCompactionTaskQuery;
import org.apache.druid.client.indexing.ClientCompactionTaskQueryTuningConfig;
import org.apache.druid.client.indexing.ClientMSQContext;
import org.apache.druid.client.indexing.TaskPayloadResponse;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.indexer.CompactionEngine;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexer.partitions.DimensionRangePartitionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.metadata.LockFilterPolicy;
import org.apache.druid.rpc.indexing.OverlordClient;
import org.apache.druid.server.coordinator.ClusterCompactionConfig;
import org.apache.druid.server.coordinator.DataSourceCompactionConfig;
import org.apache.druid.server.coordinator.duty.CoordinatorDutyUtils;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Fetches running compaction tasks from the Overlord and tracks their compaction
 * intervals and task slots.
 */
public class CompactionSlotManager
{
  /**
   * Task type for native compaction tasks.
   */
  public static final String COMPACTION_TASK_TYPE = "compact";

  private static final Logger log = new Logger(CompactionSlotManager.class);

  private final OverlordClient overlordClient;
  private final CompactionStatusTracker statusTracker;

  private final Map<String, List<Interval>> intervalsToSkipCompaction;

  private int numAvailableTaskSlots;

  public CompactionSlotManager(
      OverlordClient overlordClient,
      CompactionStatusTracker statusTracker,
      ClusterCompactionConfig clusterCompactionConfig
  )
  {
    this.overlordClient = overlordClient;
    this.statusTracker = statusTracker;
    this.numAvailableTaskSlots = getCompactionTaskCapacity(clusterCompactionConfig);
    this.intervalsToSkipCompaction = new HashMap<>();
  }

  public int getNumAvailableTaskSlots()
  {
    return numAvailableTaskSlots;
  }

  public Map<String, List<Interval>> getDatasourceIntervalsToSkipCompaction()
  {
    return intervalsToSkipCompaction;
  }

  public void reserveTaskSlots(int numSlotsToReserve)
  {
    numAvailableTaskSlots -= numSlotsToReserve;
  }

  /**
   * Reserves task slots for the given task from the overall compaction task capacity.
   */
  public void reserveTaskSlots(ClientCompactionTaskQuery compactionTaskQuery)
  {
    // Note: The default compactionRunnerType used here should match the default runner used in CompactionTask when
    // no runner is provided there.
    CompactionEngine compactionRunnerType = compactionTaskQuery.getCompactionRunner() == null
                                            ? CompactionEngine.NATIVE
                                            : compactionTaskQuery.getCompactionRunner().getType();
    if (compactionRunnerType == CompactionEngine.NATIVE) {
      numAvailableTaskSlots -=
          getMaxTaskSlotsForNativeCompactionTask(compactionTaskQuery.getTuningConfig());
    } else {
      numAvailableTaskSlots -=
          getMaxTaskSlotsForMSQCompactionTask(compactionTaskQuery.getContext());
    }
  }

  /**
   * Reserves task slots for all running compaction tasks.
   */
  public void reserveTaskSlotsForRunningCompactionTasks()
  {
    for (ClientCompactionTaskQuery task : fetchRunningCompactionTasks()) {
      reserveTaskSlots(task);
    }
  }

  /**
   * Retrieves currently running tasks of type {@link #COMPACTION_TASK_TYPE} from
   * the Overlord.
   * <p>
   * Also queries the Overlord for the status of all tasks that were submitted
   * recently but are not active anymore. The statuses are then updated in the
   * {@link CompactionStatusTracker}.
   */
  public List<ClientCompactionTaskQuery> fetchRunningCompactionTasks()
  {
    // Fetch currently running compaction tasks
    final List<TaskStatusPlus> compactionTasks = CoordinatorDutyUtils.getStatusOfActiveTasks(
        overlordClient,
        status -> status != null && COMPACTION_TASK_TYPE.equals(status.getType())
    );

    final Set<String> activeTaskIds
        = compactionTasks.stream().map(TaskStatusPlus::getId).collect(Collectors.toSet());
    trackStatusOfCompletedTasks(activeTaskIds);

    final List<ClientCompactionTaskQuery> runningCompactTasks = new ArrayList<>();
    for (TaskStatusPlus status : compactionTasks) {
      final TaskPayloadResponse response =
          FutureUtils.getUnchecked(overlordClient.taskPayload(status.getId()), true);
      if (response == null) {
        throw new ISE("Could not find payload for active compaction task[%s]", status.getId());
      } else if (!COMPACTION_TASK_TYPE.equals(response.getPayload().getType())) {
        throw new ISE(
            "Payload of active compaction task[%s] is of invalid type[%s]",
            status.getId(), response.getPayload().getType()
        );
      }

      runningCompactTasks.add((ClientCompactionTaskQuery) response.getPayload());
    }

    return runningCompactTasks;
  }

  /**
   * Cancels a currently running compaction task only if the segment granularity
   * has changed in the datasource compaction config. Otherwise, the task is
   * retained and its intervals are skipped from the current round of compaction.
   *
   * @return true if the task was canceled, false otherwise.
   */
  public boolean cancelTaskOnlyIfGranularityChanged(
      ClientCompactionTaskQuery compactionTaskQuery,
      DataSourceCompactionConfig dataSourceCompactionConfig
  )
  {
    if (dataSourceCompactionConfig == null
        || dataSourceCompactionConfig.getGranularitySpec() == null
        || compactionTaskQuery.getGranularitySpec() == null) {
      skipTaskInterval(compactionTaskQuery);
      reserveTaskSlots(compactionTaskQuery);
      return false;
    }

    Granularity configuredSegmentGranularity = dataSourceCompactionConfig.getGranularitySpec()
                                                                         .getSegmentGranularity();
    Granularity taskSegmentGranularity = compactionTaskQuery.getGranularitySpec().getSegmentGranularity();
    if (configuredSegmentGranularity == null || configuredSegmentGranularity.equals(taskSegmentGranularity)) {
      skipTaskInterval(compactionTaskQuery);
      reserveTaskSlots(compactionTaskQuery);
      return false;
    }

    log.info(
        "Cancelling task[%s] as task segmentGranularity[%s] differs from compaction config segmentGranularity[%s].",
        compactionTaskQuery.getId(), taskSegmentGranularity, configuredSegmentGranularity
    );
    overlordClient.cancelTask(compactionTaskQuery.getId());
    return true;
  }

  /**
   * Retrieves the list of intervals locked by higher priority tasks for each datasource.
   * Since compaction tasks submitted for these Intervals would have to wait anyway,
   * we skip these Intervals until the next compaction run by adding them to
   * {@link #intervalsToSkipCompaction}.
   * <p>
   */
  public void skipLockedIntervals(List<DataSourceCompactionConfig> compactionConfigs)
  {
    final List<LockFilterPolicy> lockFilterPolicies = compactionConfigs
        .stream()
        .map(config ->
                 new LockFilterPolicy(config.getDataSource(), config.getTaskPriority(), null, config.getTaskContext()))
        .collect(Collectors.toList());
    final Map<String, List<Interval>> datasourceToLockedIntervals =
        new HashMap<>(FutureUtils.getUnchecked(overlordClient.findLockedIntervals(lockFilterPolicies), true));
    log.debug(
        "Skipping the following intervals for Compaction as they are currently locked: %s",
        datasourceToLockedIntervals
    );

    // Skip all the intervals locked by higher priority tasks for each datasource
    // This must be done after the invalid compaction tasks are cancelled
    // in the loop above so that their intervals are not considered locked
    datasourceToLockedIntervals.forEach(
        (dataSource, intervals) ->
            intervalsToSkipCompaction
                .computeIfAbsent(dataSource, ds -> new ArrayList<>())
                .addAll(intervals)
    );
  }

  /**
   * Adds the compaction interval of this task to {@link #intervalsToSkipCompaction}
   */
  private void skipTaskInterval(ClientCompactionTaskQuery compactionTask)
  {
    final Interval interval = compactionTask.getIoConfig().getInputSpec().getInterval();
    intervalsToSkipCompaction.computeIfAbsent(compactionTask.getDataSource(), k -> new ArrayList<>())
                             .add(interval);
  }

  /**
   * Computes overall compaction task capacity for the cluster.
   *
   * @return A value >= 1.
   */
  private int getCompactionTaskCapacity(ClusterCompactionConfig clusterConfig)
  {
    int totalWorkerCapacity = CoordinatorDutyUtils.getTotalWorkerCapacity(overlordClient);

    int compactionTaskCapacity = Math.min(
        (int) (totalWorkerCapacity * clusterConfig.getCompactionTaskSlotRatio()),
        clusterConfig.getMaxCompactionTaskSlots()
    );

    // Always consider atleast one slot available for compaction
    return Math.max(1, compactionTaskCapacity);
  }

  /**
   * Queries the Overlord for the status of all tasks that were submitted
   * recently but are not active anymore. The statuses are then updated in the
   * {@link #statusTracker}.
   */
  private void trackStatusOfCompletedTasks(Set<String> activeTaskIds)
  {
    final Set<String> finishedTaskIds = new HashSet<>(statusTracker.getSubmittedTaskIds());
    finishedTaskIds.removeAll(activeTaskIds);

    if (finishedTaskIds.isEmpty()) {
      return;
    }

    final Map<String, TaskStatus> taskStatusMap
        = FutureUtils.getUnchecked(overlordClient.taskStatuses(finishedTaskIds), true);
    for (String taskId : finishedTaskIds) {
      // Assume unknown task to have finished successfully
      final TaskStatus taskStatus = taskStatusMap.getOrDefault(taskId, TaskStatus.success(taskId));
      if (taskStatus.isComplete()) {
        statusTracker.onTaskFinished(taskId, taskStatus);
      }
    }
  }

  /**
   * @return Maximum number of task slots used by a native compaction task at
   * any time when the task is run with the given tuning config.
   */
  public static int getMaxTaskSlotsForNativeCompactionTask(
      @Nullable ClientCompactionTaskQueryTuningConfig tuningConfig
  )
  {
    if (isParallelMode(tuningConfig)) {
      Integer maxNumConcurrentSubTasks = tuningConfig.getMaxNumConcurrentSubTasks();
      // Max number of task slots used in parallel mode = maxNumConcurrentSubTasks + 1 (supervisor task)
      return (maxNumConcurrentSubTasks == null ? 1 : maxNumConcurrentSubTasks) + 1;
    } else {
      return 1;
    }
  }

  /**
   * @return Maximum number of task slots used by an MSQ compaction task at any
   * time when the task is run with the given context.
   */
  public static int getMaxTaskSlotsForMSQCompactionTask(@Nullable Map<String, Object> context)
  {
    return context == null
           ? ClientMSQContext.DEFAULT_MAX_NUM_TASKS
           : (int) context.getOrDefault(ClientMSQContext.CTX_MAX_NUM_TASKS, ClientMSQContext.DEFAULT_MAX_NUM_TASKS);
  }


  /**
   * Returns true if the compaction task can run in the parallel mode with the given tuningConfig.
   * This method should be synchronized with ParallelIndexSupervisorTask.isParallelMode(InputSource, ParallelIndexTuningConfig).
   */
  @VisibleForTesting
  public static boolean isParallelMode(@Nullable ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    if (null == tuningConfig) {
      return false;
    }
    boolean useRangePartitions = useRangePartitions(tuningConfig);
    int minRequiredNumConcurrentSubTasks = useRangePartitions ? 1 : 2;
    return tuningConfig.getMaxNumConcurrentSubTasks() != null
           && tuningConfig.getMaxNumConcurrentSubTasks() >= minRequiredNumConcurrentSubTasks;
  }

  private static boolean useRangePartitions(ClientCompactionTaskQueryTuningConfig tuningConfig)
  {
    // dynamic partitionsSpec will be used if getPartitionsSpec() returns null
    return tuningConfig.getPartitionsSpec() instanceof DimensionRangePartitionsSpec;
  }

  public int computeSlotsRequiredForTask(
      ClientCompactionTaskQuery task,
      DataSourceCompactionConfig config
  )
  {
    if (task.getCompactionRunner().getType() == CompactionEngine.MSQ) {
      final Map<String, Object> autoCompactionContext = task.getContext();
      if (autoCompactionContext.containsKey(ClientMSQContext.CTX_MAX_NUM_TASKS)) {
        return (int) autoCompactionContext.get(ClientMSQContext.CTX_MAX_NUM_TASKS);
      } else {
        // Since MSQ needs all task slots for the calculated #tasks to be available upfront, allot all available
        // compaction slots (upto a max of MAX_TASK_SLOTS_FOR_MSQ_COMPACTION) to current compaction task to avoid
        // stalling. Setting "taskAssignment" to "auto" has the problem of not being able to determine the actual
        // count, which is required for subsequent tasks.
        final int slotsRequiredForCurrentTask = Math.min(
            // Update the slots to 2 (min required for MSQ) if only 1 slot is available.
            numAvailableTaskSlots == 1 ? 2 : numAvailableTaskSlots,
            ClientMSQContext.MAX_TASK_SLOTS_FOR_MSQ_COMPACTION_TASK
        );
        autoCompactionContext.put(ClientMSQContext.CTX_MAX_NUM_TASKS, slotsRequiredForCurrentTask);

        return slotsRequiredForCurrentTask;
      }
    } else {
      return CompactionSlotManager.getMaxTaskSlotsForNativeCompactionTask(
          config.getTuningConfig()
      );
    }
  }
}
