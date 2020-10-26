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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatusPlus;
import org.apache.druid.indexing.common.task.Task;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * In parallel batch indexing, data ingestion can be done in a single or multiple phases.
 * {@link ParallelIndexSupervisorTask} uses different implementations of this class to execute each phase.
 *
 * For best-effort rollup, parallel indexing is executed in a single phase and the supervisor task
 * uses {@link SinglePhaseParallelIndexTaskRunner} for it.
 *
 * For perfect rollup, parallel indexing is executed in multiple phases. The supervisor task currently uses
 * {@link PartialHashSegmentGenerateParallelIndexTaskRunner}, {@link PartialRangeSegmentGenerateParallelIndexTaskRunner},
 * and {@link PartialGenericSegmentMergeParallelIndexTaskRunner}.
 * More runners can be added in the future.
 */
public interface ParallelIndexTaskRunner<SubTaskType extends Task, SubTaskReportType extends SubTaskReport>
{
  /**
   * Returns the name of this runner.
   */
  String getName();

  /**
   * Runs the task.
   */
  TaskState run() throws Exception;

  /**
   * Stop this runner gracefully. This method is called when the task is killed.
   * See {@link org.apache.druid.indexing.overlord.SingleTaskBackgroundRunner#stop}.
   */
  void stopGracefully();

  /**
   * {@link SubTaskReport} is the report sent by {@link SubTaskType}s. The subTasks call this method to
   * send their reports after pushing generated segments to deep storage.
   */
  void collectReport(SubTaskReportType report);

  /**
   * Returns a map between subTaskId and its report.
   */
  Map<String, SubTaskReportType> getReports();

  /**
   * Returns the current {@link ParallelIndexingPhaseProgress}.
   */
  ParallelIndexingPhaseProgress getProgress();

  /**
   * Returns the IDs of current running tasks.
   */
  Set<String> getRunningTaskIds();

  /**
   * Returns all {@link SubTaskSpec}s.
   */
  List<SubTaskSpec<SubTaskType>> getSubTaskSpecs();

  /**
   * Returns running {@link SubTaskSpec}s. A {@link SubTaskSpec} is running if there is a running {@link Task} created
   * using that subTaskSpec.
   *
   * @see SubTaskSpec#newSubTask
   */
  List<SubTaskSpec<SubTaskType>> getRunningSubTaskSpecs();

  /**
   * Returns complete {@link SubTaskSpec}s. A {@link SubTaskSpec} is complete if there is a succeeded or failed
   * {@link Task} created using that subTaskSpec.
   *
   * @see SubTaskSpec#newSubTask
   */
  List<SubTaskSpec<SubTaskType>> getCompleteSubTaskSpecs();

  /**
   * Returns the {@link SubTaskSpec} of the given ID or null if it's not found.
   */
  @Nullable
  SubTaskSpec<SubTaskType> getSubTaskSpec(String subTaskSpecId);

  /**
   * Returns {@link SubTaskSpecStatus} of the given ID or null if it's not found.
   */
  @Nullable
  SubTaskSpecStatus getSubTaskState(String subTaskSpecId);

  /**
   * Returns {@link TaskHistory} of the given ID or null if it's not found.
   */
  @Nullable
  TaskHistory<SubTaskType> getCompleteSubTaskSpecAttemptHistory(String subTaskSpecId);

  class SubTaskSpecStatus
  {
    private final SinglePhaseSubTaskSpec spec;
    @Nullable
    private final TaskStatusPlus currentStatus; // null if there is no running task for the spec
    private final List<TaskStatusPlus> taskHistory; // can be empty if there is no history

    @JsonCreator
    public SubTaskSpecStatus(
        @JsonProperty("spec") SinglePhaseSubTaskSpec spec,
        @JsonProperty("currentStatus") @Nullable TaskStatusPlus currentStatus,
        @JsonProperty("taskHistory") List<TaskStatusPlus> taskHistory
    )
    {
      this.spec = spec;
      this.currentStatus = currentStatus;
      this.taskHistory = taskHistory;
    }

    @JsonProperty
    public SinglePhaseSubTaskSpec getSpec()
    {
      return spec;
    }

    @JsonProperty
    @Nullable
    public TaskStatusPlus getCurrentStatus()
    {
      return currentStatus;
    }

    @JsonProperty
    public List<TaskStatusPlus> getTaskHistory()
    {
      return taskHistory;
    }
  }
}
