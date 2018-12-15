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
import java.util.Set;

/**
 * ParallelIndexTaskRunner is the actual task runner of {@link ParallelIndexSupervisorTask}. There is currently a single
 * implementation, i.e. {@link SinglePhaseParallelIndexTaskRunner} which supports only best-effort roll-up. We can add
 * more implementations for different distributed indexing algorithms in the future.
 */
public interface ParallelIndexTaskRunner<T extends Task>
{
  /**
   * Runs the task.
   */
  TaskState run() throws Exception;

  /**
   * {@link PushedSegmentsReport} is the report sent by {@link ParallelIndexSubTask}s. The subTasks call this method to
   * send their reports after pushing generated segments to deep storage.
   */
  void collectReport(PushedSegmentsReport report);

  /**
   * Returns the current {@link ParallelIndexingProgress}.
   */
  ParallelIndexingProgress getProgress();

  /**
   * Returns the IDs of current running tasks.
   */
  Set<String> getRunningTaskIds();

  /**
   * Returns all {@link SubTaskSpec}s.
   */
  List<SubTaskSpec<T>> getSubTaskSpecs();

  /**
   * Returns running {@link SubTaskSpec}s. A {@link SubTaskSpec} is running if there is a running {@link Task} created
   * using that subTaskSpec.
   *
   * @see SubTaskSpec#newSubTask
   */
  List<SubTaskSpec<T>> getRunningSubTaskSpecs();

  /**
   * Returns complete {@link SubTaskSpec}s. A {@link SubTaskSpec} is complete if there is a succeeded or failed
   * {@link Task} created using that subTaskSpec.
   *
   * @see SubTaskSpec#newSubTask
   */
  List<SubTaskSpec<T>> getCompleteSubTaskSpecs();

  /**
   * Returns the {@link SubTaskSpec} of the given ID or null if it's not found.
   */
  @Nullable
  SubTaskSpec<T> getSubTaskSpec(String subTaskSpecId);

  /**
   * Returns {@link SubTaskSpecStatus} of the given ID or null if it's not found.
   */
  @Nullable
  SubTaskSpecStatus getSubTaskState(String subTaskSpecId);

  /**
   * Returns {@link TaskHistory} of the given ID or null if it's not found.
   */
  @Nullable
  TaskHistory<T> getCompleteSubTaskSpecAttemptHistory(String subTaskSpecId);

  class SubTaskSpecStatus
  {
    private final ParallelIndexSubTaskSpec spec;
    @Nullable
    private final TaskStatusPlus currentStatus; // null if there is no running task for the spec
    private final List<TaskStatusPlus> taskHistory; // can be empty if there is no history

    @JsonCreator
    public SubTaskSpecStatus(
        @JsonProperty("spec") ParallelIndexSubTaskSpec spec,
        @JsonProperty("currentStatus") @Nullable TaskStatusPlus currentStatus,
        @JsonProperty("taskHistory") List<TaskStatusPlus> taskHistory
    )
    {
      this.spec = spec;
      this.currentStatus = currentStatus;
      this.taskHistory = taskHistory;
    }

    @JsonProperty
    public ParallelIndexSubTaskSpec getSpec()
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
