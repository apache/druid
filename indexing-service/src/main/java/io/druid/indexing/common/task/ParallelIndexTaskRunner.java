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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.indexer.TaskState;
import io.druid.indexer.TaskStatusPlus;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * ParallelIndexTaskRunner is the actual task runner of {@link ParallelIndexSupervisorTask}. There is currently a single
 * implementation, i.e. {@link SinglePhaseParallelIndexTaskRunner} which supports only best-effort roll-up. We can add
 * more implementations in the future.
 */
public interface ParallelIndexTaskRunner<T extends Task>
{
  TaskState run() throws Exception;

  void collectReport(PushedSegmentsReport report);

  ParallelIndexingStatus getStatus();

  Set<String> getRunningTaskIds();

  List<SubTaskSpec<T>> getSubTaskSpecs();

  List<SubTaskSpec<T>> getRunningSubTaskSpecs();

  List<SubTaskSpec<T>> getCompleteSubTaskSpecs();

  @Nullable
  SubTaskSpec<T> getSubTaskSpec(String subTaskSpecId);

  @Nullable
  SubTaskSpecStatus getSubTaskState(String subTaskSpecId);

  @Nullable
  TaskHistory<T> getCompleteSubTaskSpecAttemptHistory(String subTaskSpecId);

  class SubTaskSpecStatus
  {
    private final ParallelIndexSubTaskSpec spec;
    @Nullable
    private final TaskStatusPlus currentStatus;
    private final List<TaskStatusPlus> taskHistory;

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
