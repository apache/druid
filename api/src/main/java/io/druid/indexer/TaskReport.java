/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * TaskReport can be optionally included in io.druid.indexing.common.TaskStatus to report some ingestion results to
 * Supervisors or supervisorTasks. See ParallelIndexSinglePhaseSupervisorTask and ParallelIndexSinglePhaseSubTask
 * as an example.
 */
public class TaskReport
{
  private final String taskId;
  private final Object payload; // can't use generic to not change TaskStatus

  @JsonCreator
  public TaskReport(
      @JsonProperty("taskId") String taskId,
      @JsonProperty("payload") Object payload)
  {
    this.taskId = taskId;
    this.payload = payload;
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonProperty
  public Object getPayload()
  {
    return payload;
  }
}
