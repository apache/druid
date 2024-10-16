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

package org.apache.druid.indexing.batch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.TaskStatus;

import java.util.Map;

public class ScheduledBatchSupervisorSnapshot
{
  @JsonProperty
  private final String supervisorId;

  @JsonProperty
  private final ScheduledBatchSupervisorPayload.BatchSupervisorStatus status;

  @JsonProperty
  private final String previousTaskExecutionTime;

  @JsonProperty
  private final String nextTaskExecutionTime;

  @JsonProperty
  private final String timeToNextExecution;

  @JsonProperty
  private final Map<String, TaskStatus> activeTasks;

  @JsonProperty
  private final Map<String, TaskStatus> completedTasks;

  @JsonCreator
  public ScheduledBatchSupervisorSnapshot(
      @JsonProperty("supervisorId") String supervisorId,
      @JsonProperty("status") ScheduledBatchSupervisorPayload.BatchSupervisorStatus status,
      @JsonProperty("previousTaskExecutionTime") String previousTaskExecutionTime,
      @JsonProperty("nextTaskExecutionTime") String nextTaskExecutionTime,
      @JsonProperty("timeToNextExecution") String timeToNextExecution,
      @JsonProperty("activeTasks") Map<String, TaskStatus> activeTasks,
      @JsonProperty("completedTasks") Map<String, TaskStatus> completedTasks
  )
  {
    this.supervisorId = supervisorId;
    this.status = status;
    this.previousTaskExecutionTime = previousTaskExecutionTime;
    this.nextTaskExecutionTime = nextTaskExecutionTime;
    this.timeToNextExecution = timeToNextExecution;
    this.activeTasks = activeTasks;
    this.completedTasks = completedTasks;
  }

  public String getSupervisorId()
  {
    return supervisorId;
  }

  public ScheduledBatchSupervisorPayload.BatchSupervisorStatus getStatus()
  {
    return status;
  }

  public String getPreviousTaskExecutionTime()
  {
    return previousTaskExecutionTime;
  }

  public String getNextTaskExecutionTime()
  {
    return nextTaskExecutionTime;
  }

  public String getTimeToNextExecution()
  {
    return timeToNextExecution;
  }

  public Map<String, TaskStatus> getActiveTasks()
  {
    return activeTasks;
  }

  public Map<String, TaskStatus> getCompletedTasks()
  {
    return completedTasks;
  }
}
