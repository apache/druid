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

package org.apache.druid.indexing.scheduledbatch;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.TaskStatus;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.Map;

public class ScheduledBatchSupervisorSnapshot
{
  public enum BatchSupervisorStatus
  {
    SCHEDULER_RUNNING,
    SCHEDULER_SHUTDOWN,
    SCHEDULER_ERROR
  }

  @JsonProperty
  private final String supervisorId;

  @JsonProperty
  private final BatchSupervisorStatus status;

  @Nullable
  @JsonProperty
  private final DateTime lastTaskSubmittedTime;

  @Nullable
  @JsonProperty
  private final DateTime nextTaskSubmissionTime;

  @Nullable
  @JsonProperty
  private final Duration timeUntilNextTaskSubmission;

  @JsonProperty
  private final Map<String, TaskStatus> activeTasks;

  @JsonProperty
  private final Map<String, TaskStatus> completedTasks;

  @JsonCreator
  public ScheduledBatchSupervisorSnapshot(
      @JsonProperty("supervisorId") String supervisorId,
      @JsonProperty("status") BatchSupervisorStatus status,
      @JsonProperty("lastTaskSubmittedTime") @Nullable DateTime lastTaskSubmittedTime,
      @JsonProperty("nextTaskSubmissionTime") @Nullable DateTime nextTaskSubmissionTime,
      @JsonProperty("timeUntilNextTaskSubmission") @Nullable Duration timeUntilNextTaskSubmission,
      @JsonProperty("activeTasks") Map<String, TaskStatus> activeTasks,
      @JsonProperty("completedTasks") Map<String, TaskStatus> completedTasks
  )
  {
    this.supervisorId = supervisorId;
    this.status = status;
    this.lastTaskSubmittedTime = lastTaskSubmittedTime;
    this.nextTaskSubmissionTime = nextTaskSubmissionTime;
    this.timeUntilNextTaskSubmission = timeUntilNextTaskSubmission;
    this.activeTasks = activeTasks;
    this.completedTasks = completedTasks;
  }

  public String getSupervisorId()
  {
    return supervisorId;
  }

  public ScheduledBatchSupervisorSnapshot.BatchSupervisorStatus getStatus()
  {
    return status;
  }

  public DateTime getLastTaskSubmittedTime()
  {
    return lastTaskSubmittedTime;
  }

  public DateTime getNextTaskSubmissionTime()
  {
    return nextTaskSubmissionTime;
  }

  public Duration getTimeUntilNextTaskSubmission()
  {
    return timeUntilNextTaskSubmission;
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
