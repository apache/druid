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
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Represents the status of a scheduled batch supervisor, including its state,
 * scheduler details, task execution aggregate counts, and recent task activity.
 */
public class ScheduledBatchSupervisorStatus
{
  private final String supervisorId;

  private final ScheduledBatchSupervisor.State state;

  @Nullable
  private final DateTime lastTaskSubmittedTime;

  @Nullable
  private final DateTime nextTaskSubmissionTime;

  @Nullable
  private final Duration timeUntilNextTaskSubmission;

  private final Integer totalSubmittedTasks;

  private final Integer totalSuccessfulTasks;

  private final Integer totalFailedTasks;

  private final List<BatchSupervisorTaskStatus> recentActiveTasks;

  private final List<BatchSupervisorTaskStatus> recentSuccessfulTasks;

  private final List<BatchSupervisorTaskStatus> recentFailedTasks;

  @JsonCreator
  public ScheduledBatchSupervisorStatus(
      @JsonProperty("supervisorId") String supervisorId,
      @JsonProperty("status") ScheduledBatchSupervisor.State state,
      @JsonProperty("lastTaskSubmittedTime") @Nullable DateTime lastTaskSubmittedTime,
      @JsonProperty("nextTaskSubmissionTime") @Nullable DateTime nextTaskSubmissionTime,
      @JsonProperty("timeUntilNextTaskSubmission") @Nullable Duration timeUntilNextTaskSubmission,
      @JsonProperty("totalSubmittedTasks") Integer totalSubmittedTasks,
      @JsonProperty("totalSuccessfulTasks") Integer totalSuccessfulTasks,
      @JsonProperty("totalFailedTasks") Integer totalFailedTasks,
      @JsonProperty("recentActiveTasks") List<BatchSupervisorTaskStatus> recentActiveTasks,
      @JsonProperty("recentSuccessfulTasks") List<BatchSupervisorTaskStatus> recentSuccessfulTasks,
      @JsonProperty("recentFailedTasks") List<BatchSupervisorTaskStatus> recentFailedTasks
  )
  {
    this.supervisorId = supervisorId;
    this.state = state;
    this.lastTaskSubmittedTime = lastTaskSubmittedTime;
    this.nextTaskSubmissionTime = nextTaskSubmissionTime;
    this.timeUntilNextTaskSubmission = timeUntilNextTaskSubmission;
    this.totalSubmittedTasks = totalSubmittedTasks;
    this.totalSuccessfulTasks = totalSuccessfulTasks;
    this.totalFailedTasks = totalFailedTasks;
    this.recentActiveTasks = recentActiveTasks;
    this.recentSuccessfulTasks = recentSuccessfulTasks;
    this.recentFailedTasks = recentFailedTasks;
  }

  @JsonProperty
  public String getSupervisorId()
  {
    return supervisorId;
  }

  @JsonProperty
  public ScheduledBatchSupervisor.State getState()
  {
    return state;
  }

  @Nullable
  @JsonProperty
  public DateTime getLastTaskSubmittedTime()
  {
    return lastTaskSubmittedTime;
  }

  @Nullable
  @JsonProperty
  public DateTime getNextTaskSubmissionTime()
  {
    return nextTaskSubmissionTime;
  }

  @Nullable
  @JsonProperty
  public Duration getTimeUntilNextTaskSubmission()
  {
    return timeUntilNextTaskSubmission;
  }

  @JsonProperty
  public Integer getTotalSubmittedTasks()
  {
    return totalSubmittedTasks;
  }

  @JsonProperty
  public Integer getTotalSuccessfulTasks()
  {
    return totalSuccessfulTasks;
  }

  @JsonProperty
  public Integer getTotalFailedTasks()
  {
    return totalFailedTasks;
  }

  @JsonProperty
  public List<BatchSupervisorTaskStatus> getRecentActiveTasks()
  {
    return recentActiveTasks;
  }

  @JsonProperty
  public List<BatchSupervisorTaskStatus> getRecentSuccessfulTasks()
  {
    return recentSuccessfulTasks;
  }

  @JsonProperty
  public List<BatchSupervisorTaskStatus> getRecentFailedTasks()
  {
    return recentFailedTasks;
  }
}
