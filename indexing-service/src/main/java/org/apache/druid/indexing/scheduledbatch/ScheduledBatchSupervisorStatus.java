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

import javax.annotation.Nullable;

/**
 * Represents the status of a scheduled batch supervisor, including its state, scheduler details and task report.
 */
public class ScheduledBatchSupervisorStatus
{
  private final String supervisorId;

  private final ScheduledBatchSupervisor.State state;

  @Nullable
  private final DateTime lastTaskSubmittedTime;

  @Nullable
  private final DateTime nextTaskSubmissionTime;

  private final BatchSupervisorTaskReport taskReport;

  @JsonCreator
  public ScheduledBatchSupervisorStatus(
      @JsonProperty("supervisorId") String supervisorId,
      @JsonProperty("state") ScheduledBatchSupervisor.State state,
      @JsonProperty("lastTaskSubmittedTime") @Nullable DateTime lastTaskSubmittedTime,
      @JsonProperty("nextTaskSubmissionTime") @Nullable DateTime nextTaskSubmissionTime,
      @JsonProperty("taskReport") BatchSupervisorTaskReport taskReport
  )
  {
    this.supervisorId = supervisorId;
    this.state = state;
    this.lastTaskSubmittedTime = lastTaskSubmittedTime;
    this.nextTaskSubmissionTime = nextTaskSubmissionTime;
    this.taskReport = taskReport;
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

  @JsonProperty
  public BatchSupervisorTaskReport getTaskReport()
  {
    return taskReport;
  }
}
