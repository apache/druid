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

public class ScheduledBatchSupervisorPayload
{
  public enum BatchSupervisorStatus
  {
    SCHEDULER_RUNNING,
    SCHEDULER_SHUTDOWN,
    SCHEDULER_ERROR
  }

  @JsonProperty
  private final String dataSource;
  @JsonProperty
  private final BatchSupervisorStatus status;
  @JsonProperty
  private final DateTime previousTaskExecutionTime;
  @JsonProperty
  private final DateTime nextTaskExecutionTime;
  @JsonProperty
  private final String detailedStatus;

  @JsonCreator
  public ScheduledBatchSupervisorPayload(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("status") BatchSupervisorStatus status,
      @JsonProperty("previousTaskExecutionTime") DateTime previousTaskExecutionTime,
      @JsonProperty("nextTaskExecutionTime") DateTime nextTaskExecutionTime,
      @JsonProperty("detailedStatus") String detailedStatus
  )
  {
    this.dataSource = dataSource;
    this.status = status;
    this.previousTaskExecutionTime = previousTaskExecutionTime;
    this.nextTaskExecutionTime = nextTaskExecutionTime;
    this.detailedStatus = detailedStatus;
  }
}
