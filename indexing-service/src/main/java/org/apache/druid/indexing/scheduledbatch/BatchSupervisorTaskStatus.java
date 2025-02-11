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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.TaskStatus;
import org.joda.time.DateTime;

/**
 * Represents the status of a scheduled batch supervisor task and the timestamp of its last update.
 */
public class BatchSupervisorTaskStatus
{
  private final String supervisorId;
  private final TaskStatus taskStatus;
  private final DateTime updatedTime;

  public BatchSupervisorTaskStatus(
      String supervisorId, // This field is used only for internal tracking, so not Jackson serializable
      @JsonProperty("taskStatus") TaskStatus taskStatus,
      @JsonProperty("updatedTime") DateTime updatedTime
  )
  {
    this.supervisorId = supervisorId;
    this.taskStatus = taskStatus;
    this.updatedTime = updatedTime;
  }

  /**
   * Used for internal tracking. So this field is *not* Jackson serialized to avoid
   * redundant information in the user-facing objects.
   */
  @JsonIgnore
  public String getSupervisorId()
  {
    return supervisorId;
  }

  @JsonProperty
  public TaskStatus getTaskStatus()
  {
    return taskStatus;
  }

  @JsonProperty
  public DateTime getUpdatedTime()
  {
    return updatedTime;
  }

  @Override
  public String toString()
  {
    return "BatchSupervisorTaskStatus{" +
           "supervisorId='" + supervisorId + '\'' +
           ", taskStatus=" + taskStatus +
           ", updatedTime=" + updatedTime +
           '}';
  }
}
