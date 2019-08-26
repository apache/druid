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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.RE;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Objects;

public class TaskStatusPlus
{
  private final String id;
  private final String type;
  private final DateTime createdTime;
  private final DateTime queueInsertionTime;
  private final TaskState statusCode;
  private final RunnerTaskState runnerTaskState;
  private final Long duration;
  private final TaskLocation location;
  private final String dataSource;

  @Nullable
  private final String errorMsg;
  @Nullable
  private final String groupId;

  public TaskStatusPlus(
      String id,
      @Nullable String groupId,
      String type, // nullable for backward compatibility
      DateTime createdTime,
      DateTime queueInsertionTime,
      @Nullable TaskState statusCode,
      @Nullable RunnerTaskState runnerStatusCode,
      @Nullable Long duration,
      TaskLocation location,
      @Nullable String dataSource, // nullable for backward compatibility
      @Nullable String errorMsg
  )
  {
    this(
        id,
        groupId,
        type,
        createdTime,
        queueInsertionTime,
        statusCode,
        statusCode,
        runnerStatusCode,
        duration,
        location,
        dataSource,
        errorMsg
    );
  }

  @JsonCreator
  public TaskStatusPlus(
      @JsonProperty("id") String id,
      @JsonProperty("groupId") @Nullable String groupId,
      @JsonProperty("type") @Nullable String type, // nullable for backward compatibility
      @JsonProperty("createdTime") DateTime createdTime,
      @JsonProperty("queueInsertionTime") DateTime queueInsertionTime,
      @JsonProperty("statusCode") @Nullable TaskState statusCode,
      @Deprecated @JsonProperty("status") @Nullable TaskState status,  // present for backwards compatibility
      @JsonProperty("runnerStatusCode") @Nullable RunnerTaskState runnerTaskState,
      @JsonProperty("duration") @Nullable Long duration,
      @JsonProperty("location") TaskLocation location,
      @JsonProperty("dataSource") @Nullable String dataSource, // nullable for backward compatibility
      @JsonProperty("errorMsg") @Nullable String errorMsg
  )
  {
    if (statusCode != null && statusCode.isComplete()) {
      Preconditions.checkNotNull(duration, "duration");
    }
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = groupId;
    this.type = type;
    this.createdTime = Preconditions.checkNotNull(createdTime, "createdTime");
    this.queueInsertionTime = Preconditions.checkNotNull(queueInsertionTime, "queueInsertionTime");
    //checks for deserialization safety
    if (statusCode != null && status == null) {
      this.statusCode = statusCode;
    } else if (statusCode == null && status != null) {
      this.statusCode = status;
    } else {
      if (statusCode != null && status != null && statusCode != status) {
        throw new RE("statusCode[%s] and status[%s] must match", statusCode, status);
      }
      this.statusCode = statusCode;
    }
    this.runnerTaskState = runnerTaskState;
    this.duration = duration;
    this.location = Preconditions.checkNotNull(location, "location");
    this.dataSource = dataSource;
    this.errorMsg = errorMsg;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @Nullable
  @JsonProperty
  public String getGroupId()
  {
    return groupId;
  }

  @Nullable
  @JsonProperty
  public String getType()
  {
    return type;
  }

  @JsonProperty
  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @JsonProperty
  public DateTime getQueueInsertionTime()
  {
    return queueInsertionTime;
  }

  @Nullable
  @JsonProperty("statusCode")
  public TaskState getStatusCode()
  {
    return statusCode;
  }

  @Deprecated
  @Nullable
  @JsonProperty("status")
  public TaskState getStatus()
  {
    return statusCode;
  }

  @Nullable
  @JsonProperty("runnerStatusCode")
  public RunnerTaskState getRunnerStatusCode()
  {
    return runnerTaskState;
  }

  @Nullable
  @JsonProperty
  public Long getDuration()
  {
    return duration;
  }

  @JsonProperty
  public TaskLocation getLocation()
  {
    return location;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @Nullable
  @JsonProperty("errorMsg")
  public String getErrorMsg()
  {
    return errorMsg;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TaskStatusPlus that = (TaskStatusPlus) o;
    return Objects.equals(getId(), that.getId()) &&
           Objects.equals(getGroupId(), that.getGroupId()) &&
           Objects.equals(getType(), that.getType()) &&
           Objects.equals(getCreatedTime(), that.getCreatedTime()) &&
           Objects.equals(getQueueInsertionTime(), that.getQueueInsertionTime()) &&
           getStatusCode() == that.getStatusCode() &&
           Objects.equals(getDuration(), that.getDuration()) &&
           Objects.equals(getLocation(), that.getLocation()) &&
           Objects.equals(getDataSource(), that.getDataSource()) &&
           Objects.equals(getErrorMsg(), that.getErrorMsg());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getId(),
        getGroupId(),
        getType(),
        getCreatedTime(),
        getQueueInsertionTime(),
        getStatusCode(),
        getDuration(),
        getLocation(),
        getDataSource(),
        getErrorMsg()
    );
  }

  @Override
  public String toString()
  {
    return "TaskStatusPlus{" +
           "id='" + id + '\'' +
           ", groupId='" + groupId + '\'' +
           ", type='" + type + '\'' +
           ", createdTime=" + createdTime +
           ", queueInsertionTime=" + queueInsertionTime +
           ", statusCode=" + statusCode +
           ", duration=" + duration +
           ", location=" + location +
           ", dataSource='" + dataSource + '\'' +
           ", errorMsg='" + errorMsg + '\'' +
           '}';
  }
}
