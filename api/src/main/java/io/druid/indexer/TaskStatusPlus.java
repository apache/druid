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
import com.google.common.base.Preconditions;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Objects;

public class TaskStatusPlus
{
  private final String id;
  private final String type;
  private final DateTime createdTime;
  private final DateTime queueInsertionTime;
  private final TaskState state;
  private final Long duration;
  private final TaskLocation location;
  private final String dataSource;

  @Nullable
  private final String errorMsg;

  @JsonCreator
  public TaskStatusPlus(
      @JsonProperty("id") String id,
      @JsonProperty("type") @Nullable String type, // nullable for backward compatibility
      @JsonProperty("createdTime") DateTime createdTime,
      @JsonProperty("queueInsertionTime") DateTime queueInsertionTime,
      @JsonProperty("statusCode") @Nullable TaskState state,
      @JsonProperty("duration") @Nullable Long duration,
      @JsonProperty("location") TaskLocation location,
      @JsonProperty("dataSource") @Nullable String dataSource, // nullable for backward compatibility
      @JsonProperty("errorMsg") @Nullable String errorMsg
  )
  {
    if (state != null && state.isComplete()) {
      Preconditions.checkNotNull(duration, "duration");
    }
    this.id = Preconditions.checkNotNull(id, "id");
    this.type = type;
    this.createdTime = Preconditions.checkNotNull(createdTime, "createdTime");
    this.queueInsertionTime = Preconditions.checkNotNull(queueInsertionTime, "queueInsertionTime");
    this.state = state;
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
  public TaskState getState()
  {
    return state;
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
           Objects.equals(getType(), that.getType()) &&
           Objects.equals(getCreatedTime(), that.getCreatedTime()) &&
           Objects.equals(getQueueInsertionTime(), that.getQueueInsertionTime()) &&
           getState() == that.getState() &&
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
        getType(),
        getCreatedTime(),
        getQueueInsertionTime(),
        getState(),
        getDuration(),
        getLocation(),
        getDataSource(),
        getErrorMsg()
    );
  }
}
