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
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

/**
 * Represents the status of a task from the perspective of the coordinator. The task may be ongoing
 * ({@link #isComplete()} false) or it may be complete ({@link #isComplete()} true).
 *
 * TaskStatus objects are immutable.
 */
public class TaskStatus
{
  public static final int MAX_ERROR_MSG_LENGTH = 100;

  public static TaskStatus running(String taskId)
  {
    return new TaskStatus(taskId, TaskState.RUNNING, -1, null, null);
  }

  public static TaskStatus success(String taskId)
  {
    return new TaskStatus(taskId, TaskState.SUCCESS, -1, null, null);
  }

  public static TaskStatus success(String taskId, String errorMsg)
  {
    return new TaskStatus(taskId, TaskState.SUCCESS, -1, errorMsg, null);
  }

  public static TaskStatus failure(String taskId)
  {
    return new TaskStatus(taskId, TaskState.FAILED, -1, null, null);
  }

  public static TaskStatus failure(String taskId, String errorMsg)
  {
    return new TaskStatus(taskId, TaskState.FAILED, -1, errorMsg, null);
  }

  public static TaskStatus fromCode(String taskId, TaskState code)
  {
    return new TaskStatus(taskId, code, -1, null, null);
  }

  /**
   * The error message can be large, so truncate it to avoid storing large objects in zookeeper/metadata storage.
   * The full error message will be available via a TaskReport.
   */
  private static @Nullable String truncateErrorMsg(@Nullable String errorMsg)
  {
    if (errorMsg != null && errorMsg.length() > MAX_ERROR_MSG_LENGTH) {
      return errorMsg.substring(0, MAX_ERROR_MSG_LENGTH) + "...";
    } else {
      return errorMsg;
    }
  }

  private final String id;
  private final TaskState status;
  private final long duration;
  private final @Nullable String errorMsg;
  private final TaskLocation location;

  @JsonCreator
  protected TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") TaskState status,
      @JsonProperty("duration") long duration,
      @JsonProperty("errorMsg") @Nullable String errorMsg,
      @Nullable @JsonProperty("location") TaskLocation location
  )
  {
    this.id = id;
    this.status = status;
    this.duration = duration;
    this.errorMsg = truncateErrorMsg(errorMsg);
    this.location = location == null ? TaskLocation.unknown() : location;

    // Check class invariants.
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkNotNull(status, "status");
  }

  @JsonProperty("id")
  public String getId()
  {
    return id;
  }

  @JsonProperty("status")
  public TaskState getStatusCode()
  {
    return status;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  @Nullable
  @JsonProperty("errorMsg")
  public String getErrorMsg()
  {
    return errorMsg;
  }

  @JsonProperty("location")
  public TaskLocation getLocation()
  {
    return location;
  }

  /**
   * Signals that a task is not yet complete, and is still runnable on a worker. Exactly one of isRunnable,
   * isSuccess, or isFailure will be true at any one time.
   *
   * @return whether the task is runnable.
   */
  @JsonIgnore
  public boolean isRunnable()
  {
    return status == TaskState.RUNNING;
  }

  /**
   * Inverse of {@link #isRunnable}.
   *
   * @return whether the task is complete.
   */
  @JsonIgnore
  public boolean isComplete()
  {
    return !isRunnable();
  }

  /**
   * Returned by tasks when they spawn subtasks. Exactly one of isRunnable, isSuccess, or isFailure will
   * be true at any one time.
   *
   * @return whether the task succeeded.
   */
  @JsonIgnore
  public boolean isSuccess()
  {
    return status == TaskState.SUCCESS;
  }

  /**
   * Returned by tasks when they complete unsuccessfully. Exactly one of isRunnable, isSuccess, or
   * isFailure will be true at any one time.
   *
   * @return whether the task failed
   */
  @JsonIgnore
  public boolean isFailure()
  {
    return status == TaskState.FAILED;
  }

  public TaskStatus withDuration(long _duration)
  {
    return new TaskStatus(id, status, _duration, errorMsg, location);
  }

  public TaskStatus withLocation(TaskLocation location)
  {
    if (location == null) {
      location = TaskLocation.unknown();
    }
    return new TaskStatus(
        id,
        status,
        duration,
        errorMsg,
        location
    );
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("status", status)
                  .add("duration", duration)
                  .add("errorMsg", errorMsg)
                  .toString();
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
    TaskStatus that = (TaskStatus) o;
    return getDuration() == that.getDuration() &&
           java.util.Objects.equals(getId(), that.getId()) &&
           status == that.status &&
           java.util.Objects.equals(getErrorMsg(), that.getErrorMsg());
  }

  @Override
  public int hashCode()
  {
    return java.util.Objects.hash(getId(), status, getDuration(), getErrorMsg());
  }
}
