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

package io.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import io.druid.indexer.TaskState;

import java.util.Map;

/**
 * Represents the status of a task from the perspective of the coordinator. The task may be ongoing
 * ({@link #isComplete()} false) or it may be complete ({@link #isComplete()} true).
 *
 * TaskStatus objects are immutable.
 */
public class TaskStatus
{
  public static TaskStatus running(String taskId)
  {
    return new TaskStatus(taskId, TaskState.RUNNING, -1, null, null, null);
  }

  public static TaskStatus success(String taskId)
  {
    return new TaskStatus(taskId, TaskState.SUCCESS, -1, null, null, null);
  }

  public static TaskStatus success(String taskId, Map<String, Object> metrics, String errorMsg, Map<String, Object> context)
  {
    return new TaskStatus(taskId, TaskState.SUCCESS, -1, metrics, errorMsg, context);
  }

  public static TaskStatus failure(String taskId)
  {
    return new TaskStatus(taskId, TaskState.FAILED, -1, null, null, null);
  }

  public static TaskStatus failure(String taskId, Map<String, Object> metrics, String errorMsg, Map<String, Object> context)
  {
    return new TaskStatus(taskId, TaskState.FAILED, -1, metrics, errorMsg, context);
  }

  public static TaskStatus fromCode(String taskId, TaskState code)
  {
    return new TaskStatus(taskId, code, -1, null, null, null);
  }

  private final String id;
  private final TaskState status;
  private final long duration;
  private final Map<String, Object> metrics;
  private final String errorMsg;
  private final Map<String, Object> context;

  @JsonCreator
  protected TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") TaskState status,
      @JsonProperty("duration") long duration,
      @JsonProperty("metrics") Map<String, Object> metrics,
      @JsonProperty("errorMsg") String errorMsg,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    this.id = id;
    this.status = status;
    this.duration = duration;
    this.metrics = metrics;
    this.errorMsg = errorMsg;
    this.context = context;

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

  @JsonProperty("metrics")
  public Map<String, Object> getMetrics()
  {
    return metrics;
  }

  @JsonProperty("errorMsg")
  public String getErrorMsg()
  {
    return errorMsg;
  }

  @JsonProperty("context")
  public Map<String, Object> getContext()
  {
    return context;
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
    return new TaskStatus(id, status, _duration, metrics, errorMsg, context);
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
    return duration == that.duration &&
           java.util.Objects.equals(id, that.id) &&
           status == that.status &&
           java.util.Objects.equals(metrics, that.metrics) &&
           java.util.Objects.equals(errorMsg, that.errorMsg) &&
           java.util.Objects.equals(context, that.context);
  }

  @Override
  public int hashCode()
  {
    return java.util.Objects.hash(id, status, duration);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("status", status)
                  .add("duration", duration)
                  .add("metrics", metrics)
                  .add("errorMsg", errorMsg)
                  .add("context", context)
                  .toString();
  }
}
