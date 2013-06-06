/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;

/**
 * Represents the status of a task. The task may be ongoing ({@link #isComplete()} false) or it may be
 * complete ({@link #isComplete()} true).
 * <p/>
 * TaskStatus objects are immutable.
 */
public class TaskStatus
{
  public static enum Status
  {
    RUNNING,
    SUCCESS,
    FAILED
  }

  public static TaskStatus running(String taskId)
  {
    return new TaskStatus(taskId, Status.RUNNING, -1);
  }

  public static TaskStatus success(String taskId)
  {
    return new TaskStatus(taskId, Status.SUCCESS, -1);
  }

  public static TaskStatus failure(String taskId)
  {
    return new TaskStatus(taskId, Status.FAILED, -1);
  }

  private final String id;
  private final Status status;
  private final long duration;

  @JsonCreator
  private TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") Status status,
      @JsonProperty("duration") long duration
  )
  {
    this.id = id;
    this.status = status;
    this.duration = duration;

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
  public Status getStatusCode()
  {
    return status;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  /**
   * Signals that a task is not yet complete, and is still runnable on a worker. Exactly one of isRunnable,
   * isSuccess, or isFailure will be true at any one time.
   */
  @JsonIgnore
  public boolean isRunnable()
  {
    return status == Status.RUNNING;
  }

  /**
   * Inverse of {@link #isRunnable}.
   */
  @JsonIgnore
  public boolean isComplete()
  {
    return !isRunnable();
  }

  /**
   * Returned by tasks when they spawn subtasks. Exactly one of isRunnable, isSuccess, or isFailure will
   * be true at any one time.
   */
  @JsonIgnore
  public boolean isSuccess()
  {
    return status == Status.SUCCESS;
  }

  /**
   * Returned by tasks when they complete unsuccessfully. Exactly one of isRunnable, isSuccess, or
   * isFailure will be true at any one time.
   */
  @JsonIgnore
  public boolean isFailure()
  {
    return status == Status.FAILED;
  }

  public TaskStatus withDuration(long _duration)
  {
    return new TaskStatus(id, status, _duration);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("status", status)
                  .add("duration", duration)
                  .toString();
  }
}
