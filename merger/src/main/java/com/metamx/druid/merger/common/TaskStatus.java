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

package com.metamx.druid.merger.common;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.task.Task;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.Collections;
import java.util.List;

public class TaskStatus
{
  public static enum Status
  {
    RUNNING,
    SUCCESS,
    FAILED,
    CONTINUED
  }

  public static TaskStatus running(String taskId)
  {
    return new TaskStatus(
        taskId,
        Status.RUNNING,
        Collections.<DataSegment>emptyList(),
        Collections.<Task>emptyList(),
        -1
    );
  }

  public static TaskStatus failure(String taskId)
  {
    return new TaskStatus(taskId, Status.FAILED, Collections.<DataSegment>emptyList(), Collections.<Task>emptyList(), -1);
  }

  public static TaskStatus success(String taskId, List<DataSegment> segments)
  {
    return new TaskStatus(taskId, Status.SUCCESS, ImmutableList.copyOf(segments), Collections.<Task>emptyList(), -1);
  }

  public static TaskStatus continued(String taskId, List<Task> nextTasks)
  {
    Preconditions.checkArgument(nextTasks.size() > 0, "nextTasks.size() > 0");
    return new TaskStatus(
        taskId,
        Status.CONTINUED,
        Collections.<DataSegment>emptyList(),
        ImmutableList.copyOf(nextTasks),
        -1
    );
  }

  private final String id;
  private final List<DataSegment> segments;
  private final List<Task> nextTasks;
  private final Status status;
  private final long duration;

  @JsonCreator
  private TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") Status status,
      @JsonProperty("segments") List<DataSegment> segments,
      @JsonProperty("nextTasks") List<Task> nextTasks,
      @JsonProperty("duration") long duration
  )
  {
    this.id = id;
    this.segments = segments;
    this.nextTasks = nextTasks;
    this.status = status;
    this.duration = duration;
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

  @JsonProperty("segments")
  public List<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty("nextTasks")
  public List<Task> getNextTasks()
  {
    return nextTasks;
  }

  @JsonProperty("duration")
  public long getDuration()
  {
    return duration;
  }

  /**
   * Signals that a task is not yet complete, and is still runnable on a worker. Exactly one of isRunnable,
   * isContinued, isSuccess, or isFailure will be true at any one time.
   */
  public boolean isRunnable()
  {
    return status == Status.RUNNING;
  }

  /**
   * Returned by tasks when they complete successfully without spawning subtasks. Exactly one of isRunnable,
   * isContinued, isSuccess, or isFailure will be true at any one time.
   */
  public boolean isContinued()
  {
    return status == Status.CONTINUED;
  }

  /**
   * Inverse of {@link #isRunnable}.
   */
  public boolean isComplete()
  {
    return !isRunnable();
  }

  /**
   * Returned by tasks when they spawn subtasks. Exactly one of isRunnable, isContinued, isSuccess, or isFailure will
   * be true at any one time.
   */
  public boolean isSuccess()
  {
    return status == Status.SUCCESS;
  }

  /**
   * Returned by tasks when they complete unsuccessfully. Exactly one of isRunnable, isContinued, isSuccess, or
   * isFailure will be true at any one time.
   */
  public boolean isFailure()
  {
    return status == Status.FAILED;
  }

  public TaskStatus withDuration(long _duration)
  {
    return new TaskStatus(id, status, segments, nextTasks, _duration);
  }

  @Override
  public String toString()
  {
    return Objects.toStringHelper(this)
                  .add("id", id)
                  .add("segments", segments)
                  .add("nextTasks", nextTasks)
                  .add("status", status)
                  .add("duration", duration)
                  .toString();
  }
}
