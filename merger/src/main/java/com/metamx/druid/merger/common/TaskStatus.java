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
import com.google.common.collect.ImmutableSet;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.merger.common.task.Task;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import java.util.List;
import java.util.Set;

/**
 * Represents the status of a task. The task may be ongoing ({@link #isComplete()} false) or it may be
 * complete ({@link #isComplete()} true).
 *
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
    return new TaskStatus(
        taskId,
        Status.RUNNING,
        ImmutableSet.<DataSegment>of(),
        ImmutableSet.<DataSegment>of(),
        ImmutableList.<Task>of(),
        -1
    );
  }

  public static TaskStatus success(String taskId)
  {
    return success(taskId, ImmutableSet.<DataSegment>of());
  }

  public static TaskStatus success(String taskId, Set<DataSegment> segments)
  {
    return new TaskStatus(
        taskId,
        Status.SUCCESS,
        segments,
        ImmutableSet.<DataSegment>of(),
        ImmutableList.<Task>of(),
        -1
    );
  }

  public static TaskStatus failure(String taskId)
  {
    return new TaskStatus(
        taskId,
        Status.FAILED,
        ImmutableSet.<DataSegment>of(),
        ImmutableSet.<DataSegment>of(),
        ImmutableList.<Task>of(),
        -1
    );
  }

  private final String id;
  private final ImmutableSet<DataSegment> segments;
  private final ImmutableSet<DataSegment> segmentsNuked;
  private final ImmutableList<Task> nextTasks;
  private final Status status;
  private final long duration;

  @JsonCreator
  private TaskStatus(
      @JsonProperty("id") String id,
      @JsonProperty("status") Status status,
      @JsonProperty("segments") Set<DataSegment> segments,
      @JsonProperty("segmentsNuked") Set<DataSegment> segmentsNuked,
      @JsonProperty("nextTasks") List<Task> nextTasks,
      @JsonProperty("duration") long duration
  )
  {
    this.id = id;
    this.segments = ImmutableSet.copyOf(segments);
    this.segmentsNuked = ImmutableSet.copyOf(segmentsNuked);
    this.nextTasks = ImmutableList.copyOf(nextTasks);
    this.status = status;
    this.duration = duration;

    // Check class invariants.
    Preconditions.checkNotNull(id, "id");
    Preconditions.checkNotNull(status, "status");

    if (this.segments.size() > 0) {
      Preconditions.checkArgument(
          status == Status.RUNNING || status == Status.SUCCESS,
          "segments not allowed for %s tasks",
          status
      );
    }

    if (this.segmentsNuked.size() > 0) {
      Preconditions.checkArgument(status == Status.SUCCESS, "segmentsNuked not allowed for %s tasks", status);
    }

    if (this.nextTasks.size() > 0) {
      Preconditions.checkArgument(
          status == Status.SUCCESS || status == Status.RUNNING,
          "nextTasks not allowed for %s tasks",
          status
      );
    }
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
  public Set<DataSegment> getSegments()
  {
    return segments;
  }

  @JsonProperty("segmentsNuked")
  public Set<DataSegment> getSegmentsNuked()
  {
    return segmentsNuked;
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

  public TaskStatus withSegments(Set<DataSegment> _segments)
  {
    return new TaskStatus(id, status, _segments, segmentsNuked, nextTasks, duration);
  }


  public TaskStatus withSegmentsNuked(Set<DataSegment> _segmentsNuked)
  {
    return new TaskStatus(id, status, segments, _segmentsNuked, nextTasks, duration);
  }

  public TaskStatus withNextTasks(List<Task> _nextTasks)
  {
    return new TaskStatus(id, status, segments, segmentsNuked, _nextTasks, duration);
  }

  public TaskStatus withDuration(long _duration)
  {
    return new TaskStatus(id, status, segments, segmentsNuked, nextTasks, _duration);
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
