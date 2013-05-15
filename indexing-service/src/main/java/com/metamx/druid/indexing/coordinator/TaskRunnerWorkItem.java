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

package com.metamx.druid.indexing.coordinator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ComparisonChain;
import com.google.common.util.concurrent.ListenableFuture;
import com.metamx.druid.indexing.common.RetryPolicy;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.task.Task;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

/**
 * A holder for a task and different components associated with the task
 */
public class TaskRunnerWorkItem implements Comparable<TaskRunnerWorkItem>
{
  private final Task task;
  private final ListenableFuture<TaskStatus> result;
  private final RetryPolicy retryPolicy;
  private final DateTime createdTime;

  private volatile DateTime queueInsertionTime;

  public TaskRunnerWorkItem(
      Task task,
      ListenableFuture<TaskStatus> result,
      RetryPolicy retryPolicy,
      DateTime createdTime
  )
  {
    this.task = task;
    this.result = result;
    this.retryPolicy = retryPolicy;
    this.createdTime = createdTime;
  }

  @JsonProperty
  public Task getTask()
  {
    return task;
  }

  public ListenableFuture<TaskStatus> getResult()
  {
    return result;
  }

  public RetryPolicy getRetryPolicy()
  {
    return retryPolicy;
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

  public TaskRunnerWorkItem withQueueInsertionTime(DateTime time)
  {
    this.queueInsertionTime = time;
    return this;
  }

  @Override
  public int compareTo(TaskRunnerWorkItem taskRunnerWorkItem)
  {
    return ComparisonChain.start()
                          .compare(createdTime, taskRunnerWorkItem.getCreatedTime(), DateTimeComparator.getInstance())
                          .compare(task.getId(), taskRunnerWorkItem.getTask().getId())
                          .result();
  }

  @Override
  public String toString()
  {
    return "TaskRunnerWorkItem{" +
           "task=" + task +
           ", result=" + result +
           ", retryPolicy=" + retryPolicy +
           ", createdTime=" + createdTime +
           '}';
  }
}
