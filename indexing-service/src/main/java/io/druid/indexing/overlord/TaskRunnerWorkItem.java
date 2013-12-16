/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ComparisonChain;
import com.google.common.util.concurrent.ListenableFuture;
import io.druid.indexing.common.TaskStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

/**
 * A holder for a task and different components associated with the task
 */
public class TaskRunnerWorkItem implements Comparable<TaskRunnerWorkItem>
{
  private final String taskId;
  private final ListenableFuture<TaskStatus> result;
  private final DateTime createdTime;
  private final DateTime queueInsertionTime;

  public TaskRunnerWorkItem(
      String taskId,
      ListenableFuture<TaskStatus> result
  )
  {
    this(taskId, result, new DateTime(), new DateTime());
  }

  public TaskRunnerWorkItem(
      String taskId,
      ListenableFuture<TaskStatus> result,
      DateTime createdTime,
      DateTime queueInsertionTime
  )
  {
    this.taskId = taskId;
    this.result = result;
    this.createdTime = createdTime;
    this.queueInsertionTime = queueInsertionTime;
  }

  @JsonProperty
  public String getTaskId()
  {
    return taskId;
  }

  @JsonIgnore
  public ListenableFuture<TaskStatus> getResult()
  {
    return result;
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
    return new TaskRunnerWorkItem(taskId, result, createdTime, time);
  }

  @Override
  public int compareTo(TaskRunnerWorkItem taskRunnerWorkItem)
  {
    return ComparisonChain.start()
                          .compare(createdTime, taskRunnerWorkItem.getCreatedTime(), DateTimeComparator.getInstance())
                          .compare(taskId, taskRunnerWorkItem.getTaskId())
                          .result();
  }

  @Override
  public String toString()
  {
    return "TaskRunnerWorkItem{" +
           "taskId='" + taskId + '\'' +
           ", result=" + result +
           ", createdTime=" + createdTime +
           ", queueInsertionTime=" + queueInsertionTime +
           '}';
  }
}
