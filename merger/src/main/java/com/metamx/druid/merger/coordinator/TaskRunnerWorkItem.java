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

package com.metamx.druid.merger.coordinator;

import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.task.Task;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

/**
 * A holder for a task and different components associated with the task
 */
public class TaskRunnerWorkItem implements Comparable<TaskRunnerWorkItem>
{
  private final Task task;
  private final TaskContext taskContext;
  private final TaskCallback callback;
  private final RetryPolicy retryPolicy;
  private final DateTime createdTime;

  private volatile DateTime queueInsertionTime;

  public TaskRunnerWorkItem(
      Task task,
      TaskContext taskContext,
      TaskCallback callback,
      RetryPolicy retryPolicy,
      DateTime createdTime
  )
  {
    this.task = task;
    this.taskContext = taskContext;
    this.callback = callback;
    this.retryPolicy = retryPolicy;
    this.createdTime = createdTime;
  }

  public Task getTask()
  {
    return task;
  }

  public TaskContext getTaskContext()
  {
    return taskContext;
  }

  public TaskCallback getCallback()
  {
    return callback;
  }

  public RetryPolicy getRetryPolicy()
  {
    return retryPolicy;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

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
    return DateTimeComparator.getInstance().compare(createdTime, taskRunnerWorkItem.getCreatedTime());
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

    TaskRunnerWorkItem that = (TaskRunnerWorkItem) o;

    if (callback != null ? !callback.equals(that.callback) : that.callback != null) {
      return false;
    }
    if (retryPolicy != null ? !retryPolicy.equals(that.retryPolicy) : that.retryPolicy != null) {
      return false;
    }
    if (task != null ? !task.equals(that.task) : that.task != null) {
      return false;
    }
    if (taskContext != null ? !taskContext.equals(that.taskContext) : that.taskContext != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = task != null ? task.hashCode() : 0;
    result = 31 * result + (taskContext != null ? taskContext.hashCode() : 0);
    result = 31 * result + (callback != null ? callback.hashCode() : 0);
    result = 31 * result + (retryPolicy != null ? retryPolicy.hashCode() : 0);
    return result;
  }

  @Override
  public String toString()
  {
    return "TaskRunnerWorkItem{" +
           "task=" + task +
           ", taskContext=" + taskContext +
           ", callback=" + callback +
           ", retryPolicy=" + retryPolicy +
           ", createdTime=" + createdTime +
           '}';
  }
}
