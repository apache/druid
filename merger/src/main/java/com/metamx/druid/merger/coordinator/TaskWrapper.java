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

import com.google.common.primitives.Longs;
import com.metamx.druid.merger.common.TaskCallback;
import com.metamx.druid.merger.common.task.Task;

/**
 */
public class TaskWrapper implements Comparable<TaskWrapper>
{
  private final Task task;
  private final TaskContext taskContext;
  private final TaskCallback callback;
  private final RetryPolicy retryPolicy;
  private final long createdTime;

  public TaskWrapper(
      Task task,
      TaskContext taskContext,
      TaskCallback callback,
      RetryPolicy retryPolicy,
      long createdTime
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

  public long getCreatedTime()
  {
    return createdTime;
  }

  @Override
  public int compareTo(TaskWrapper taskWrapper)
  {
    return Longs.compare(createdTime, taskWrapper.getCreatedTime());
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

    TaskWrapper that = (TaskWrapper) o;

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
    return "TaskWrapper{" +
           "task=" + task +
           ", taskContext=" + taskContext +
           ", callback=" + callback +
           ", retryPolicy=" + retryPolicy +
           ", createdTime=" + createdTime +
           '}';
  }
}
