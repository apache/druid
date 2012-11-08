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

import com.metamx.druid.merger.common.task.Task;

/**
 */
public class TaskWrapper
{
  private final Task task;
  private final TaskContext taskContext;
  private final TaskCallback callback;
  private final RetryPolicy retryPolicy;

  public TaskWrapper(Task task, TaskContext taskContext, TaskCallback callback, RetryPolicy retryPolicy)
  {
    this.task = task;
    this.taskContext = taskContext;
    this.callback = callback;
    this.retryPolicy = retryPolicy;
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
}
