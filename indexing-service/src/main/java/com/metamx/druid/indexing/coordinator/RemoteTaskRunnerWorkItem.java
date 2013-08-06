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

import com.google.common.util.concurrent.SettableFuture;
import com.metamx.druid.indexing.common.TaskStatus;
import com.metamx.druid.indexing.common.task.Task;
import org.joda.time.DateTime;

/**
 */
public class RemoteTaskRunnerWorkItem extends TaskRunnerWorkItem
{
  private final SettableFuture<TaskStatus> result;

  public RemoteTaskRunnerWorkItem(
      Task task,
      SettableFuture<TaskStatus> result
  )
  {
    super(task, result);
    this.result = result;
  }

  public RemoteTaskRunnerWorkItem(
      Task task,
      SettableFuture<TaskStatus> result,
      DateTime createdTime,
      DateTime queueInsertionTime
  )
  {
    super(task, result, createdTime, queueInsertionTime);
    this.result = result;
  }

  public void setResult(TaskStatus status)
  {
    result.set(status);
  }

  @Override
  public RemoteTaskRunnerWorkItem withQueueInsertionTime(DateTime time)
  {
    return new RemoteTaskRunnerWorkItem(getTask(), result, getCreatedTime(), time);
  }
}
