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

import com.google.common.util.concurrent.SettableFuture;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.worker.Worker;
import org.joda.time.DateTime;

/**
 */
public class RemoteTaskRunnerWorkItem extends TaskRunnerWorkItem
{
  private final SettableFuture<TaskStatus> result;
  private final Worker worker;

  public RemoteTaskRunnerWorkItem(
      String taskId,
      SettableFuture<TaskStatus> result,
      Worker worker
  )
  {
    super(taskId, result);
    this.result = result;
    this.worker = worker;
  }

  public RemoteTaskRunnerWorkItem(
      String taskId,
      SettableFuture<TaskStatus> result,
      DateTime createdTime,
      DateTime queueInsertionTime,
      Worker worker
  )
  {
    super(taskId, result, createdTime, queueInsertionTime);
    this.result = result;
    this.worker = worker;
  }

  public Worker getWorker()
  {
    return worker;
  }

  public void setResult(TaskStatus status)
  {
    result.set(status);
  }

  @Override
  public RemoteTaskRunnerWorkItem withQueueInsertionTime(DateTime time)
  {
    return new RemoteTaskRunnerWorkItem(getTaskId(), result, getCreatedTime(), time, worker);
  }

  public RemoteTaskRunnerWorkItem withWorker(Worker theWorker)
  {
    return new RemoteTaskRunnerWorkItem(getTaskId(), result, getCreatedTime(), getQueueInsertionTime(), theWorker);
  }
}
