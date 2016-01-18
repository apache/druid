/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
      Worker worker
  )
  {
    this(taskId, SettableFuture.<TaskStatus>create(), worker);
  }

  public RemoteTaskRunnerWorkItem(
      String taskId,
      DateTime createdTime,
      DateTime queueInsertionTime,
      Worker worker
  )
  {
    this(taskId, SettableFuture.<TaskStatus>create(), createdTime, queueInsertionTime, worker);
  }

  private RemoteTaskRunnerWorkItem(
      String taskId,
      SettableFuture<TaskStatus> result,
      Worker worker
  )
  {
    super(taskId, result);
    this.result = result;
    this.worker = worker;
  }

  private RemoteTaskRunnerWorkItem(
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
