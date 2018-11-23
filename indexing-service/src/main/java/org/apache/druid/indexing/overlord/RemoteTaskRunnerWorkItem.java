/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.overlord;

import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.worker.Worker;
import org.joda.time.DateTime;

/**
 */
public class RemoteTaskRunnerWorkItem extends TaskRunnerWorkItem
{
  private final SettableFuture<TaskStatus> result;
  private String taskType;
  private final String dataSource;
  private Worker worker;
  private TaskLocation location;

  public RemoteTaskRunnerWorkItem(
      String taskId,
      String taskType,
      Worker worker,
      TaskLocation location,
      String dataSource
  )
  {
    this(taskId, taskType, SettableFuture.create(), worker, location, dataSource);
  }

  private RemoteTaskRunnerWorkItem(
      String taskId,
      String taskType,
      SettableFuture<TaskStatus> result,
      Worker worker,
      TaskLocation location,
      String dataSource
  )
  {
    super(taskId, result);
    this.result = result;
    this.taskType = taskType;
    this.worker = worker;
    this.location = location == null ? TaskLocation.unknown() : location;
    this.dataSource = dataSource;
  }

  private RemoteTaskRunnerWorkItem(
      String taskId,
      String taskType,
      SettableFuture<TaskStatus> result,
      DateTime createdTime,
      DateTime queueInsertionTime,
      Worker worker,
      TaskLocation location,
      String dataSource
  )
  {
    super(taskId, result, createdTime, queueInsertionTime);
    this.result = result;
    this.taskType = taskType;
    this.worker = worker;
    this.location = location == null ? TaskLocation.unknown() : location;
    this.dataSource = dataSource;
  }

  public void setLocation(TaskLocation location)
  {
    this.location = location;
  }

  @Override
  public TaskLocation getLocation()
  {
    return location;
  }

  public void setTaskType(String taskType)
  {
    this.taskType = taskType;
  }

  @Override
  public String getTaskType()
  {
    return taskType;
  }
  
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  public void setWorker(Worker worker)
  {
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

  public RemoteTaskRunnerWorkItem withQueueInsertionTime(DateTime time)
  {
    return new RemoteTaskRunnerWorkItem(getTaskId(), taskType, result, getCreatedTime(), time, worker, location, dataSource);
  }

  public RemoteTaskRunnerWorkItem withWorker(Worker theWorker, TaskLocation location)
  {
    return new RemoteTaskRunnerWorkItem(
        getTaskId(),
        taskType,
        result,
        getCreatedTime(),
        getQueueInsertionTime(),
        theWorker,
        location,
        dataSource
    );
  }
}
