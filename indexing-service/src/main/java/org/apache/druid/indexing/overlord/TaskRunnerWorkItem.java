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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.java.util.common.DateTimes;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

/**
 * A holder for a task and different components associated with the task
 */
public abstract class TaskRunnerWorkItem
{
  private final String taskId;
  private final ListenableFuture<TaskStatus> result;
  private final DateTime createdTime;
  private final DateTime queueInsertionTime;

  public TaskRunnerWorkItem(String taskId, ListenableFuture<TaskStatus> result)
  {
    this(taskId, result, DateTimes.nowUtc());
  }

  private TaskRunnerWorkItem(String taskId, ListenableFuture<TaskStatus> result, DateTime createdTime)
  {
    this(taskId, result, createdTime, createdTime);
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

  @JsonProperty
  public abstract TaskLocation getLocation();

  /**
   * Returns the type of task. The return value can be null for backward compatibility.
   */
  @Nullable
  public abstract String getTaskType();
  public abstract String getDataSource();

  @Override
  public String toString()
  {
    return "TaskRunnerWorkItem{" +
           "taskId='" + taskId + '\'' +
           ", result=" + result +
           ", createdTime=" + createdTime +
           ", queueInsertionTime=" + queueInsertionTime +
           ", location=" + getLocation() +
           '}';
  }
}
