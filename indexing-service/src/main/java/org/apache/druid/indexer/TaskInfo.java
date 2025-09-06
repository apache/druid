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

package org.apache.druid.indexer;

import com.google.common.base.Preconditions;
import org.apache.druid.indexing.common.task.Task;
import org.joda.time.DateTime;

/**
 * Contains the {@link TaskStatus} of a {@link Task} and its created time.
 */
public class TaskInfo
{
  private final DateTime createdTime;
  private final TaskStatus status;
  private final Task task;

  public TaskInfo(
      DateTime createdTime,
      TaskStatus status,
      Task task
  )
  {
    this.createdTime = Preconditions.checkNotNull(createdTime, "createdTime");
    this.status = Preconditions.checkNotNull(status, "status");
    this.task = Preconditions.checkNotNull(task, "Task cannot be null");
  }

  public String getId()
  {
    return task.getId();
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public TaskStatus getStatus()
  {
    return status;
  }

  public String getDataSource()
  {
    return task.getDataSource();
  }

  public Task getTask()
  {
    return task;
  }

  /**
   * Returns a copy of this TaskInfo object with the given status.
   */
  public TaskInfo withStatus(TaskStatus status)
  {
    return new TaskInfo(createdTime, status, task);
  }
}
