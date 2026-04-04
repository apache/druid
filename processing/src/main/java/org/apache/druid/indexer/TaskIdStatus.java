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

import org.joda.time.DateTime;

/**
 * Contains a {@link TaskIdentifier} and its {@link TaskStatus}.
 */
public class TaskIdStatus
{
  private final TaskIdentifier taskId;
  private final TaskStatus taskStatus;
  private final String dataSource;
  private final DateTime createdTime;

  public TaskIdStatus(
      TaskIdentifier taskId,
      TaskStatus taskStatus,
      String dataSource,
      DateTime createdTime
  )
  {
    this.taskId = taskId;
    this.taskStatus = taskStatus;
    this.dataSource = dataSource;
    this.createdTime = createdTime;
  }

  public TaskIdentifier getTaskIdentifier()
  {
    return taskId;
  }

  public TaskStatus getStatus()
  {
    return taskStatus;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }
}
