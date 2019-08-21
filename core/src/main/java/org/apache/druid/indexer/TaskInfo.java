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
import org.joda.time.DateTime;

import javax.annotation.Nullable;

/**
 * This class is used to store task info from runner query and cache in OverlordResource
 */
public class TaskInfo<EntryType, StatusType>
{
  private final String id;
  private final DateTime createdTime;
  private final StatusType status;
  private final String dataSource;
  @Nullable
  private final EntryType task;

  public TaskInfo(
      String id,
      DateTime createdTime,
      StatusType status,
      String dataSource,
      @Nullable EntryType task
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.createdTime = Preconditions.checkNotNull(createdTime, "createdTime");
    this.status = Preconditions.checkNotNull(status, "status");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.task = task;
  }

  public String getId()
  {
    return id;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public StatusType getStatus()
  {
    return status;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  @Nullable
  public EntryType getTask()
  {
    return task;
  }
}

