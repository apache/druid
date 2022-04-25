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
 * This class is used to store task info that is only necessary to some status queries in the OverlordResource
 */
public class TaskInfoLite
{
  private final String id;
  private final String groupId;
  private final String type;
  private final String dataSource;
  private final TaskLocation location;
  private final DateTime createdTime;
  private final String status;
  private final Long duration;
  private final @Nullable String errorMsg;

  public TaskInfoLite(
      String id,
      String groupId,
      String type,
      String dataSource,
      TaskLocation location,
      DateTime createdTime,
      String status,
      Long duration,
      @Nullable String errorMsg
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.groupId = Preconditions.checkNotNull(groupId, "groupId");
    this.type = Preconditions.checkNotNull(type, "type");
    this.dataSource = Preconditions.checkNotNull(dataSource, "dataSource");
    this.location = Preconditions.checkNotNull(location, "location");
    this.createdTime = Preconditions.checkNotNull(createdTime, "createdTime");
    this.status = Preconditions.checkNotNull(status, "status");
    this.duration = Preconditions.checkNotNull(duration, "duration");
    this.errorMsg = errorMsg;
  }

  public String getId()
  {
    return id;
  }

  public String getGroupId()
  {
    return groupId;
  }

  public String getType()
  {
    return type;
  }

  public String getDataSource()
  {
    return dataSource;
  }

  public TaskLocation getLocation()
  {
    return location;
  }

  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  public TaskState getStatus()
  {
    switch(status) {
      case "SUCCESS":
        return TaskState.SUCCESS;
      case "FAILED":
        return TaskState.FAILED;
      case "RUNNING":
      default:
        return TaskState.RUNNING;
    }
  }

  public Long getDuration()
  {
    return duration;
  }

  public String getErrorMsg() {
    return errorMsg;
  }
}