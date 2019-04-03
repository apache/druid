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

package org.apache.druid.testing.clients;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.TaskState;
import org.joda.time.DateTime;

public class TaskResponseObject
{

  private final String id;
  private final String type;
  private final DateTime createdTime;
  private final DateTime queueInsertionTime;
  private final TaskState status;

  @JsonCreator
  private TaskResponseObject(
      @JsonProperty("id") String id,
      @JsonProperty("type") String type,
      @JsonProperty("createdTime") DateTime createdTime,
      @JsonProperty("queueInsertionTime") DateTime queueInsertionTime,
      @JsonProperty("status") TaskState status
  )
  {
    this.id = id;
    this.type = type;
    this.createdTime = createdTime;
    this.queueInsertionTime = queueInsertionTime;
    this.status = status;
  }

  @SuppressWarnings("unused") // Used by Jackson serialization?
  public String getId()
  {
    return id;
  }

  @SuppressWarnings("unused") // Used by Jackson serialization?
  public String getType()
  {
    return type;
  }

  @SuppressWarnings("unused") // Used by Jackson serialization?
  public DateTime getCreatedTime()
  {
    return createdTime;
  }

  @SuppressWarnings("unused") // Used by Jackson serialization?
  public DateTime getQueueInsertionTime()
  {
    return queueInsertionTime;
  }

  @SuppressWarnings("unused") // Used by Jackson serialization?
  public TaskState getStatus()
  {
    return status;
  }
}
