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

package org.apache.druid.indexing.pubsub.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

public class TaskReportData
{
  private final String id;
  private final DateTime startTime;
  private final Long remainingSeconds;
  private final TaskType type;

  public TaskReportData(
      String id,
      @Nullable DateTime startTime,
      Long remainingSeconds,
      TaskType type
  )
  {
    this.id = id;
    this.startTime = startTime;
    this.remainingSeconds = remainingSeconds;
    this.type = type;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  public DateTime getStartTime()
  {
    return startTime;
  }

  @JsonProperty
  public Long getRemainingSeconds()
  {
    return remainingSeconds;
  }

  @JsonProperty
  public TaskType getType()
  {
    return type;
  }

  @Override
  public String toString()
  {
    return "{" +
           "id='" + id + '\'' +
           ", startTime=" + startTime +
           ", remainingSeconds=" + remainingSeconds +
           '}';
  }

  /**
   * Used by the Supervisor to report status of tasks
   * ACTIVE - task is waiting to be started, started, or reading
   * PUBLISHING - task is publishing or registering handoff
   * UNNKNOWN - unknown
   */
  public enum TaskType
  {
    ACTIVE, PUBLISHING, UNKNOWN
  }
}
