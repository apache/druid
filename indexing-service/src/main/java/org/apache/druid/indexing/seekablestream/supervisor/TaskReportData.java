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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Map;

public class TaskReportData<partitionType, sequenceType>
{
  private final String id;
  private final Map<partitionType, sequenceType> startingOffsets;
  private final DateTime startTime;
  private final Long remainingSeconds;
  private final TaskType type;
  private Map<partitionType, sequenceType> currentOffsets;
  private final Map<partitionType, sequenceType> lag;

  public TaskReportData(
      String id,
      @Nullable Map<partitionType, sequenceType> startingOffsets,
      @Nullable Map<partitionType, sequenceType> currentOffsets,
      @Nullable DateTime startTime,
      Long remainingSeconds,
      TaskType type,
      @Nullable Map<partitionType, sequenceType> lag
  )
  {
    this.id = id;
    this.startingOffsets = startingOffsets;
    this.currentOffsets = currentOffsets;
    this.startTime = startTime;
    this.remainingSeconds = remainingSeconds;
    this.type = type;
    this.lag = lag;
  }

  @JsonProperty
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<partitionType, sequenceType> getStartingOffsets()
  {
    return startingOffsets;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<partitionType, sequenceType> getCurrentOffsets()
  {
    return currentOffsets;
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

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Map<partitionType, sequenceType> getLag()
  {
    return lag;
  }

  public void setCurrentSequenceNumbers(Map<partitionType, sequenceType> currentOffsets)
  {
    this.currentOffsets = currentOffsets;
  }

  @Override
  public String toString()
  {
    return "{" +
           "id='" + id + '\'' +
           (startingOffsets != null ? ", startingOffsets=" + startingOffsets : "") +
           (currentOffsets != null ? ", currentOffsets=" + currentOffsets : "") +
           ", startTime=" + startTime +
           ", remainingSeconds=" + remainingSeconds +
           (lag != null ? ", lag=" + lag : "") +
           '}';
  }

  public enum TaskType
  {
    ACTIVE, PUBLISHING, UNKNOWN
  }
}
