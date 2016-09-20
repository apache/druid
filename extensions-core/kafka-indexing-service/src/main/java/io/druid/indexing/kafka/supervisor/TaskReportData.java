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

package io.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.joda.time.DateTime;

import java.util.Map;

public class TaskReportData
{
  public enum TaskType
  {
    ACTIVE, PUBLISHING, UNKNOWN
  }

  private final String id;
  private final Map<Integer, Long> startingOffsets;
  private final DateTime startTime;
  private final Long remainingSeconds;
  private final TaskType type;
  private Map<Integer, Long> currentOffsets;

  public TaskReportData(
      String id,
      Map<Integer, Long> startingOffsets,
      Map<Integer, Long> currentOffsets,
      DateTime startTime,
      Long remainingSeconds,
      TaskType type
  )
  {
    this.id = id;
    this.startingOffsets = startingOffsets;
    this.currentOffsets = currentOffsets;
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
  public Map<Integer, Long> getStartingOffsets()
  {
    return startingOffsets;
  }

  @JsonProperty
  public Map<Integer, Long> getCurrentOffsets()
  {
    return currentOffsets;
  }

  public void setCurrentOffsets(Map<Integer, Long> currentOffsets)
  {
    this.currentOffsets = currentOffsets;
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
           (startingOffsets != null ? ", startingOffsets=" + startingOffsets : "") +
           (currentOffsets != null ? ", currentOffsets=" + currentOffsets : "") +
           ", startTime=" + startTime +
           ", remainingSeconds=" + remainingSeconds +
           '}';
  }
}
