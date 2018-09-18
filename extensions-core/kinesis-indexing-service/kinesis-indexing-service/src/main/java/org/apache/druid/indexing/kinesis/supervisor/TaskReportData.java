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

package org.apache.druid.indexing.kinesis.supervisor;

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
  private final Map<String, String> startingSequenceNumbers;
  private final DateTime startTime;
  private final Long remainingSeconds;
  private final TaskType type;
  private Map<String, String> currentSequenceNumbers;

  public TaskReportData(
      String id,
      Map<String, String> startingSequenceNumbers,
      Map<String, String> currentSequenceNumbers,
      DateTime startTime,
      Long remainingSeconds,
      TaskType type
  )
  {
    this.id = id;
    this.startingSequenceNumbers = startingSequenceNumbers;
    this.currentSequenceNumbers = currentSequenceNumbers;
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
  public Map<String, String> getStartingSequenceNumbers()
  {
    return startingSequenceNumbers;
  }

  @JsonProperty
  public Map<String, String> getCurrentSequenceNumbers()
  {
    return currentSequenceNumbers;
  }

  public void setCurrentSequenceNumbers(Map<String, String> currentSequenceNumbers)
  {
    this.currentSequenceNumbers = currentSequenceNumbers;
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
           (startingSequenceNumbers != null ? ", startingSequenceNumbers=" + startingSequenceNumbers : "") +
           (currentSequenceNumbers != null ? ", currentSequenceNumbers=" + currentSequenceNumbers : "") +
           ", startTime=" + startTime +
           ", remainingSeconds=" + remainingSeconds +
           '}';
  }
}
