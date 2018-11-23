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

package org.apache.druid.indexing.common;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

import java.util.Objects;

@JsonTypeName("ingestionStatsAndErrors")
public class IngestionStatsAndErrorsTaskReport implements TaskReport
{
  public static final String REPORT_KEY = "ingestionStatsAndErrors";

  @JsonProperty
  private String taskId;

  @JsonProperty
  private IngestionStatsAndErrorsTaskReportData payload;

  public IngestionStatsAndErrorsTaskReport(
      @JsonProperty("taskId") String taskId,
      @JsonProperty("payload") IngestionStatsAndErrorsTaskReportData payload
  )
  {
    this.taskId = taskId;
    this.payload = payload;
  }

  @Override
  public String getTaskId()
  {
    return taskId;
  }

  @Override
  public String getReportKey()
  {
    return REPORT_KEY;
  }

  @Override
  public Object getPayload()
  {
    return payload;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IngestionStatsAndErrorsTaskReport that = (IngestionStatsAndErrorsTaskReport) o;
    return Objects.equals(getTaskId(), that.getTaskId()) &&
           Objects.equals(getPayload(), that.getPayload());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getTaskId(), getPayload());
  }

  @Override
  public String toString()
  {
    return "IngestionStatsAndErrorsTaskReport{" +
           "taskId='" + taskId + '\'' +
           ", payload=" + payload +
           '}';
  }

  // TaskReports are put into a Map and serialized.
  // Jackson doesn't normally serialize the TaskReports with a "type" field in that situation,
  // so explictly serialize the "type" field (otherwise, deserialization fails).
  @JsonProperty("type")
  private String getType()
  {
    return "ingestionStatsAndErrors";
  }
}
