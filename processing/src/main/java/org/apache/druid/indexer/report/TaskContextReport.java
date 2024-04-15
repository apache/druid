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

package org.apache.druid.indexer.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

public class TaskContextReport implements TaskReport
{
  public static final String REPORT_KEY = "taskContext";
  private final String taskId;
  @Nullable
  private final Map<String, Object> taskContext;

  @JsonCreator
  public TaskContextReport(
      @JsonProperty("taskId") final String taskId,
      @JsonProperty("payload") @Nullable final Map<String, Object> taskContext
  )
  {
    this.taskId = taskId;
    this.taskContext = taskContext;
  }

  @Override
  @JsonProperty
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
  @JsonProperty
  public Map<String, Object> getPayload()
  {
    return taskContext;
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
    TaskContextReport that = (TaskContextReport) o;
    return Objects.equals(taskId, that.taskId) && Objects.equals(taskContext, that.taskContext);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(taskId, taskContext);
  }
}
