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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.HashMap;
import java.util.Map;

/**
 * TaskReport objects contain additional information about an indexing task, such as row statistics, errors, and
 * published segments. They are kept in deep storage along with task logs.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "ingestionStatsAndErrors", value = IngestionStatsAndErrorsTaskReport.class)
})
public interface TaskReport
{
  String getTaskId();

  String getReportKey();

  /**
   * @return A JSON-serializable Object that contains a TaskReport's information
   */
  Object getPayload();

  static Map<String, TaskReport> buildTaskReports(TaskReport... taskReports)
  {
    Map<String, TaskReport> taskReportMap = new HashMap<>();
    for (TaskReport taskReport : taskReports) {
      taskReportMap.put(taskReport.getReportKey(), taskReport);
    }
    return taskReportMap;
  }
}
