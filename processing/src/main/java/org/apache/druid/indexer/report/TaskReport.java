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

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.LinkedHashMap;
import java.util.Optional;

/**
 * TaskReport objects contain additional information about an indexing task, such as row statistics, errors, and
 * published segments. They are kept in deep storage along with task logs.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(
        name = IngestionStatsAndErrorsTaskReport.REPORT_KEY,
        value = IngestionStatsAndErrorsTaskReport.class
    ),
    @JsonSubTypes.Type(name = KillTaskReport.REPORT_KEY, value = KillTaskReport.class),
    @JsonSubTypes.Type(name = TaskContextReport.REPORT_KEY, value = TaskContextReport.class)
})
public interface TaskReport
{
  String getTaskId();

  String getReportKey();

  /**
   * @return A JSON-serializable Object that contains a TaskReport's information
   */
  Object getPayload();

  /**
   * Returns an order-preserving map that is suitable for passing into {@link TaskReportFileWriter#write}.
   */
  static ReportMap buildTaskReports(TaskReport... taskReports)
  {
    ReportMap taskReportMap = new ReportMap();
    for (TaskReport taskReport : taskReports) {
      taskReportMap.put(taskReport.getReportKey(), taskReport);
    }
    return taskReportMap;
  }

  /**
   * Represents an ordered map from report key to a TaskReport that is compatible
   * for writing out reports to files or serving over HTTP.
   * <p>
   * This class is needed for Jackson serde to work correctly. Without this class,
   * a TaskReport is serialized without the type information and cannot be
   * deserialized back into a concrete implementation.
   */
  class ReportMap extends LinkedHashMap<String, TaskReport>
  {
    @SuppressWarnings("unchecked")
    public <T extends TaskReport> Optional<T> findReport(String reportKey)
    {
      return Optional.ofNullable((T) get(reportKey));
    }
  }
}
