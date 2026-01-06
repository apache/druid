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

package org.apache.druid.sql.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.report.TaskReport;

import java.util.Objects;

/**
 * Class returned by {@link SqlResource#doGetQueryReport}, the "get query report" API.
 */
public class GetReportResponse
{
  private final QueryInfo queryInfo;
  private final TaskReport.ReportMap reportMap;

  @JsonCreator
  public GetReportResponse(
      @JsonProperty("query") QueryInfo queryInfo,
      @JsonProperty("report") TaskReport.ReportMap reportMap
  )
  {
    this.queryInfo = queryInfo;
    this.reportMap = reportMap;
  }

  @JsonProperty("query")
  public QueryInfo getQueryInfo()
  {
    return queryInfo;
  }

  @JsonProperty("report")
  public TaskReport.ReportMap getReportMap()
  {
    return reportMap;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GetReportResponse that = (GetReportResponse) o;
    return Objects.equals(queryInfo, that.queryInfo) && Objects.equals(reportMap, that.reportMap);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryInfo, reportMap);
  }

  @Override
  public String toString()
  {
    return "GetReportResponse{" +
           "queryInfo=" + queryInfo +
           ", report=" + reportMap +
           '}';
  }
}
