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

package org.apache.druid.msq.dart.controller;

import org.apache.druid.indexer.report.TaskReport;
import org.apache.druid.msq.dart.controller.http.DartQueryInfo;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Object returned by {@link DartControllerRegistry#getQueryInfoAndReport(String)}.
 */
public class QueryInfoAndReport
{
  private final DartQueryInfo queryInfo;
  @Nullable
  private final TaskReport report;
  private final DateTime timestamp;

  public QueryInfoAndReport(DartQueryInfo queryInfo, @Nullable TaskReport report, DateTime timestamp)
  {
    this.queryInfo = queryInfo;
    this.report = report;
    this.timestamp = timestamp;
  }

  public DartQueryInfo getQueryInfo()
  {
    return queryInfo;
  }

  @Nullable
  public TaskReport getReport()
  {
    return report;
  }

  /**
   * Timestamp that the query details or report was last updated. Generally "now" for currently-running queries,
   * and the query finish time for completed queries.
   */
  public DateTime getTimestamp()
  {
    return timestamp;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryInfoAndReport that = (QueryInfoAndReport) o;
    return Objects.equals(queryInfo, that.queryInfo)
           && Objects.equals(report, that.report)
           && Objects.equals(timestamp, that.timestamp);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(queryInfo, report, timestamp);
  }

  @Override
  public String toString()
  {
    return "QueryInfoAndReport{" +
           "queryInfo=" + queryInfo +
           ", report=" + report +
           ", timestamp=" + timestamp +
           '}';
  }
}
