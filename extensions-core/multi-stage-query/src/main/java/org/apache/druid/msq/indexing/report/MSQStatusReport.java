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

package org.apache.druid.msq.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

public class MSQStatusReport
{
  private final TaskState status;

  @Nullable
  private final MSQErrorReport errorReport;

  private final Collection<MSQErrorReport> warningReports;

  @Nullable
  private final DateTime startTime;

  private final long durationMs;


  @JsonCreator
  public MSQStatusReport(
      @JsonProperty("status") TaskState status,
      @JsonProperty("errorReport") @Nullable MSQErrorReport errorReport,
      @JsonProperty("warnings") Collection<MSQErrorReport> warningReports,
      @JsonProperty("startTime") @Nullable DateTime startTime,
      @JsonProperty("durationMs") long durationMs
  )
  {
    this.status = Preconditions.checkNotNull(status, "status");
    this.errorReport = errorReport;
    this.warningReports = warningReports != null ? warningReports : Collections.emptyList();
    this.startTime = startTime;
    this.durationMs = durationMs;
  }

  @JsonProperty
  public TaskState getStatus()
  {
    return status;
  }

  @Nullable
  @JsonProperty("errorReport")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public MSQErrorReport getErrorReport()
  {
    return errorReport;
  }

  @JsonProperty("warnings")
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Collection<MSQErrorReport> getWarningReports()
  {
    return warningReports;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public DateTime getStartTime()
  {
    return startTime;
  }

  @JsonProperty
  public long getDurationMs()
  {
    return durationMs;
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
    MSQStatusReport that = (MSQStatusReport) o;
    return durationMs == that.durationMs
           && status == that.status
           && Objects.equals(errorReport, that.errorReport)
           && Objects.equals(warningReports, that.warningReports)
           && Objects.equals(startTime, that.startTime);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(status, errorReport, warningReports, startTime, durationMs);
  }

  @Override
  public String toString()
  {
    return "MSQStatusReport{" +
           "status=" + status +
           ", errorReport=" + errorReport +
           ", warningReports=" + warningReports +
           ", startTime=" + startTime +
           ", durationMs=" + durationMs +
           '}';
  }
}
