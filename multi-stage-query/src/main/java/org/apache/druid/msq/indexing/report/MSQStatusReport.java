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
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.msq.exec.SegmentLoadStatusFetcher;
import org.apache.druid.msq.exec.WorkerStats;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.msq.indexing.error.MSQFaultUtils;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

  private final Map<Integer, List<WorkerStats>> workerStats;

  private final int pendingTasks;

  private final int runningTasks;

  @Nullable
  private final SegmentLoadStatusFetcher.SegmentLoadWaiterStatus segmentLoadWaiterStatus;

  @Nullable
  private final MSQSegmentReport segmentReport;

  @JsonCreator
  public MSQStatusReport(
      @JsonProperty("status") TaskState status,
      @JsonProperty("errorReport") @Nullable MSQErrorReport errorReport,
      @JsonProperty("warnings") Collection<MSQErrorReport> warningReports,
      @JsonProperty("startTime") @Nullable DateTime startTime,
      @JsonProperty("durationMs") long durationMs,
      @JsonProperty("workers") Map<Integer, List<WorkerStats>> workerStats,
      @JsonProperty("pendingTasks") int pendingTasks,
      @JsonProperty("runningTasks") int runningTasks,
      @JsonProperty("segmentLoadWaiterStatus") @Nullable
      SegmentLoadStatusFetcher.SegmentLoadWaiterStatus segmentLoadWaiterStatus,
      @JsonProperty("segmentReport") @Nullable MSQSegmentReport segmentReport
  )
  {
    this.status = Preconditions.checkNotNull(status, "status");
    this.errorReport = errorReport;
    this.warningReports = warningReports != null ? warningReports : Collections.emptyList();
    this.startTime = startTime;
    this.durationMs = durationMs;
    this.workerStats = workerStats;
    this.pendingTasks = pendingTasks;
    this.runningTasks = runningTasks;
    this.segmentLoadWaiterStatus = segmentLoadWaiterStatus;
    this.segmentReport = segmentReport;
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
  public int getPendingTasks()
  {
    return pendingTasks;
  }

  @JsonProperty
  public int getRunningTasks()
  {
    return runningTasks;
  }

  @JsonProperty
  public long getDurationMs()
  {
    return durationMs;
  }

  @JsonProperty("workers")
  public Map<Integer, List<WorkerStats>> getWorkerStats()
  {
    return workerStats;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public SegmentLoadStatusFetcher.SegmentLoadWaiterStatus getSegmentLoadWaiterStatus()
  {
    return segmentLoadWaiterStatus;
  }

  @JsonProperty("segmentReport")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  @Nullable
  public MSQSegmentReport getSegmentReport()
  {
    return segmentReport;
  }

  /**
   * Returns a {@link TaskStatus} appropriate for this status report.
   */
  public TaskStatus toTaskStatus(final String taskId)
  {
    if (status == TaskState.SUCCESS) {
      return TaskStatus.success(taskId);
    } else {
      // Error report is nonnull when status code != SUCCESS. Use that message.
      return TaskStatus.failure(
          taskId,
          MSQFaultUtils.generateMessageWithErrorCode(errorReport.getFault())
      );
    }
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
