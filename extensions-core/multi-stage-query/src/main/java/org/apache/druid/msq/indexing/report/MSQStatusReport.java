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
import java.util.Queue;

public class MSQStatusReport
{
  private final TaskState status;

  @Nullable
  private final MSQErrorReport errorReport;

  private final Queue<MSQErrorReport> warningReports;

  @Nullable
  private final DateTime startTime;

  private final long durationMs;


  @JsonCreator
  public MSQStatusReport(
      @JsonProperty("status") TaskState status,
      @JsonProperty("error") @Nullable MSQErrorReport errorReport,
      @JsonProperty("warnings") Queue<MSQErrorReport> warningReports,
      @JsonProperty("startTime") @Nullable DateTime startTime,
      @JsonProperty("durationMs") long durationMs
  )
  {
    this.status = Preconditions.checkNotNull(status, "status");
    this.errorReport = errorReport;
    this.warningReports = warningReports;
    this.startTime = Preconditions.checkNotNull(startTime, "startTime");
    this.durationMs = durationMs;

  }

  @JsonProperty
  public TaskState getStatus()
  {
    return status;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public MSQErrorReport getErrorReport()
  {
    return errorReport;
  }

  @JsonProperty
  public Queue<MSQErrorReport> getWarningReports()
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
}
