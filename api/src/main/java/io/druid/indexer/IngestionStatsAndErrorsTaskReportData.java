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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Objects;

public class IngestionStatsAndErrorsTaskReportData
{
  @JsonProperty
  private IngestionState ingestionState;

  @JsonProperty
  private Map<String, Object> unparseableEvents;

  @JsonProperty
  private Map<String, Object> rowStats;

  public IngestionStatsAndErrorsTaskReportData(
      @JsonProperty("ingestionState") IngestionState ingestionState,
      @JsonProperty("unparseableEvents") Map<String, Object> unparseableEvents,
      @JsonProperty("rowStats") Map<String, Object> rowStats
  )
  {
    this.ingestionState = ingestionState;
    this.unparseableEvents = unparseableEvents;
    this.rowStats = rowStats;
  }

  @JsonProperty
  public IngestionState getIngestionState()
  {
    return ingestionState;
  }

  @JsonProperty
  public Map<String, Object> getUnparseableEvents()
  {
    return unparseableEvents;
  }

  @JsonProperty
  public Map<String, Object> getRowStats()
  {
    return rowStats;
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
    IngestionStatsAndErrorsTaskReportData that = (IngestionStatsAndErrorsTaskReportData) o;
    return getIngestionState() == that.getIngestionState() &&
           Objects.equals(getUnparseableEvents(), that.getUnparseableEvents()) &&
           Objects.equals(getRowStats(), that.getRowStats());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getIngestionState(), getUnparseableEvents(), getRowStats());
  }

  public static IngestionStatsAndErrorsTaskReportData getPayloadFromTaskReports(
      Map<String, TaskReport> taskReports
  )
  {
    return (IngestionStatsAndErrorsTaskReportData) taskReports.get(IngestionStatsAndErrorsTaskReport.REPORT_KEY)
                                                              .getPayload();
  }

  @Override
  public String toString()
  {
    return "IngestionStatsAndErrorsTaskReportData{" +
           "ingestionState=" + ingestionState +
           ", unparseableEvents=" + unparseableEvents +
           ", rowStats=" + rowStats +
           '}';
  }
}
