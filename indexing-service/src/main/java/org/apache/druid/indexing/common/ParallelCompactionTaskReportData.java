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
import org.apache.druid.indexer.IngestionState;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

/**
 * This class has fields specific to the parallel compaction task and hence extends the generic
 * task report data {@link IngestionStatsAndErrorsTaskReportData}
 */
public class ParallelCompactionTaskReportData extends IngestionStatsAndErrorsTaskReportData
{
  @JsonProperty
  private Long segmentsRead;
  @JsonProperty
  private Long segmentsPublished;

  public ParallelCompactionTaskReportData(
      @JsonProperty("ingestionState") IngestionState ingestionState,
      @JsonProperty("unparseableEvents") Map<String, Object> unparseableEvents,
      @JsonProperty("rowStats") Map<String, Object> rowStats,
      @JsonProperty("errorMsg") @Nullable String errorMsg,
      @JsonProperty("segmentAvailabilityConfirmed") boolean segmentAvailabilityConfirmed,
      @JsonProperty("segmentAvailabilityWaitTimeMs") long segmentAvailabilityWaitTimeMs,
      @JsonProperty("recordsProcessed") Map<String, Long> recordsProcessed,
      @JsonProperty("segmentsRead") Long segmentsRead,
      @JsonProperty("segmentsPublished") Long segmentsPublished
  )
  {
    super(
        ingestionState,
        unparseableEvents,
        rowStats,
        errorMsg,
        segmentAvailabilityConfirmed,
        segmentAvailabilityWaitTimeMs,
        recordsProcessed
    );
    this.segmentsRead = segmentsRead;
    this.segmentsPublished = segmentsPublished;
  }

  @JsonProperty
  @Nullable
  public Long getSegmentsRead()
  {
    return segmentsRead;
  }

  @JsonProperty
  @Nullable
  public Long getSegmentsPublished()
  {
    return segmentsPublished;
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
    ParallelCompactionTaskReportData that = (ParallelCompactionTaskReportData) o;
    return super.equals(o) &&
           Objects.equals(getSegmentsRead(), that.getSegmentsRead()) &&
           Objects.equals(getSegmentsPublished(), that.getSegmentsPublished());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        getSegmentsRead(),
        getSegmentsPublished()
    );
  }

  @Override
  public String toString()
  {
    return "ParallelCompactionTaskReportData {" +
           "IngestionStatsAndErrorsTaskReportData=" + super.toString() +
           ", segmentsRead=" + segmentsRead +
           ", segmentsPublished=" + segmentsPublished +
           '}';
  }
}
