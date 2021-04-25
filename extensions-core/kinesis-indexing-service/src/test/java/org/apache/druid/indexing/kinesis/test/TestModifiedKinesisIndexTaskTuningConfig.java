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

package org.apache.druid.indexing.kinesis.test;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

@JsonTypeName("KinesisTuningConfig")
public class TestModifiedKinesisIndexTaskTuningConfig extends KinesisIndexTaskTuningConfig
{
  private final String extra;

  @JsonCreator
  public TestModifiedKinesisIndexTaskTuningConfig(
      @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
      @JsonProperty("skipBytesInMemoryOverheadCheck") @Nullable Boolean skipBytesInMemoryOverheadCheck,
      @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
      @JsonProperty("maxTotalRows") Long maxTotalRows,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("resetOffsetAutomatically") Boolean resetOffsetAutomatically,
      @JsonProperty("skipSequenceNumberAvailabilityCheck") Boolean skipSequenceNumberAvailabilityCheck,
      @JsonProperty("recordBufferSize") Integer recordBufferSize,
      @JsonProperty("recordBufferOfferTimeout") Integer recordBufferOfferTimeout,
      @JsonProperty("recordBufferFullWait") Integer recordBufferFullWait,
      @JsonProperty("fetchSequenceNumberTimeout") Integer fetchSequenceNumberTimeout,
      @JsonProperty("fetchThreads") Integer fetchThreads,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("maxRecordsPerPoll") @Nullable Integer maxRecordsPerPoll,
      @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod,
      @JsonProperty("extra") String extra
  )
  {
    super(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        maxRowsPerSegment,
        maxTotalRows,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        indexSpecForIntermediatePersists,
        buildV9Directly,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        skipSequenceNumberAvailabilityCheck,
        recordBufferSize,
        recordBufferOfferTimeout,
        recordBufferFullWait,
        fetchSequenceNumberTimeout,
        fetchThreads,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        maxRecordsPerPoll,
        intermediateHandoffPeriod
    );
    this.extra = extra;
  }

  public TestModifiedKinesisIndexTaskTuningConfig(KinesisIndexTaskTuningConfig base, String extra)
  {
    super(
        base.getAppendableIndexSpec(),
        base.getMaxRowsInMemory(),
        base.getMaxBytesInMemory(),
        base.isSkipBytesInMemoryOverheadCheck(),
        base.getMaxRowsPerSegment(),
        base.getMaxTotalRows(),
        base.getIntermediatePersistPeriod(),
        base.getBasePersistDirectory(),
        base.getMaxPendingPersists(),
        base.getIndexSpec(),
        base.getIndexSpecForIntermediatePersists(),
        base.getBuildV9Directly(),
        base.isReportParseExceptions(),
        base.getHandoffConditionTimeout(),
        base.isResetOffsetAutomatically(),
        base.isSkipSequenceNumberAvailabilityCheck(),
        base.getRecordBufferSize(),
        base.getRecordBufferOfferTimeout(),
        base.getRecordBufferFullWait(),
        base.getFetchSequenceNumberTimeout(),
        base.getFetchThreads(),
        base.getSegmentWriteOutMediumFactory(),
        base.isLogParseExceptions(),
        base.getMaxParseExceptions(),
        base.getMaxSavedParseExceptions(),
        base.getMaxRecordsPerPoll(),
        base.getIntermediateHandoffPeriod()
    );
    this.extra = extra;
  }

  @JsonProperty("extra")
  public String getExtra()
  {
    return extra;
  }
}
