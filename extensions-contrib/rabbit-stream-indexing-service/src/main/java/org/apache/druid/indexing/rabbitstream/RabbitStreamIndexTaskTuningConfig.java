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

package org.apache.druid.indexing.rabbitstream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.primitives.Ints;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;

import java.io.File;

public class RabbitStreamIndexTaskTuningConfig extends SeekableStreamIndexTaskTuningConfig
{
  private static final int DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT = 5000;
  private static final int DEFAULT_MAX_RECORDS_PER_POLL = 100;


  static final int ASSUMED_RECORD_SIZE = 10_000;

  private static final double RECORD_BUFFER_MEMORY_MAX_HEAP_FRACTION = 0.1;

  private static final int MAX_RECORD_BUFFER_MEMORY = 100_000_000;


  private final Integer recordBufferSize;
  private final int recordBufferOfferTimeout;

  private final Integer maxRecordsPerPoll;

  public RabbitStreamIndexTaskTuningConfig(
      @Nullable AppendableIndexSpec appendableIndexSpec,
      @Nullable Integer maxRowsInMemory,
      @Nullable Long maxBytesInMemory,
      @Nullable Boolean skipBytesInMemoryOverheadCheck,
      @Nullable Integer maxRowsPerSegment,
      @Nullable Long maxTotalRows,
      @Nullable Period intermediatePersistPeriod,
      @Nullable File basePersistDirectory,
      @Nullable Integer maxPendingPersists,
      @Nullable IndexSpec indexSpec,
      @Nullable IndexSpec indexSpecForIntermediatePersists,
      @Nullable Boolean reportParseExceptions,
      @Nullable Long handoffConditionTimeout,
      @Nullable Boolean resetOffsetAutomatically,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @Nullable Period intermediateHandoffPeriod,
      @Nullable Boolean logParseExceptions,
      @Nullable Integer maxParseExceptions,
      @Nullable Integer maxSavedParseExceptions,
      @Nullable Integer numPersistThreads,
      @Nullable Integer recordBufferSize,
      @Nullable Integer recordBufferOfferTimeout,
      @Nullable Integer maxRecordsPerPoll)
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
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        false,
        segmentWriteOutMediumFactory,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numPersistThreads
    );

    this.recordBufferSize = recordBufferSize;
    this.recordBufferOfferTimeout = recordBufferOfferTimeout == null
                                    ? DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT
                                    : recordBufferOfferTimeout;
    this.maxRecordsPerPoll = maxRecordsPerPoll;
  }

  @JsonCreator
  private RabbitStreamIndexTaskTuningConfig(
      @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("skipBytesInMemoryOverheadCheck") @Nullable Boolean skipBytesInMemoryOverheadCheck,
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("intermediatePersistPeriod") @Nullable Period intermediatePersistPeriod,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      @Deprecated @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") @Nullable Long handoffConditionTimeout,
      @JsonProperty("recordBufferSize") Integer recordBufferSize,
      @JsonProperty("recordBufferOfferTimeout") Integer recordBufferOfferTimeout,
      @JsonProperty("resetOffsetAutomatically") @Nullable Boolean resetOffsetAutomatically,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("numPersistThreads") @Nullable Integer numPersistThreads,
      @JsonProperty("maxRecordsPerPoll") @Nullable Integer maxRecordsPerPoll
  )
  {
    this(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        maxRowsPerSegment,
        maxTotalRows,
        intermediatePersistPeriod,
        null,
        maxPendingPersists,
        indexSpec,
        indexSpecForIntermediatePersists,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        segmentWriteOutMediumFactory,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numPersistThreads,
        recordBufferSize,
        recordBufferOfferTimeout,
        maxRecordsPerPoll);
  }

  @Nullable
  @JsonProperty("recordBufferSize")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getRecordBufferSizeConfigured()
  {
    return recordBufferSize;
  }

  @JsonProperty
  public int getRecordBufferOfferTimeout()
  {
    return recordBufferOfferTimeout;
  }

  @Nullable
  @JsonProperty("maxRecordsPerPoll")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getMaxRecordsPerPollConfigured()
  {
    return maxRecordsPerPoll;
  }

  public int getRecordBufferSizeOrDefault(final long maxHeapSize)
  {
    if (recordBufferSize != null) {
      return recordBufferSize;
    } else {
      final long memoryToUse = Math.min(
          MAX_RECORD_BUFFER_MEMORY,
          (long) (maxHeapSize * RECORD_BUFFER_MEMORY_MAX_HEAP_FRACTION)
      );

      return Ints.checkedCast(Math.max(1, memoryToUse / ASSUMED_RECORD_SIZE));
    }
  }

  public int getMaxRecordsPerPollOrDefault()
  {
    return (this.maxRecordsPerPoll != null) ? this.maxRecordsPerPoll : DEFAULT_MAX_RECORDS_PER_POLL;
  }

  @Override
  public RabbitStreamIndexTaskTuningConfig withBasePersistDirectory(File dir)
  {
    return new RabbitStreamIndexTaskTuningConfig(
        getAppendableIndexSpec(),
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        isSkipBytesInMemoryOverheadCheck(),
        getMaxRowsPerSegment(),
        getMaxTotalRows(),
        getIntermediatePersistPeriod(),
        dir,
        getMaxPendingPersists(),
        getIndexSpec(),
        getIndexSpecForIntermediatePersists(),
        isReportParseExceptions(),
        getHandoffConditionTimeout(),
        isResetOffsetAutomatically(),
        getSegmentWriteOutMediumFactory(),
        getIntermediateHandoffPeriod(),
        isLogParseExceptions(),
        getMaxParseExceptions(),
        getMaxSavedParseExceptions(),
        getNumPersistThreads(),
        getRecordBufferSizeConfigured(),
        getRecordBufferOfferTimeout(),
        getMaxRecordsPerPollConfigured()
        );
  }

  @Override
  public String toString()
  {
    return "RabbitStreamIndexTaskTuningConfig{" +
        "maxRowsInMemory=" + getMaxRowsInMemory() +
        ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
        ", maxTotalRows=" + getMaxTotalRows() +
        ", maxBytesInMemory=" + getMaxBytesInMemory() +
        ", skipBytesInMemoryOverheadCheck=" + isSkipBytesInMemoryOverheadCheck() +
        ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
        ", maxPendingPersists=" + getMaxPendingPersists() +
        ", indexSpec=" + getIndexSpec() +
        ", indexSpecForIntermediatePersists=" + getIndexSpecForIntermediatePersists() +
        ", reportParseExceptions=" + isReportParseExceptions() +
        ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
        ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
        ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
        ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
        ", logParseExceptions=" + isLogParseExceptions() +
        ", maxParseExceptions=" + getMaxParseExceptions() +
        ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
        ", numPersistThreads=" + getNumPersistThreads() +
        ", maxRecordsPerPole=" + getMaxRecordsPerPollConfigured() +
        '}';
  }

}
