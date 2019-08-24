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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

@JsonTypeName("KinesisTuningConfig")
public class KinesisIndexTaskTuningConfig extends SeekableStreamIndexTaskTuningConfig
{
  private static final int DEFAULT_RECORD_BUFFER_SIZE = 10000;
  private static final int DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT = 5000;
  private static final int DEFAULT_RECORD_BUFFER_FULL_WAIT = 5000;
  private static final int DEFAULT_FETCH_SEQUENCE_NUMBER_TIMEOUT = 20000;
  private static final int DEFAULT_MAX_RECORDS_PER_POLL = 100;

  private final int recordBufferSize;
  private final int recordBufferOfferTimeout;
  private final int recordBufferFullWait;
  private final int fetchSequenceNumberTimeout;
  private final Integer fetchThreads;
  private final int maxRecordsPerPoll;

  @JsonCreator
  public KinesisIndexTaskTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
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
      @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod
  )
  {
    super(
        maxRowsInMemory,
        maxBytesInMemory,
        maxRowsPerSegment,
        maxTotalRows,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        indexSpecForIntermediatePersists,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        skipSequenceNumberAvailabilityCheck,
        segmentWriteOutMediumFactory,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions
    );
    this.recordBufferSize = recordBufferSize == null ? DEFAULT_RECORD_BUFFER_SIZE : recordBufferSize;
    this.recordBufferOfferTimeout = recordBufferOfferTimeout == null
                                    ? DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT
                                    : recordBufferOfferTimeout;
    this.recordBufferFullWait = recordBufferFullWait == null ? DEFAULT_RECORD_BUFFER_FULL_WAIT : recordBufferFullWait;
    this.fetchSequenceNumberTimeout = fetchSequenceNumberTimeout
                                      == null ? DEFAULT_FETCH_SEQUENCE_NUMBER_TIMEOUT : fetchSequenceNumberTimeout;
    this.fetchThreads = fetchThreads; // we handle this being null later
    this.maxRecordsPerPoll = maxRecordsPerPoll == null ? DEFAULT_MAX_RECORDS_PER_POLL : maxRecordsPerPoll;

    Preconditions.checkArgument(
        !(super.isResetOffsetAutomatically() && super.isSkipSequenceNumberAvailabilityCheck()),
        "resetOffsetAutomatically cannot be used if skipSequenceNumberAvailabilityCheck=true"
    );
  }

  @JsonProperty
  public int getRecordBufferSize()
  {
    return recordBufferSize;
  }

  @JsonProperty
  public int getRecordBufferOfferTimeout()
  {
    return recordBufferOfferTimeout;
  }

  @JsonProperty
  public int getRecordBufferFullWait()
  {
    return recordBufferFullWait;
  }

  @JsonProperty
  public int getFetchSequenceNumberTimeout()
  {
    return fetchSequenceNumberTimeout;
  }

  @JsonProperty
  public Integer getFetchThreads()
  {
    return fetchThreads;
  }

  @JsonProperty
  public int getMaxRecordsPerPoll()
  {
    return maxRecordsPerPoll;
  }

  @Override
  public KinesisIndexTaskTuningConfig withBasePersistDirectory(File dir)
  {
    return new KinesisIndexTaskTuningConfig(
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        getMaxRowsPerSegment(),
        getMaxTotalRows(),
        getIntermediatePersistPeriod(),
        dir,
        getMaxPendingPersists(),
        getIndexSpec(),
        getIndexSpecForIntermediatePersists(),
        true,
        isReportParseExceptions(),
        getHandoffConditionTimeout(),
        isResetOffsetAutomatically(),
        isSkipSequenceNumberAvailabilityCheck(),
        getRecordBufferSize(),
        getRecordBufferOfferTimeout(),
        getRecordBufferFullWait(),
        getFetchSequenceNumberTimeout(),
        getFetchThreads(),
        getSegmentWriteOutMediumFactory(),
        isLogParseExceptions(),
        getMaxParseExceptions(),
        getMaxSavedParseExceptions(),
        getMaxRecordsPerPoll(),
        getIntermediateHandoffPeriod()
    );
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
    if (!super.equals(o)) {
      return false;
    }
    KinesisIndexTaskTuningConfig that = (KinesisIndexTaskTuningConfig) o;
    return recordBufferSize == that.recordBufferSize &&
           recordBufferOfferTimeout == that.recordBufferOfferTimeout &&
           recordBufferFullWait == that.recordBufferFullWait &&
           fetchSequenceNumberTimeout == that.fetchSequenceNumberTimeout &&
           maxRecordsPerPoll == that.maxRecordsPerPoll &&
           Objects.equals(fetchThreads, that.fetchThreads);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        recordBufferSize,
        recordBufferOfferTimeout,
        recordBufferFullWait,
        fetchSequenceNumberTimeout,
        fetchThreads,
        maxRecordsPerPoll
    );
  }

  @Override
  public String toString()
  {
    return "KinesisIndexTaskTuningConfig{" +
           "maxRowsInMemory=" + getMaxRowsInMemory() +
           ", maxBytesInMemory=" + getMaxBytesInMemory() +
           ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
           ", maxTotalRows=" + getMaxTotalRows() +
           ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
           ", basePersistDirectory=" + getBasePersistDirectory() +
           ", maxPendingPersists=" + getMaxPendingPersists() +
           ", indexSpec=" + getIndexSpec() +
           ", reportParseExceptions=" + isReportParseExceptions() +
           ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
           ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
           ", skipSequenceNumberAvailabilityCheck=" + isSkipSequenceNumberAvailabilityCheck() +
           ", recordBufferSize=" + recordBufferSize +
           ", recordBufferOfferTimeout=" + recordBufferOfferTimeout +
           ", recordBufferFullWait=" + recordBufferFullWait +
           ", fetchSequenceNumberTimeout=" + fetchSequenceNumberTimeout +
           ", fetchThreads=" + fetchThreads +
           ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
           ", logParseExceptions=" + isLogParseExceptions() +
           ", maxParseExceptions=" + getMaxParseExceptions() +
           ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
           ", maxRecordsPerPoll=" + maxRecordsPerPoll +
           ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
           '}';
  }
}
