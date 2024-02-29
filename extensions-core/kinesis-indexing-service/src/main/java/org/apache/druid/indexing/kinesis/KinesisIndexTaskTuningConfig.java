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
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

@JsonTypeName("KinesisTuningConfig")
public class KinesisIndexTaskTuningConfig extends SeekableStreamIndexTaskTuningConfig
{
  // Assumed record buffer size is larger when dealing with aggregated messages, because aggregated messages tend to
  // be larger, up to 1MB in size.
  static final int ASSUMED_RECORD_SIZE = 10_000;
  static final int ASSUMED_RECORD_SIZE_AGGREGATE = 1_000_000;

  /**
   * Together with {@link KinesisIndexTaskIOConfig#MAX_RECORD_FETCH_MEMORY}, don't take up more than 200MB per task.
   */
  private static final int MAX_RECORD_BUFFER_MEMORY = 100_000_000;

  /**
   * Together with {@link KinesisIndexTaskIOConfig#RECORD_FETCH_MEMORY_MAX_HEAP_FRACTION}, don't take up more
   * than 15% of the heap per task.
   */
  private static final double RECORD_BUFFER_MEMORY_MAX_HEAP_FRACTION = 0.1;

  private static final int DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT = 5000;
  private static final int DEFAULT_RECORD_BUFFER_FULL_WAIT = 5000;
  private static final int DEFAULT_MAX_RECORDS_PER_POLL = 100;
  private static final int DEFAULT_MAX_BYTES_PER_POLL = 1_000_000;
  private final Integer recordBufferSize;
  private final Integer recordBufferSizeBytes;
  private final int recordBufferOfferTimeout;
  private final int recordBufferFullWait;
  private final Integer fetchThreads;
  private final Integer maxRecordsPerPoll;
  private final Integer maxBytesPerPoll;

  public KinesisIndexTaskTuningConfig(
      @Nullable AppendableIndexSpec appendableIndexSpec,
      Integer maxRowsInMemory,
      Long maxBytesInMemory,
      @Nullable Boolean skipBytesInMemoryOverheadCheck,
      Integer maxRowsPerSegment,
      Long maxTotalRows,
      Period intermediatePersistPeriod,
      File basePersistDirectory,
      Integer maxPendingPersists,
      IndexSpec indexSpec,
      @Nullable IndexSpec indexSpecForIntermediatePersists,
      Boolean reportParseExceptions,
      Long handoffConditionTimeout,
      Boolean resetOffsetAutomatically,
      Boolean skipSequenceNumberAvailabilityCheck,
      @Deprecated @Nullable Integer recordBufferSize,
      @Nullable Integer recordBufferSizeBytes,
      Integer recordBufferOfferTimeout,
      Integer recordBufferFullWait,
      Integer fetchThreads,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @Nullable Boolean logParseExceptions,
      @Nullable Integer maxParseExceptions,
      @Nullable Integer maxSavedParseExceptions,
      @Deprecated @Nullable Integer maxRecordsPerPoll,
      @Nullable Integer maxBytesPerPoll,
      @Nullable Period intermediateHandoffPeriod
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
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        skipSequenceNumberAvailabilityCheck,
        segmentWriteOutMediumFactory,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        null
    );
    this.recordBufferSize = recordBufferSize;
    this.recordBufferSizeBytes = recordBufferSizeBytes;
    this.recordBufferOfferTimeout = recordBufferOfferTimeout == null
                                    ? DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT
                                    : recordBufferOfferTimeout;
    this.recordBufferFullWait = recordBufferFullWait == null ? DEFAULT_RECORD_BUFFER_FULL_WAIT : recordBufferFullWait;
    this.fetchThreads = fetchThreads; // we handle this being null later
    this.maxRecordsPerPoll = maxRecordsPerPoll;
    this.maxBytesPerPoll = maxBytesPerPoll;

    Preconditions.checkArgument(
        !(super.isResetOffsetAutomatically() && super.isSkipSequenceNumberAvailabilityCheck()),
        "resetOffsetAutomatically cannot be used if skipSequenceNumberAvailabilityCheck=true"
    );
  }

  @JsonCreator
  private KinesisIndexTaskTuningConfig(
      @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
      @JsonProperty("skipBytesInMemoryOverheadCheck") @Nullable Boolean skipBytesInMemoryOverheadCheck,
      @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
      @JsonProperty("maxTotalRows") Long maxTotalRows,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("resetOffsetAutomatically") Boolean resetOffsetAutomatically,
      @JsonProperty("skipSequenceNumberAvailabilityCheck") Boolean skipSequenceNumberAvailabilityCheck,
      @JsonProperty("recordBufferSize") @Deprecated @Nullable Integer recordBufferSize,
      @JsonProperty("recordBufferSizeBytes") Integer recordBufferSizeBytes,
      @JsonProperty("recordBufferOfferTimeout") Integer recordBufferOfferTimeout,
      @JsonProperty("recordBufferFullWait") Integer recordBufferFullWait,
      @JsonProperty("fetchThreads") Integer fetchThreads,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("maxRecordsPerPoll") @Deprecated @Nullable Integer maxRecordsPerPoll,
      @JsonProperty("maxBytesPerPoll") @Nullable Integer maxBytesPerPoll,
      @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod
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
        skipSequenceNumberAvailabilityCheck,
        recordBufferSize,
        recordBufferSizeBytes,
        recordBufferOfferTimeout,
        recordBufferFullWait,
        fetchThreads,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        maxRecordsPerPoll,
        maxBytesPerPoll,
        intermediateHandoffPeriod
    );
  }

  @Nullable
  @JsonProperty("recordBufferSize")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getRecordBufferSizeConfigured()
  {
    return recordBufferSize;
  }

  @Nullable
  @JsonProperty("recordBufferSizeBytes")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getRecordBufferSizeBytesConfigured()
  {
    return recordBufferSizeBytes;
  }

  public int getRecordBufferSizeBytesOrDefault(final long maxHeapSize)
  {
    if (recordBufferSizeBytes != null) {
      return recordBufferSizeBytes;
    } else {
      return (int) Math.min(
          MAX_RECORD_BUFFER_MEMORY,
          (long) (maxHeapSize * RECORD_BUFFER_MEMORY_MAX_HEAP_FRACTION)
      );
    }
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

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getFetchThreads()
  {
    return fetchThreads;
  }

  @Nullable
  @JsonProperty("maxRecordsPerPoll")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getMaxRecordsPerPollConfigured()
  {
    return maxRecordsPerPoll;
  }

  @Nullable
  @JsonProperty("maxBytesPerPoll")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getMaxBytesPerPollConfigured()
  {
    return maxBytesPerPoll;
  }

  public int getMaxBytesPerPollOrDefault()
  {
    return maxBytesPerPoll != null ? maxBytesPerPoll : DEFAULT_MAX_BYTES_PER_POLL;
  }

  @Override
  public KinesisIndexTaskTuningConfig withBasePersistDirectory(File dir)
  {
    return new KinesisIndexTaskTuningConfig(
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
        isSkipSequenceNumberAvailabilityCheck(),
        getRecordBufferSizeConfigured(),
        getRecordBufferSizeBytesConfigured(),
        getRecordBufferOfferTimeout(),
        getRecordBufferFullWait(),
        getFetchThreads(),
        getSegmentWriteOutMediumFactory(),
        isLogParseExceptions(),
        getMaxParseExceptions(),
        getMaxSavedParseExceptions(),
        getMaxRecordsPerPollConfigured(),
        getMaxBytesPerPollConfigured(),
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
    return Objects.equals(recordBufferSize, that.recordBufferSize) &&
           Objects.equals(recordBufferSizeBytes, that.recordBufferSizeBytes) &&
           recordBufferOfferTimeout == that.recordBufferOfferTimeout &&
           recordBufferFullWait == that.recordBufferFullWait &&
           Objects.equals(maxRecordsPerPoll, that.maxRecordsPerPoll) &&
           Objects.equals(maxBytesPerPoll, that.maxBytesPerPoll) &&
           Objects.equals(fetchThreads, that.fetchThreads);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        super.hashCode(),
        recordBufferSize,
        recordBufferSizeBytes,
        recordBufferOfferTimeout,
        recordBufferFullWait,
        fetchThreads,
        maxRecordsPerPoll,
        maxBytesPerPoll
    );
  }

  @Override
  public String toString()
  {
    return "KinesisIndexTaskTuningConfig{" +
           "maxRowsInMemory=" + getMaxRowsInMemory() +
           ", maxBytesInMemory=" + getMaxBytesInMemory() +
           ", skipBytesInMemoryOverheadCheck=" + isSkipBytesInMemoryOverheadCheck() +
           ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
           ", maxTotalRows=" + getMaxTotalRows() +
           ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
           ", maxPendingPersists=" + getMaxPendingPersists() +
           ", indexSpec=" + getIndexSpec() +
           ", reportParseExceptions=" + isReportParseExceptions() +
           ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
           ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
           ", skipSequenceNumberAvailabilityCheck=" + isSkipSequenceNumberAvailabilityCheck() +
           ", recordBufferSize=" + recordBufferSize +
           ", recordBufferSizeBytes=" + recordBufferSizeBytes +
           ", recordBufferOfferTimeout=" + recordBufferOfferTimeout +
           ", recordBufferFullWait=" + recordBufferFullWait +
           ", fetchThreads=" + fetchThreads +
           ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
           ", logParseExceptions=" + isLogParseExceptions() +
           ", maxParseExceptions=" + getMaxParseExceptions() +
           ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
           ", maxRecordsPerPoll=" + maxRecordsPerPoll +
           ", maxBytesPerPoll=" + maxBytesPerPoll +
           ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
            '}';
  }
}
