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
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.seekablestream.SeekableStreamTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public class KinesisTuningConfig extends SeekableStreamTuningConfig
{

  private static final int DEFAULT_RECORD_BUFFER_SIZE = 10000;
  private static final int DEFAULT_RECORD_BUFFER_OFFER_TIMEOUT = 5000;
  private static final int DEFAULT_RECORD_BUFFER_FULL_WAIT = 5000;
  private static final int DEFAULT_FETCH_SEQUENCE_NUMBER_TIMEOUT = 60000;


  private final int recordBufferSize;
  private final int recordBufferOfferTimeout;
  private final int recordBufferFullWait;
  private final int fetchSequenceNumberTimeout;
  private final Integer fetchThreads;

  @JsonCreator
  public KinesisTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
      @JsonProperty("maxRowsPerSegment") Integer maxRowsPerSegment,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
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
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions
  )
  {
    super(
        maxRowsInMemory,
        maxBytesInMemory,
        maxRowsPerSegment,
        null,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        true,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        skipSequenceNumberAvailabilityCheck,
        segmentWriteOutMediumFactory,
        null,
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

    Preconditions.checkArgument(
        !(super.isResetOffsetAutomatically() && super.isSkipSequenceNumberAvailabilityCheck()),
        "resetOffsetAutomatically cannot be used if skipSequenceNumberAvailabilityCheck=true"
    );
  }

  @Override
  public KinesisTuningConfig copyOf()
  {
    return new KinesisTuningConfig(
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        getMaxRowsPerSegment(),
        getIntermediatePersistPeriod(),
        getBasePersistDirectory(),
        0,
        getIndexSpec(),
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
        getMaxSavedParseExceptions()
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

  @Override
  public KinesisTuningConfig withBasePersistDirectory(File dir)
  {
    return new KinesisTuningConfig(
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        getMaxRowsPerSegment(),
        getIntermediatePersistPeriod(),
        dir,
        0,
        getIndexSpec(),
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
        getMaxSavedParseExceptions()
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
    KinesisTuningConfig that = (KinesisTuningConfig) o;
    return getMaxRowsInMemory() == that.getMaxRowsInMemory() &&
           getMaxBytesInMemory() == that.getMaxBytesInMemory() &&
           getMaxRowsPerSegment() == that.getMaxRowsPerSegment() &&
           getMaxPendingPersists() == that.getMaxPendingPersists() &&
           getBuildV9Directly() == that.getBuildV9Directly() &&
           isReportParseExceptions() == that.isReportParseExceptions() &&
           getHandoffConditionTimeout() == that.getHandoffConditionTimeout() &&
           isResetOffsetAutomatically() == that.isResetOffsetAutomatically() &&
           isSkipSequenceNumberAvailabilityCheck() == that.isSkipSequenceNumberAvailabilityCheck() &&
           getRecordBufferSize() == that.getRecordBufferSize() &&
           getRecordBufferOfferTimeout() == that.getRecordBufferOfferTimeout() &&
           getRecordBufferFullWait() == that.getRecordBufferFullWait() &&
           getFetchSequenceNumberTimeout() == that.getFetchSequenceNumberTimeout() &&
           isLogParseExceptions() == that.isLogParseExceptions() &&
           getMaxParseExceptions() == that.getMaxParseExceptions() &&
           getMaxSavedParseExceptions() == that.getMaxSavedParseExceptions() &&
           Objects.equals(getIntermediatePersistPeriod(), that.getIntermediatePersistPeriod()) &&
           Objects.equals(getBasePersistDirectory(), that.getBasePersistDirectory()) &&
           Objects.equals(getIndexSpec(), that.getIndexSpec()) &&
           Objects.equals(getFetchThreads(), that.getFetchThreads()) &&
           Objects.equals(getSegmentWriteOutMediumFactory(), that.getSegmentWriteOutMediumFactory());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        getMaxRowsPerSegment(),
        getIntermediatePersistPeriod(),
        getBasePersistDirectory(),
        0,
        getIndexSpec(),
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
        getMaxSavedParseExceptions()
    );
  }

  @Override
  public String toString()
  {
    return "KinesisTuningConfig{" +
           "maxRowsInMemory=" + getMaxRowsInMemory() +
           ", maxBytesInMemory=" + getMaxBytesInMemory() +
           ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
           ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
           ", basePersistDirectory=" + getBasePersistDirectory() +
           ", maxPendingPersists=" + 0 +
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
           '}';
  }
}
