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

package org.apache.druid.indexing.kinesis.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

public class KinesisSupervisorTuningConfig extends KinesisIndexTaskTuningConfig
    implements SeekableStreamSupervisorTuningConfig
{
  private final Integer workerThreads;
  private final Integer chatThreads;
  private final Long chatRetries;
  private final Duration httpTimeout;
  private final Duration shutdownTimeout;
  private final Duration repartitionTransitionDuration;

  public KinesisSupervisorTuningConfig(
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
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("workerThreads") Integer workerThreads,
      @JsonProperty("chatThreads") Integer chatThreads,
      @JsonProperty("chatRetries") Long chatRetries,
      @JsonProperty("httpTimeout") Period httpTimeout,
      @JsonProperty("shutdownTimeout") Period shutdownTimeout,
      @JsonProperty("recordBufferSize") Integer recordBufferSize,
      @JsonProperty("recordBufferOfferTimeout") Integer recordBufferOfferTimeout,
      @JsonProperty("recordBufferFullWait") Integer recordBufferFullWait,
      @JsonProperty("fetchSequenceNumberTimeout") Integer fetchSequenceNumberTimeout,
      @JsonProperty("fetchThreads") Integer fetchThreads,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("maxRecordsPerPoll") @Nullable Integer maxRecordsPerPoll,
      @JsonProperty("intermediateHandoffPeriod") Period intermediateHandoffPeriod,
      @JsonProperty("repartitionTransitionDuration") Period repartitionTransitionDuration
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

    this.workerThreads = workerThreads;
    this.chatThreads = chatThreads;
    this.chatRetries = (chatRetries != null ? chatRetries : DEFAULT_CHAT_RETRIES);
    this.httpTimeout = SeekableStreamSupervisorTuningConfig.defaultDuration(httpTimeout, DEFAULT_HTTP_TIMEOUT);
    this.shutdownTimeout = SeekableStreamSupervisorTuningConfig.defaultDuration(
        shutdownTimeout,
        DEFAULT_SHUTDOWN_TIMEOUT
    );
    this.repartitionTransitionDuration = SeekableStreamSupervisorTuningConfig.defaultDuration(
        repartitionTransitionDuration,
        DEFAULT_REPARTITION_TRANSITION_DURATION
    );
  }

  @Override
  @JsonProperty
  public Integer getWorkerThreads()
  {
    return workerThreads;
  }

  @Override
  @JsonProperty
  public Integer getChatThreads()
  {
    return chatThreads;
  }

  @Override
  @JsonProperty
  public Long getChatRetries()
  {
    return chatRetries;
  }

  @Override
  @JsonProperty
  public Duration getHttpTimeout()
  {
    return httpTimeout;
  }

  @Override
  @JsonProperty
  public Duration getShutdownTimeout()
  {
    return shutdownTimeout;
  }

  @Override
  public Duration getRepartitionTransitionDuration()
  {
    return repartitionTransitionDuration;
  }

  @Override
  public String toString()
  {
    return "KinesisSupervisorTuningConfig{" +
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
           ", workerThreads=" + workerThreads +
           ", chatThreads=" + chatThreads +
           ", chatRetries=" + chatRetries +
           ", httpTimeout=" + httpTimeout +
           ", shutdownTimeout=" + shutdownTimeout +
           ", recordBufferSize=" + getRecordBufferSize() +
           ", recordBufferOfferTimeout=" + getRecordBufferOfferTimeout() +
           ", recordBufferFullWait=" + getRecordBufferFullWait() +
           ", fetchSequenceNumberTimeout=" + getFetchSequenceNumberTimeout() +
           ", fetchThreads=" + getFetchThreads() +
           ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
           ", logParseExceptions=" + isLogParseExceptions() +
           ", maxParseExceptions=" + getMaxParseExceptions() +
           ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
           ", maxRecordsPerPoll=" + getMaxRecordsPerPoll() +
           ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
           ", repartitionTransitionDuration=" + getRepartitionTransitionDuration() +
           '}';
  }

  @Override
  public KinesisIndexTaskTuningConfig convertToTaskTuningConfig()
  {
    return new KinesisIndexTaskTuningConfig(
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        getMaxRowsPerSegment(),
        getMaxTotalRows(),
        getIntermediatePersistPeriod(),
        getBasePersistDirectory(),
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

}
