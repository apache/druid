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

package org.apache.druid.indexing.rabbitstream.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.rabbitstream.RabbitStreamIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;

public class RabbitStreamSupervisorTuningConfig extends RabbitStreamIndexTaskTuningConfig
    implements SeekableStreamSupervisorTuningConfig
{
  private final Integer workerThreads;
  private final Boolean chatAsync;
  private final Integer chatThreads;
  private final Long chatRetries;
  private final Duration httpTimeout;
  private final Duration shutdownTimeout;
  private final Duration offsetFetchPeriod;

  public static RabbitStreamSupervisorTuningConfig defaultConfig()
  {
    return new RabbitStreamSupervisorTuningConfig(
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null
        );
  }

  public RabbitStreamSupervisorTuningConfig(
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
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("workerThreads") Integer workerThreads,
      @JsonProperty("chatAsync") Boolean chatAsync,
      @JsonProperty("chatThreads") Integer chatThreads,
      @JsonProperty("chatRetries") Long chatRetries,
      @JsonProperty("httpTimeout") Period httpTimeout,
      @JsonProperty("shutdownTimeout") Period shutdownTimeout,
      @JsonProperty("recordBufferSize") Integer recordBufferSize,
      @JsonProperty("recordBufferOfferTimeout") Integer recordBufferOfferTimeout,
      @JsonProperty("offsetFetchPeriod") Period offsetFetchPeriod,
      @JsonProperty("intermediateHandoffPeriod") Period intermediateHandoffPeriod,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("numPersistThreads") @Nullable Integer numPersistThreads,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("maxRecordsPerPoll") @Nullable Integer maxRecordsPerPoll)
  {
    super(
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
        maxRecordsPerPoll
    );
    this.workerThreads = workerThreads;
    this.chatAsync = chatAsync;
    this.chatThreads = chatThreads;
    this.chatRetries = (chatRetries != null ? chatRetries : DEFAULT_CHAT_RETRIES);
    this.httpTimeout = SeekableStreamSupervisorTuningConfig.defaultDuration(httpTimeout, DEFAULT_HTTP_TIMEOUT);
    this.shutdownTimeout = SeekableStreamSupervisorTuningConfig.defaultDuration(
        shutdownTimeout,
        DEFAULT_SHUTDOWN_TIMEOUT);
    this.offsetFetchPeriod = SeekableStreamSupervisorTuningConfig.defaultDuration(
        offsetFetchPeriod,
        DEFAULT_OFFSET_FETCH_PERIOD);
  }

  @Override
  @JsonProperty
  public Integer getWorkerThreads()
  {
    return workerThreads;
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
    // just return a default for now.
    return SeekableStreamSupervisorTuningConfig.defaultDuration(
        null,
        SeekableStreamSupervisorTuningConfig.DEFAULT_REPARTITION_TRANSITION_DURATION);
  }

  @Override
  @JsonProperty
  public Duration getOffsetFetchPeriod()
  {
    return offsetFetchPeriod;
  }

  @Override
  public String toString()
  {
    return "RabbitStreamSupervisorTuningConfig{" +
        "maxRowsInMemory=" + getMaxRowsInMemory() +
        ", maxRowsPerSegment=" + getMaxRowsPerSegment() +
        ", maxTotalRows=" + getMaxTotalRows() +
        ", maxBytesInMemory=" + getMaxBytesInMemoryOrDefault() +
        ", skipBytesInMemoryOverheadCheck=" + isSkipBytesInMemoryOverheadCheck() +
        ", intermediatePersistPeriod=" + getIntermediatePersistPeriod() +
        ", maxPendingPersists=" + getMaxPendingPersists() +
        ", indexSpec=" + getIndexSpec() +
        ", reportParseExceptions=" + isReportParseExceptions() +
        ", handoffConditionTimeout=" + getHandoffConditionTimeout() +
        ", resetOffsetAutomatically=" + isResetOffsetAutomatically() +
        ", segmentWriteOutMediumFactory=" + getSegmentWriteOutMediumFactory() +
        ", workerThreads=" + workerThreads +
        ", chatThreads=" + chatThreads +
        ", chatRetries=" + chatRetries +
        ", httpTimeout=" + httpTimeout +
        ", shutdownTimeout=" + shutdownTimeout +
        ", recordBufferSize=" + getRecordBufferSizeConfigured() +
        ", recordBufferOfferTimeout=" + getRecordBufferOfferTimeout() +
        ", offsetFetchPeriod=" + offsetFetchPeriod +
        ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
        ", logParseExceptions=" + isLogParseExceptions() +
        ", maxParseExceptions=" + getMaxParseExceptions() +
        ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
        ", numPersistThreads=" + getNumPersistThreads() +
        ", maxRecordsPerPoll=" + getMaxRecordsPerPollConfigured() +
        '}';
  }

  @Override
  public RabbitStreamIndexTaskTuningConfig convertToTaskTuningConfig()
  {
    return new RabbitStreamIndexTaskTuningConfig(
        getAppendableIndexSpec(),
        getMaxRowsInMemory(),
        getMaxBytesInMemory(),
        isSkipBytesInMemoryOverheadCheck(),
        getMaxRowsPerSegment(),
        getMaxTotalRows(),
        getIntermediatePersistPeriod(),
        null,
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
        getRecordBufferSizeConfigured(),
        getRecordBufferOfferTimeout(),
        getMaxRecordsPerPollConfigured(),
        getNumPersistThreads()
        );
  }
}
