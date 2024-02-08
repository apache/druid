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

package org.apache.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.kafka.KafkaIndexTaskTuningConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Duration;
import org.joda.time.Period;

import javax.annotation.Nullable;

public class KafkaSupervisorTuningConfig extends KafkaIndexTaskTuningConfig
    implements SeekableStreamSupervisorTuningConfig
{
  private final Integer workerThreads;
  private final Long chatRetries;
  private final Duration httpTimeout;
  private final Duration shutdownTimeout;
  private final Duration offsetFetchPeriod;

  public static KafkaSupervisorTuningConfig defaultConfig()
  {
    return new KafkaSupervisorTuningConfig(
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

  public KafkaSupervisorTuningConfig(
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
      @JsonProperty("chatRetries") Long chatRetries,
      @JsonProperty("httpTimeout") Period httpTimeout,
      @JsonProperty("shutdownTimeout") Period shutdownTimeout,
      @JsonProperty("offsetFetchPeriod") Period offsetFetchPeriod,
      @JsonProperty("intermediateHandoffPeriod") Period intermediateHandoffPeriod,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("numPersistThreads") @Nullable Integer numPersistThreads
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
        numPersistThreads
    );
    this.workerThreads = workerThreads;
    this.chatRetries = (chatRetries != null ? chatRetries : DEFAULT_CHAT_RETRIES);
    this.httpTimeout = SeekableStreamSupervisorTuningConfig.defaultDuration(httpTimeout, DEFAULT_HTTP_TIMEOUT);
    this.shutdownTimeout = SeekableStreamSupervisorTuningConfig.defaultDuration(
        shutdownTimeout,
        DEFAULT_SHUTDOWN_TIMEOUT
    );
    this.offsetFetchPeriod = SeekableStreamSupervisorTuningConfig.defaultDuration(
        offsetFetchPeriod,
        DEFAULT_OFFSET_FETCH_PERIOD
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
    // Stopping tasks early for Kafka ingestion on partition set change is not supported yet,
    // just return a default for now.
    return SeekableStreamSupervisorTuningConfig.defaultDuration(
        null,
        SeekableStreamSupervisorTuningConfig.DEFAULT_REPARTITION_TRANSITION_DURATION
    );
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
    return "KafkaSupervisorTuningConfig{" +
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
           ", chatRetries=" + chatRetries +
           ", httpTimeout=" + httpTimeout +
           ", shutdownTimeout=" + shutdownTimeout +
           ", offsetFetchPeriod=" + offsetFetchPeriod +
           ", intermediateHandoffPeriod=" + getIntermediateHandoffPeriod() +
           ", logParseExceptions=" + isLogParseExceptions() +
           ", maxParseExceptions=" + getMaxParseExceptions() +
           ", maxSavedParseExceptions=" + getMaxSavedParseExceptions() +
           ", numPersistThreads=" + getNumPersistThreads() +
           '}';
  }

  @Override
  public KafkaIndexTaskTuningConfig convertToTaskTuningConfig()
  {
    return new KafkaIndexTaskTuningConfig(
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
        getNumPersistThreads()
    );
  }
}
