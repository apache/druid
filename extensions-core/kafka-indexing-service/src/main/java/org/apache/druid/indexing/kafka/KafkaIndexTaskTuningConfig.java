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

package org.apache.druid.indexing.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskTuningConfig;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

public class KafkaIndexTaskTuningConfig extends SeekableStreamIndexTaskTuningConfig
{
  public KafkaIndexTaskTuningConfig(
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
      @Nullable Integer numPersistThreads
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
        false,
        segmentWriteOutMediumFactory,
        intermediateHandoffPeriod,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numPersistThreads
    );
  }

  @JsonCreator
  private KafkaIndexTaskTuningConfig(
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
      @JsonProperty("resetOffsetAutomatically") @Nullable Boolean resetOffsetAutomatically,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("intermediateHandoffPeriod") @Nullable Period intermediateHandoffPeriod,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("numPersistThreads") @Nullable Integer numPersistThreads
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
        numPersistThreads
    );
  }

  @Override
  public KafkaIndexTaskTuningConfig withBasePersistDirectory(File dir)
  {
    return new KafkaIndexTaskTuningConfig(
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
        getNumPersistThreads()
    );
  }


  @Override
  public String toString()
  {
    return "KafkaIndexTaskTuningConfig{" +
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
            '}';
  }

}
