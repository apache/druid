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

package org.apache.druid.indexing.seekablestream;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.indexing.RealtimeTuningConfig;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

public abstract class SeekableStreamIndexTaskTuningConfig implements AppenderatorConfig
{
  private static final boolean DEFAULT_RESET_OFFSET_AUTOMATICALLY = false;
  private static final boolean DEFAULT_SKIP_SEQUENCE_NUMBER_AVAILABILITY_CHECK = false;

  private final AppendableIndexSpec appendableIndexSpec;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final boolean skipBytesInMemoryOverheadCheck;
  private final DynamicPartitionsSpec partitionsSpec;
  private final Period intermediatePersistPeriod;
  private final File basePersistDirectory;
  @Deprecated
  private final int maxPendingPersists;
  private final IndexSpec indexSpec;
  private final IndexSpec indexSpecForIntermediatePersists;
  private final boolean reportParseExceptions;
  private final long handoffConditionTimeout;
  private final boolean resetOffsetAutomatically;
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
  private final Period intermediateHandoffPeriod;
  private final boolean skipSequenceNumberAvailabilityCheck;

  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final int maxSavedParseExceptions;

  private final int numPersistThreads;

  public SeekableStreamIndexTaskTuningConfig(
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
      @Deprecated @Nullable Boolean reportParseExceptions,
      @Nullable Long handoffConditionTimeout,
      @Nullable Boolean resetOffsetAutomatically,
      Boolean skipSequenceNumberAvailabilityCheck,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @Nullable Period intermediateHandoffPeriod,
      @Nullable Boolean logParseExceptions,
      @Nullable Integer maxParseExceptions,
      @Nullable Integer maxSavedParseExceptions,
      @Nullable Integer numPersistThreads
  )
  {
    // Cannot be a static because default basePersistDirectory is unique per-instance
    final RealtimeTuningConfig defaults = RealtimeTuningConfig.makeDefaultTuningConfig(basePersistDirectory);

    this.appendableIndexSpec = appendableIndexSpec == null ? DEFAULT_APPENDABLE_INDEX : appendableIndexSpec;
    this.maxRowsInMemory = maxRowsInMemory == null ? defaults.getMaxRowsInMemory() : maxRowsInMemory;
    this.partitionsSpec = new DynamicPartitionsSpec(maxRowsPerSegment, maxTotalRows);
    // initializing this to 0, it will be lazily initialized to a value
    // @see #getMaxBytesInMemoryOrDefault()
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck == null ?
                                          DEFAULT_SKIP_BYTES_IN_MEMORY_OVERHEAD_CHECK : skipBytesInMemoryOverheadCheck;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? defaults.getIntermediatePersistPeriod()
                                     : intermediatePersistPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.maxPendingPersists = maxPendingPersists == null ? 0 : maxPendingPersists;
    this.indexSpec = indexSpec == null ? defaults.getIndexSpec() : indexSpec;
    this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists == null ?
                                            this.indexSpec : indexSpecForIntermediatePersists;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? defaults.isReportParseExceptions()
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? defaults.getHandoffConditionTimeout()
                                   : handoffConditionTimeout;
    this.resetOffsetAutomatically = resetOffsetAutomatically == null
                                    ? DEFAULT_RESET_OFFSET_AUTOMATICALLY
                                    : resetOffsetAutomatically;
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.intermediateHandoffPeriod = intermediateHandoffPeriod == null
                                     ? new Period().withDays(Integer.MAX_VALUE)
                                     : intermediateHandoffPeriod;
    this.skipSequenceNumberAvailabilityCheck = skipSequenceNumberAvailabilityCheck == null
                                               ? DEFAULT_SKIP_SEQUENCE_NUMBER_AVAILABILITY_CHECK
                                               : skipSequenceNumberAvailabilityCheck;

    if (this.reportParseExceptions) {
      this.maxParseExceptions = 0;
      this.maxSavedParseExceptions = maxSavedParseExceptions == null ? 0 : Math.min(1, maxSavedParseExceptions);
    } else {
      this.maxParseExceptions = maxParseExceptions == null
                                ? TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS
                                : maxParseExceptions;
      this.maxSavedParseExceptions = maxSavedParseExceptions == null
                                     ? TuningConfig.DEFAULT_MAX_SAVED_PARSE_EXCEPTIONS
                                     : maxSavedParseExceptions;
    }
    this.logParseExceptions = logParseExceptions == null
                              ? TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS
                              : logParseExceptions;
    this.numPersistThreads = numPersistThreads == null ?
                                DEFAULT_NUM_PERSIST_THREADS : Math.max(numPersistThreads, DEFAULT_NUM_PERSIST_THREADS);
  }

  @Override
  @JsonProperty
  public AppendableIndexSpec getAppendableIndexSpec()
  {
    return appendableIndexSpec;
  }

  @Override
  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @Override
  @JsonProperty
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  @JsonProperty
  @Override
  public boolean isSkipBytesInMemoryOverheadCheck()
  {
    return skipBytesInMemoryOverheadCheck;
  }

  @Override
  @JsonProperty
  public Integer getMaxRowsPerSegment()
  {
    return partitionsSpec.getMaxRowsPerSegment();
  }

  @JsonProperty
  @Override
  @Nullable
  public Long getMaxTotalRows()
  {
    return partitionsSpec.getMaxTotalRows();
  }

  @Override
  public DynamicPartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @Override
  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  @Override
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @Override
  @JsonProperty
  @Deprecated
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @Override
  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  @Override
  public IndexSpec getIndexSpecForIntermediatePersists()
  {
    return indexSpecForIntermediatePersists;
  }

  @Override
  @JsonProperty
  public boolean isReportParseExceptions()
  {
    return reportParseExceptions;
  }

  @JsonProperty
  public long getHandoffConditionTimeout()
  {
    return handoffConditionTimeout;
  }

  @JsonProperty
  public boolean isResetOffsetAutomatically()
  {
    return resetOffsetAutomatically;
  }

  @Override
  @JsonProperty
  @Nullable
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
  }

  @JsonProperty
  public Period getIntermediateHandoffPeriod()
  {
    return intermediateHandoffPeriod;
  }

  @JsonProperty
  public boolean isLogParseExceptions()
  {
    return logParseExceptions;
  }

  @JsonProperty
  public int getMaxParseExceptions()
  {
    return maxParseExceptions;
  }

  @JsonProperty
  public int getMaxSavedParseExceptions()
  {
    return maxSavedParseExceptions;
  }

  @JsonProperty
  public boolean isSkipSequenceNumberAvailabilityCheck()
  {
    return skipSequenceNumberAvailabilityCheck;
  }

  @Override
  @JsonProperty
  public int getNumPersistThreads()
  {
    return numPersistThreads;
  }

  @Override
  public abstract SeekableStreamIndexTaskTuningConfig withBasePersistDirectory(File dir);

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SeekableStreamIndexTaskTuningConfig that = (SeekableStreamIndexTaskTuningConfig) o;
    return Objects.equals(appendableIndexSpec, that.appendableIndexSpec) &&
           maxRowsInMemory == that.maxRowsInMemory &&
           maxBytesInMemory == that.maxBytesInMemory &&
           skipBytesInMemoryOverheadCheck == that.skipBytesInMemoryOverheadCheck &&
           maxPendingPersists == that.maxPendingPersists &&
           reportParseExceptions == that.reportParseExceptions &&
           handoffConditionTimeout == that.handoffConditionTimeout &&
           resetOffsetAutomatically == that.resetOffsetAutomatically &&
           skipSequenceNumberAvailabilityCheck == that.skipSequenceNumberAvailabilityCheck &&
           logParseExceptions == that.logParseExceptions &&
           maxParseExceptions == that.maxParseExceptions &&
           maxSavedParseExceptions == that.maxSavedParseExceptions &&
           numPersistThreads == that.numPersistThreads &&
           Objects.equals(partitionsSpec, that.partitionsSpec) &&
           Objects.equals(intermediatePersistPeriod, that.intermediatePersistPeriod) &&
           Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(indexSpecForIntermediatePersists, that.indexSpecForIntermediatePersists) &&
           Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory) &&
           Objects.equals(intermediateHandoffPeriod, that.intermediateHandoffPeriod);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        partitionsSpec,
        intermediatePersistPeriod,
        basePersistDirectory,
        maxPendingPersists,
        indexSpec,
        indexSpecForIntermediatePersists,
        reportParseExceptions,
        handoffConditionTimeout,
        resetOffsetAutomatically,
        segmentWriteOutMediumFactory,
        intermediateHandoffPeriod,
        skipSequenceNumberAvailabilityCheck,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numPersistThreads
    );
  }

  @Override
  public abstract String toString();
}
