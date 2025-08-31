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
import org.apache.druid.common.config.Configs;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.time.Duration;
import java.util.Objects;

public abstract class SeekableStreamIndexTaskTuningConfig implements TuningConfig
{
  public static final int DEFAULT_MAX_COLUMNS_TO_MERGE = -1;
  private static final boolean DEFAULT_RESET_OFFSET_AUTOMATICALLY = false;
  private static final boolean DEFAULT_SKIP_SEQUENCE_NUMBER_AVAILABILITY_CHECK = false;
  private static final Period DEFAULT_INTERMEDIATE_PERSIST_PERIOD = new Period("PT10M");
  private static final IndexSpec DEFAULT_INDEX_SPEC = IndexSpec.DEFAULT;
  private static final Boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = Boolean.FALSE;
  private static final boolean DEFAULT_RELEASE_LOCKS_ON_HANDOFF = false;
  private static final long DEFAULT_HANDOFF_CONDITION_TIMEOUT = Duration.ofMinutes(15).toMillis();

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
  private final int maxColumnsToMerge;
  private final boolean releaseLocksOnHandoff;

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
      @Nullable Integer numPersistThreads,
      @Nullable Integer maxColumnsToMerge,
      @Nullable Boolean releaseLocksOnHandoff
  )
  {
    this.appendableIndexSpec = appendableIndexSpec == null ? DEFAULT_APPENDABLE_INDEX : appendableIndexSpec;
    this.maxRowsInMemory = maxRowsInMemory == null ? DEFAULT_MAX_ROWS_IN_MEMORY_REALTIME : maxRowsInMemory;
    this.partitionsSpec = new DynamicPartitionsSpec(maxRowsPerSegment, maxTotalRows);
    // initializing this to 0, it will be lazily initialized to a value
    // @see #getMaxBytesInMemoryOrDefault()
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck == null ?
                                          DEFAULT_SKIP_BYTES_IN_MEMORY_OVERHEAD_CHECK : skipBytesInMemoryOverheadCheck;
    this.intermediatePersistPeriod =
        intermediatePersistPeriod == null ? DEFAULT_INTERMEDIATE_PERSIST_PERIOD : intermediatePersistPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.maxPendingPersists = maxPendingPersists == null ? 0 : maxPendingPersists;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists == null ?
                                            this.indexSpec : indexSpecForIntermediatePersists;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? DEFAULT_REPORT_PARSE_EXCEPTIONS
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? DEFAULT_HANDOFF_CONDITION_TIMEOUT
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
    if (numPersistThreads == null) {
      this.numPersistThreads = AppenderatorConfig.DEFAULT_NUM_PERSIST_THREADS;
    } else {
      this.numPersistThreads = Math.max(numPersistThreads, AppenderatorConfig.DEFAULT_NUM_PERSIST_THREADS);
    }
    this.maxColumnsToMerge = maxColumnsToMerge == null ? DEFAULT_MAX_COLUMNS_TO_MERGE : maxColumnsToMerge;
    this.releaseLocksOnHandoff = Configs.valueOrDefault(releaseLocksOnHandoff, DEFAULT_RELEASE_LOCKS_ON_HANDOFF);
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
  public boolean isSkipBytesInMemoryOverheadCheck()
  {
    return skipBytesInMemoryOverheadCheck;
  }

  @JsonProperty
  public Integer getMaxRowsPerSegment()
  {
    return partitionsSpec.getMaxRowsPerSegment();
  }

  @JsonProperty
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

  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

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

  @JsonProperty
  public int getNumPersistThreads()
  {
    return numPersistThreads;
  }

  @JsonProperty
  public int getMaxColumnsToMerge()
  {
    return maxColumnsToMerge;
  }

  @JsonProperty
  public boolean isReleaseLocksOnHandoff()
  {
    return releaseLocksOnHandoff;
  }

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
           maxColumnsToMerge == that.maxColumnsToMerge &&
           releaseLocksOnHandoff == that.releaseLocksOnHandoff &&
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
        numPersistThreads,
        maxColumnsToMerge,
        releaseLocksOnHandoff
    );
  }

  @Override
  public abstract String toString();
}
