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

package org.apache.druid.indexing.common.index;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

@JsonTypeName("realtime_appenderator")
public class RealtimeAppenderatorTuningConfig implements AppenderatorConfig
{
  private static final Period DEFAULT_INTERMEDIATE_PERSIST_PERIOD = new Period("PT10M");
  private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
  private static final ShardSpec DEFAULT_SHARD_SPEC = new NumberedShardSpec(0, 1);
  private static final IndexSpec DEFAULT_INDEX_SPEC = IndexSpec.DEFAULT;
  private static final Boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = Boolean.FALSE;
  private static final long DEFAULT_HANDOFF_CONDITION_TIMEOUT = 0;
  private static final long DEFAULT_ALERT_TIMEOUT = 0;

  private final AppendableIndexSpec appendableIndexSpec;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final boolean skipBytesInMemoryOverheadCheck;
  private final DynamicPartitionsSpec partitionsSpec;
  private final Period intermediatePersistPeriod;
  private final File basePersistDirectory;
  private final int maxPendingPersists;
  private final ShardSpec shardSpec;
  private final IndexSpec indexSpec;
  private final IndexSpec indexSpecForIntermediatePersists;
  private final boolean reportParseExceptions;
  private final long publishAndHandoffTimeout;
  private final long alertTimeout;
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final int maxSavedParseExceptions;

  private final int numPersistThreads;

  public RealtimeAppenderatorTuningConfig(
      @Nullable AppendableIndexSpec appendableIndexSpec,
      Integer maxRowsInMemory,
      @Nullable Long maxBytesInMemory,
      @Nullable Boolean skipBytesInMemoryOverheadCheck,
      @Nullable Integer maxRowsPerSegment,
      @Nullable Long maxTotalRows,
      Period intermediatePersistPeriod,
      File basePersistDirectory,
      Integer maxPendingPersists,
      ShardSpec shardSpec,
      IndexSpec indexSpec,
      @Nullable IndexSpec indexSpecForIntermediatePersists,
      Boolean reportParseExceptions,
      Long publishAndHandoffTimeout,
      Long alertTimeout,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @Nullable Boolean logParseExceptions,
      @Nullable Integer maxParseExceptions,
      @Nullable Integer maxSavedParseExceptions,
      @Nullable Integer numPersistThreads
  )
  {
    this.appendableIndexSpec = appendableIndexSpec == null ? DEFAULT_APPENDABLE_INDEX : appendableIndexSpec;
    this.maxRowsInMemory = maxRowsInMemory == null ? DEFAULT_MAX_ROWS_IN_MEMORY_REALTIME : maxRowsInMemory;
    // initializing this to 0, it will be lazily initialized to a value
    // @see #getMaxBytesInMemoryOrDefault()
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.skipBytesInMemoryOverheadCheck = skipBytesInMemoryOverheadCheck == null ?
                                          DEFAULT_SKIP_BYTES_IN_MEMORY_OVERHEAD_CHECK : skipBytesInMemoryOverheadCheck;
    this.partitionsSpec = new DynamicPartitionsSpec(maxRowsPerSegment, maxTotalRows);
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? DEFAULT_INTERMEDIATE_PERSIST_PERIOD
                                     : intermediatePersistPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
    this.shardSpec = shardSpec == null ? DEFAULT_SHARD_SPEC : shardSpec;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists == null ?
                                            this.indexSpec : indexSpecForIntermediatePersists;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? DEFAULT_REPORT_PARSE_EXCEPTIONS
                                 : reportParseExceptions;
    this.publishAndHandoffTimeout = publishAndHandoffTimeout == null
                                    ? DEFAULT_HANDOFF_CONDITION_TIMEOUT
                                    : publishAndHandoffTimeout;
    Preconditions.checkArgument(this.publishAndHandoffTimeout >= 0, "publishAndHandoffTimeout must be >= 0");

    this.alertTimeout = alertTimeout == null ? DEFAULT_ALERT_TIMEOUT : alertTimeout;
    Preconditions.checkArgument(this.alertTimeout >= 0, "alertTimeout must be >= 0");
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;

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

  @JsonCreator
  private RealtimeAppenderatorTuningConfig(
      @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("skipBytesInMemoryOverheadCheck") @Nullable Boolean skipBytesInMemoryOverheadCheck,
      @JsonProperty("maxRowsPerSegment") @Nullable Integer maxRowsPerSegment,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("shardSpec") ShardSpec shardSpec,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("publishAndHandoffTimeout") Long publishAndHandoffTimeout,
      @JsonProperty("alertTimeout") Long alertTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
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
        shardSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        reportParseExceptions,
        publishAndHandoffTimeout,
        alertTimeout,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numPersistThreads
    );
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

  @Override
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

  @Override
  @JsonProperty
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  @Override
  public File getBasePersistDirectory()
  {
    return Preconditions.checkNotNull(basePersistDirectory, "basePersistDirectory not set");
  }

  @Override
  @JsonProperty
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @JsonProperty
  public ShardSpec getShardSpec()
  {
    return shardSpec;
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
  public long getPublishAndHandoffTimeout()
  {
    return publishAndHandoffTimeout;
  }

  @JsonProperty
  public long getAlertTimeout()
  {
    return alertTimeout;
  }

  @Override
  @JsonProperty
  @Nullable
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
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

  @Override
  @JsonProperty
  public int getNumPersistThreads()
  {
    return numPersistThreads;
  }

  @Override
  public RealtimeAppenderatorTuningConfig withBasePersistDirectory(File dir)
  {
    return new RealtimeAppenderatorTuningConfig(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        partitionsSpec.getMaxRowsPerSegment(),
        partitionsSpec.getMaxTotalRows(),
        intermediatePersistPeriod,
        dir,
        maxPendingPersists,
        shardSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        reportParseExceptions,
        publishAndHandoffTimeout,
        alertTimeout,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numPersistThreads
    );
  }
}
