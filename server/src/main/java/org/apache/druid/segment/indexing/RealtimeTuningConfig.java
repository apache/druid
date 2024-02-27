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

package org.apache.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.realtime.plumber.IntervalStartVersioningPolicy;
import org.apache.druid.segment.realtime.plumber.RejectionPolicyFactory;
import org.apache.druid.segment.realtime.plumber.ServerTimeRejectionPolicyFactory;
import org.apache.druid.segment.realtime.plumber.VersioningPolicy;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.ShardSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.time.Duration;

/**
 *
 */
public class RealtimeTuningConfig implements AppenderatorConfig
{
  private static final Period DEFAULT_INTERMEDIATE_PERSIST_PERIOD = new Period("PT10M");
  private static final Period DEFAULT_WINDOW_PERIOD = new Period("PT10M");
  private static final VersioningPolicy DEFAULT_VERSIONING_POLICY = new IntervalStartVersioningPolicy();
  private static final RejectionPolicyFactory DEFAULT_REJECTION_POLICY_FACTORY = new ServerTimeRejectionPolicyFactory();
  private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
  private static final ShardSpec DEFAULT_SHARD_SPEC = new NumberedShardSpec(0, 1);
  private static final IndexSpec DEFAULT_INDEX_SPEC = IndexSpec.DEFAULT;
  private static final Boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = Boolean.FALSE;
  private static final long DEFAULT_HANDOFF_CONDITION_TIMEOUT = Duration.ofMinutes(15).toMillis();
  private static final long DEFAULT_ALERT_TIMEOUT = 0;
  private static final String DEFAULT_DEDUP_COLUMN = null;

  // Might make sense for this to be a builder
  public static RealtimeTuningConfig makeDefaultTuningConfig(final @Nullable File basePersistDirectory)
  {
    return new RealtimeTuningConfig(
        DEFAULT_APPENDABLE_INDEX,
        DEFAULT_MAX_ROWS_IN_MEMORY_REALTIME,
        0L,
        DEFAULT_SKIP_BYTES_IN_MEMORY_OVERHEAD_CHECK,
        DEFAULT_INTERMEDIATE_PERSIST_PERIOD,
        DEFAULT_WINDOW_PERIOD,
        basePersistDirectory,
        DEFAULT_VERSIONING_POLICY,
        DEFAULT_REJECTION_POLICY_FACTORY,
        DEFAULT_MAX_PENDING_PERSISTS,
        DEFAULT_SHARD_SPEC,
        DEFAULT_INDEX_SPEC,
        DEFAULT_INDEX_SPEC,
        0,
        0,
        DEFAULT_REPORT_PARSE_EXCEPTIONS,
        DEFAULT_HANDOFF_CONDITION_TIMEOUT,
        DEFAULT_ALERT_TIMEOUT,
        null,
        DEFAULT_DEDUP_COLUMN,
        DEFAULT_NUM_PERSIST_THREADS
    );
  }

  private final AppendableIndexSpec appendableIndexSpec;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final boolean skipBytesInMemoryOverheadCheck;
  private final Period intermediatePersistPeriod;
  private final Period windowPeriod;
  private final File basePersistDirectory;
  private final VersioningPolicy versioningPolicy;
  private final RejectionPolicyFactory rejectionPolicyFactory;
  private final int maxPendingPersists;
  private final ShardSpec shardSpec;
  private final IndexSpec indexSpec;
  private final IndexSpec indexSpecForIntermediatePersists;
  private final int persistThreadPriority;
  private final int mergeThreadPriority;
  private final boolean reportParseExceptions;
  private final long handoffConditionTimeout;
  private final long alertTimeout;
  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;
  @Nullable
  private final String dedupColumn;
  private final int numPersistThreads;

  public RealtimeTuningConfig(
      @Nullable AppendableIndexSpec appendableIndexSpec,
      Integer maxRowsInMemory,
      Long maxBytesInMemory,
      @Nullable Boolean skipBytesInMemoryOverheadCheck,
      Period intermediatePersistPeriod,
      Period windowPeriod,
      File basePersistDirectory,
      VersioningPolicy versioningPolicy,
      RejectionPolicyFactory rejectionPolicyFactory,
      Integer maxPendingPersists,
      ShardSpec shardSpec,
      IndexSpec indexSpec,
      @Nullable IndexSpec indexSpecForIntermediatePersists,
      int persistThreadPriority,
      int mergeThreadPriority,
      Boolean reportParseExceptions,
      Long handoffConditionTimeout,
      Long alertTimeout,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @Nullable String dedupColumn,
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
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? DEFAULT_INTERMEDIATE_PERSIST_PERIOD
                                     : intermediatePersistPeriod;
    this.windowPeriod = windowPeriod == null ? DEFAULT_WINDOW_PERIOD : windowPeriod;
    this.basePersistDirectory = basePersistDirectory;
    this.versioningPolicy = versioningPolicy;
    this.rejectionPolicyFactory = rejectionPolicyFactory == null
                                  ? DEFAULT_REJECTION_POLICY_FACTORY
                                  : rejectionPolicyFactory;
    this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
    this.shardSpec = shardSpec == null ? DEFAULT_SHARD_SPEC : shardSpec;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists == null ?
                                            this.indexSpec : indexSpecForIntermediatePersists;
    this.mergeThreadPriority = mergeThreadPriority;
    this.persistThreadPriority = persistThreadPriority;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? DEFAULT_REPORT_PARSE_EXCEPTIONS
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? DEFAULT_HANDOFF_CONDITION_TIMEOUT
                                   : handoffConditionTimeout;
    Preconditions.checkArgument(this.handoffConditionTimeout >= 0, "handoffConditionTimeout must be >= 0");

    this.alertTimeout = alertTimeout == null ? DEFAULT_ALERT_TIMEOUT : alertTimeout;
    Preconditions.checkArgument(this.alertTimeout >= 0, "alertTimeout must be >= 0");
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.dedupColumn = dedupColumn == null ? DEFAULT_DEDUP_COLUMN : dedupColumn;
    this.numPersistThreads = numPersistThreads == null ?
            DEFAULT_NUM_PERSIST_THREADS : Math.max(numPersistThreads, DEFAULT_NUM_PERSIST_THREADS);
  }

  @JsonCreator
  private RealtimeTuningConfig(
      @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
      @JsonProperty("skipBytesInMemoryOverheadCheck") @Nullable Boolean skipBytesInMemoryOverheadCheck,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("rejectionPolicy") RejectionPolicyFactory rejectionPolicyFactory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("shardSpec") ShardSpec shardSpec,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      @JsonProperty("persistThreadPriority") int persistThreadPriority,
      @JsonProperty("mergeThreadPriority") int mergeThreadPriority,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("alertTimeout") Long alertTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("dedupColumn") @Nullable String dedupColumn,
      @JsonProperty("numPersistThreads") @Nullable Integer numPersistThreads
  )
  {
    this(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        intermediatePersistPeriod,
        windowPeriod,
        null,
        null,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        persistThreadPriority,
        mergeThreadPriority,
        reportParseExceptions,
        handoffConditionTimeout,
        alertTimeout,
        segmentWriteOutMediumFactory,
        dedupColumn,
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
  public Period getIntermediatePersistPeriod()
  {
    return intermediatePersistPeriod;
  }

  @JsonProperty
  public Period getWindowPeriod()
  {
    return windowPeriod;
  }

  @Override
  public File getBasePersistDirectory()
  {
    return Preconditions.checkNotNull(basePersistDirectory, "basePersistDirectory not set");
  }

  public VersioningPolicy getVersioningPolicy()
  {
    return Preconditions.checkNotNull(versioningPolicy, "versioningPolicy not set");
  }

  @JsonProperty("rejectionPolicy")
  public RejectionPolicyFactory getRejectionPolicyFactory()
  {
    return rejectionPolicyFactory;
  }

  @Override
  @JsonProperty
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  @Override
  public PartitionsSpec getPartitionsSpec()
  {
    throw new UnsupportedOperationException();
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

  @JsonProperty
  public int getPersistThreadPriority()
  {
    return this.persistThreadPriority;
  }

  @JsonProperty
  public int getMergeThreadPriority()
  {
    return this.mergeThreadPriority;
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
  @Nullable
  public String getDedupColumn()
  {
    return dedupColumn;
  }

  @Override
  @JsonProperty
  public int getNumPersistThreads()
  {
    return numPersistThreads;
  }

  public RealtimeTuningConfig withVersioningPolicy(VersioningPolicy policy)
  {
    return new RealtimeTuningConfig(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        intermediatePersistPeriod,
        windowPeriod,
        basePersistDirectory,
        policy,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        persistThreadPriority,
        mergeThreadPriority,
        reportParseExceptions,
        handoffConditionTimeout,
        alertTimeout,
        segmentWriteOutMediumFactory,
        dedupColumn,
        numPersistThreads
    );
  }

  @Override
  public RealtimeTuningConfig withBasePersistDirectory(File dir)
  {
    return new RealtimeTuningConfig(
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        skipBytesInMemoryOverheadCheck,
        intermediatePersistPeriod,
        windowPeriod,
        dir,
        versioningPolicy,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        persistThreadPriority,
        mergeThreadPriority,
        reportParseExceptions,
        handoffConditionTimeout,
        alertTimeout,
        segmentWriteOutMediumFactory,
        dedupColumn,
        numPersistThreads
    );
  }
}
