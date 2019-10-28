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
import com.google.common.io.Files;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.IndexSpec;
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

/**
 */
public class RealtimeTuningConfig implements TuningConfig, AppenderatorConfig
{
  private static final int DEFAULT_MAX_ROWS_IN_MEMORY = TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY;
  private static final Period DEFAULT_INTERMEDIATE_PERSIST_PERIOD = new Period("PT10M");
  private static final Period DEFAULT_WINDOW_PERIOD = new Period("PT10M");
  private static final VersioningPolicy DEFAULT_VERSIONING_POLICY = new IntervalStartVersioningPolicy();
  private static final RejectionPolicyFactory DEFAULT_REJECTION_POLICY_FACTORY = new ServerTimeRejectionPolicyFactory();
  private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
  private static final ShardSpec DEFAULT_SHARD_SPEC = new NumberedShardSpec(0, 1);
  private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  private static final Boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = Boolean.FALSE;
  private static final long DEFAULT_HANDOFF_CONDITION_TIMEOUT = 0;
  private static final long DEFAULT_ALERT_TIMEOUT = 0;
  private static final String DEFAULT_DEDUP_COLUMN = null;

  private static File createNewBasePersistDirectory()
  {
    try {
      return Files.createTempDir();
    }
    catch (IllegalStateException e) {
      String messageTemplate = "Failed to create temporary directory in [%s]! " +
              "Make sure the `java.io.tmpdir` property is set to an existing and writable directory " +
              "with enough free space.";
      throw new ISE(e, messageTemplate, System.getProperty("java.io.tmpdir"));
    }
  }

  // Might make sense for this to be a builder
  public static RealtimeTuningConfig makeDefaultTuningConfig(final @Nullable File basePersistDirectory)
  {
    return new RealtimeTuningConfig(
        DEFAULT_MAX_ROWS_IN_MEMORY,
        0L,
        DEFAULT_INTERMEDIATE_PERSIST_PERIOD,
        DEFAULT_WINDOW_PERIOD,
        basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory,
        DEFAULT_VERSIONING_POLICY,
        DEFAULT_REJECTION_POLICY_FACTORY,
        DEFAULT_MAX_PENDING_PERSISTS,
        DEFAULT_SHARD_SPEC,
        DEFAULT_INDEX_SPEC,
        DEFAULT_INDEX_SPEC,
        true,
        0,
        0,
        DEFAULT_REPORT_PARSE_EXCEPTIONS,
        DEFAULT_HANDOFF_CONDITION_TIMEOUT,
        DEFAULT_ALERT_TIMEOUT,
        null,
        DEFAULT_DEDUP_COLUMN
    );
  }

  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
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

  @JsonCreator
  public RealtimeTuningConfig(
      @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") Long maxBytesInMemory,
      @JsonProperty("intermediatePersistPeriod") Period intermediatePersistPeriod,
      @JsonProperty("windowPeriod") Period windowPeriod,
      @JsonProperty("basePersistDirectory") File basePersistDirectory,
      @JsonProperty("versioningPolicy") VersioningPolicy versioningPolicy,
      @JsonProperty("rejectionPolicy") RejectionPolicyFactory rejectionPolicyFactory,
      @JsonProperty("maxPendingPersists") Integer maxPendingPersists,
      @JsonProperty("shardSpec") ShardSpec shardSpec,
      @JsonProperty("indexSpec") IndexSpec indexSpec,
      @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      @JsonProperty("persistThreadPriority") int persistThreadPriority,
      @JsonProperty("mergeThreadPriority") int mergeThreadPriority,
      @JsonProperty("reportParseExceptions") Boolean reportParseExceptions,
      @JsonProperty("handoffConditionTimeout") Long handoffConditionTimeout,
      @JsonProperty("alertTimeout") Long alertTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("dedupColumn") @Nullable String dedupColumn
  )
  {
    this.maxRowsInMemory = maxRowsInMemory == null ? DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
    // initializing this to 0, it will be lazily initialized to a value
    // @see server.src.main.java.org.apache.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? DEFAULT_INTERMEDIATE_PERSIST_PERIOD
                                     : intermediatePersistPeriod;
    this.windowPeriod = windowPeriod == null ? DEFAULT_WINDOW_PERIOD : windowPeriod;
    this.basePersistDirectory = basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory;
    this.versioningPolicy = versioningPolicy == null ? DEFAULT_VERSIONING_POLICY : versioningPolicy;
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
  }

  @Override
  @JsonProperty
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @Override
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
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
  @JsonProperty
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @JsonProperty
  public VersioningPolicy getVersioningPolicy()
  {
    return versioningPolicy;
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

  /**
   * Always returns true, doesn't affect the version being built.
   */
  @Deprecated
  @JsonProperty
  public Boolean getBuildV9Directly()
  {
    return true;
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

  public RealtimeTuningConfig withVersioningPolicy(VersioningPolicy policy)
  {
    return new RealtimeTuningConfig(
        maxRowsInMemory,
        maxBytesInMemory,
        intermediatePersistPeriod,
        windowPeriod,
        basePersistDirectory,
        policy,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        true,
        persistThreadPriority,
        mergeThreadPriority,
        reportParseExceptions,
        handoffConditionTimeout,
        alertTimeout,
        segmentWriteOutMediumFactory,
        dedupColumn
    );
  }

  @Override
  public RealtimeTuningConfig withBasePersistDirectory(File dir)
  {
    return new RealtimeTuningConfig(
        maxRowsInMemory,
        maxBytesInMemory,
        intermediatePersistPeriod,
        windowPeriod,
        dir,
        versioningPolicy,
        rejectionPolicyFactory,
        maxPendingPersists,
        shardSpec,
        indexSpec,
        indexSpecForIntermediatePersists,
        true,
        persistThreadPriority,
        mergeThreadPriority,
        reportParseExceptions,
        handoffConditionTimeout,
        alertTimeout,
        segmentWriteOutMediumFactory,
        dedupColumn
    );
  }
}
