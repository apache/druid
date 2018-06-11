/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.segment.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.io.Files;
import io.druid.java.util.common.ISE;
import io.druid.segment.IndexSpec;
import io.druid.segment.realtime.appenderator.AppenderatorConfig;
import io.druid.segment.realtime.plumber.IntervalStartVersioningPolicy;
import io.druid.segment.realtime.plumber.RejectionPolicyFactory;
import io.druid.segment.realtime.plumber.ServerTimeRejectionPolicyFactory;
import io.druid.segment.realtime.plumber.VersioningPolicy;
import io.druid.segment.writeout.SegmentWriteOutMediumFactory;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;

/**
 */
public class RealtimeTuningConfig implements TuningConfig, AppenderatorConfig
{
  private static final int defaultMaxRowsInMemory = TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY;
  private static final Period defaultIntermediatePersistPeriod = new Period("PT10M");
  private static final Period defaultWindowPeriod = new Period("PT10M");
  private static final VersioningPolicy defaultVersioningPolicy = new IntervalStartVersioningPolicy();
  private static final RejectionPolicyFactory defaultRejectionPolicyFactory = new ServerTimeRejectionPolicyFactory();
  private static final int defaultMaxPendingPersists = 0;
  private static final ShardSpec defaultShardSpec = NoneShardSpec.instance();
  private static final IndexSpec defaultIndexSpec = new IndexSpec();
  private static final Boolean defaultReportParseExceptions = Boolean.FALSE;
  private static final long defaultHandoffConditionTimeout = 0;
  private static final long defaultAlertTimeout = 0;
  private static final String defaultDedupColumn = null;

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
        defaultMaxRowsInMemory,
        0L,
        defaultIntermediatePersistPeriod,
        defaultWindowPeriod,
        basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory,
        defaultVersioningPolicy,
        defaultRejectionPolicyFactory,
        defaultMaxPendingPersists,
        defaultShardSpec,
        defaultIndexSpec,
        true,
        0,
        0,
        defaultReportParseExceptions,
        defaultHandoffConditionTimeout,
        defaultAlertTimeout,
        null,
        defaultDedupColumn
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
    this.maxRowsInMemory = maxRowsInMemory == null ? defaultMaxRowsInMemory : maxRowsInMemory;
    // initializing this to 0, it will be lazily initialized to a value
    // @see server.src.main.java.io.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.intermediatePersistPeriod = intermediatePersistPeriod == null
                                     ? defaultIntermediatePersistPeriod
                                     : intermediatePersistPeriod;
    this.windowPeriod = windowPeriod == null ? defaultWindowPeriod : windowPeriod;
    this.basePersistDirectory = basePersistDirectory == null ? createNewBasePersistDirectory() : basePersistDirectory;
    this.versioningPolicy = versioningPolicy == null ? defaultVersioningPolicy : versioningPolicy;
    this.rejectionPolicyFactory = rejectionPolicyFactory == null
                                  ? defaultRejectionPolicyFactory
                                  : rejectionPolicyFactory;
    this.maxPendingPersists = maxPendingPersists == null ? defaultMaxPendingPersists : maxPendingPersists;
    this.shardSpec = shardSpec == null ? defaultShardSpec : shardSpec;
    this.indexSpec = indexSpec == null ? defaultIndexSpec : indexSpec;
    this.mergeThreadPriority = mergeThreadPriority;
    this.persistThreadPriority = persistThreadPriority;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? defaultReportParseExceptions
                                 : reportParseExceptions;
    this.handoffConditionTimeout = handoffConditionTimeout == null
                                   ? defaultHandoffConditionTimeout
                                   : handoffConditionTimeout;
    Preconditions.checkArgument(this.handoffConditionTimeout >= 0, "handoffConditionTimeout must be >= 0");

    this.alertTimeout = alertTimeout == null ? defaultAlertTimeout : alertTimeout;
    Preconditions.checkArgument(this.alertTimeout >= 0, "alertTimeout must be >= 0");
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
    this.dedupColumn = dedupColumn == null ? defaultDedupColumn : dedupColumn;
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
