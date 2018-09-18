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
package org.apache.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;
import org.apache.druid.segment.realtime.appenderator.AppenderatorConfig;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Period;

import javax.annotation.Nullable;
import java.io.File;
import java.util.Objects;

@JsonTypeName("index")
public class IndexTuningConfig implements TuningConfig, AppenderatorConfig
{
  private static final int DEFAULT_MAX_TOTAL_ROWS = 20_000_000;
  private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  private static final int DEFAULT_MAX_PENDING_PERSISTS = 0;
  private static final boolean DEFAULT_FORCE_EXTENDABLE_SHARD_SPECS = false;
  private static final boolean DEFAULT_GUARANTEE_ROLLUP = false;
  private static final boolean DEFAULT_REPORT_PARSE_EXCEPTIONS = false;
  private static final long DEFAULT_PUSH_TIMEOUT = 0;

  static final int DEFAULT_TARGET_PARTITION_SIZE = 5000000;

  private final Integer targetPartitionSize;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final Long maxTotalRows;
  private final Integer numShards;
  private final IndexSpec indexSpec;
  private final File basePersistDirectory;
  private final int maxPendingPersists;

  /**
   * This flag is to force to always use an extendableShardSpec (like {@link NumberedShardSpec} even if
   * {@link #forceGuaranteedRollup} is set.
   */
  private final boolean forceExtendableShardSpecs;

  /**
   * This flag is to force _perfect rollup mode_. {@link IndexTask} will scan the whole input data twice to 1) figure
   * out proper shard specs for each segment and 2) generate segments. Note that perfect rollup mode basically assumes
   * that no more data will be appended in the future. As a result, in perfect rollup mode, {@link NoneShardSpec} and
   * {@link HashBasedNumberedShardSpec} are used for a single shard and two or shards, respectively.
   */
  private final boolean forceGuaranteedRollup;
  private final boolean reportParseExceptions;
  private final long pushTimeout;
  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final int maxSavedParseExceptions;
  private final int numFilesPerMerge;

  @Nullable
  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  @JsonCreator
  public IndexTuningConfig(
      @JsonProperty("targetPartitionSize") @Nullable Integer targetPartitionSize,
      @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      @JsonProperty("maxTotalRows") @Nullable Long maxTotalRows,
      @JsonProperty("rowFlushBoundary") @Nullable Integer rowFlushBoundary_forBackCompatibility, // DEPRECATED
      @JsonProperty("numShards") @Nullable Integer numShards,
      @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      @JsonProperty("maxPendingPersists") @Nullable Integer maxPendingPersists,
      // This parameter is left for compatibility when reading existing JSONs, to be removed in Druid 0.12.
      @JsonProperty("buildV9Directly") @Nullable Boolean buildV9Directly,
      @JsonProperty("forceExtendableShardSpecs") @Nullable Boolean forceExtendableShardSpecs,
      @JsonProperty("forceGuaranteedRollup") @Nullable Boolean forceGuaranteedRollup,
      @Deprecated @JsonProperty("reportParseExceptions") @Nullable Boolean reportParseExceptions,
      @JsonProperty("publishTimeout") @Nullable Long publishTimeout, // deprecated
      @JsonProperty("pushTimeout") @Nullable Long pushTimeout,
      @JsonProperty("segmentWriteOutMediumFactory") @Nullable
          SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      @JsonProperty("maxSavedParseExceptions") @Nullable Integer maxSavedParseExceptions,
      @JsonProperty("numFilesPerMerge") @Nullable Integer numFilesPerMerge
  )
  {
    this(
        targetPartitionSize,
        maxRowsInMemory != null ? maxRowsInMemory : rowFlushBoundary_forBackCompatibility,
        maxBytesInMemory != null ? maxBytesInMemory : 0,
        maxTotalRows,
        numShards,
        indexSpec,
        maxPendingPersists,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        reportParseExceptions,
        pushTimeout != null ? pushTimeout : publishTimeout,
        null,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numFilesPerMerge
    );
  }

  private IndexTuningConfig(
      @Nullable Integer targetPartitionSize,
      @Nullable Integer maxRowsInMemory,
      @Nullable Long maxBytesInMemory,
      @Nullable Long maxTotalRows,
      @Nullable Integer numShards,
      @Nullable IndexSpec indexSpec,
      @Nullable Integer maxPendingPersists,
      @Nullable Boolean forceExtendableShardSpecs,
      @Nullable Boolean forceGuaranteedRollup,
      @Nullable Boolean reportParseExceptions,
      @Nullable Long pushTimeout,
      @Nullable File basePersistDirectory,
      @Nullable SegmentWriteOutMediumFactory segmentWriteOutMediumFactory,
      @Nullable Boolean logParseExceptions,
      @Nullable Integer maxParseExceptions,
      @Nullable Integer maxSavedParseExceptions,
      @Nullable Integer numFilesPerMerge
  )
  {
    Preconditions.checkArgument(
        targetPartitionSize == null || targetPartitionSize.equals(-1) || numShards == null || numShards.equals(-1),
        "targetPartitionSize and numShards cannot both be set"
    );

    this.targetPartitionSize = initializeTargetPartitionSize(numShards, targetPartitionSize);
    this.maxRowsInMemory = maxRowsInMemory == null ? TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY : maxRowsInMemory;
    // initializing this to 0, it will be lazily initialized to a value
    // @see server.src.main.java.org.apache.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.maxTotalRows = initializeMaxTotalRows(numShards, maxTotalRows);
    this.numShards = numShards == null || numShards.equals(-1) ? null : numShards;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.maxPendingPersists = maxPendingPersists == null ? DEFAULT_MAX_PENDING_PERSISTS : maxPendingPersists;
    this.forceExtendableShardSpecs = forceExtendableShardSpecs == null
                                     ? DEFAULT_FORCE_EXTENDABLE_SHARD_SPECS
                                     : forceExtendableShardSpecs;
    this.forceGuaranteedRollup = forceGuaranteedRollup == null ? DEFAULT_GUARANTEE_ROLLUP : forceGuaranteedRollup;
    this.reportParseExceptions = reportParseExceptions == null
                                 ? DEFAULT_REPORT_PARSE_EXCEPTIONS
                                 : reportParseExceptions;
    this.pushTimeout = pushTimeout == null ? DEFAULT_PUSH_TIMEOUT : pushTimeout;
    this.basePersistDirectory = basePersistDirectory;

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
    this.numFilesPerMerge = TuningConfig.validateAndGetNumFilesPerMerge(numFilesPerMerge);
  }

  private static Integer initializeTargetPartitionSize(Integer numShards, Integer targetPartitionSize)
  {
    if (numShards == null || numShards == -1) {
      return targetPartitionSize == null || targetPartitionSize.equals(-1)
             ? DEFAULT_TARGET_PARTITION_SIZE
             : targetPartitionSize;
    } else {
      return null;
    }
  }

  private static Long initializeMaxTotalRows(Integer numShards, Long maxTotalRows)
  {
    if (numShards == null || numShards == -1) {
      return maxTotalRows == null ? DEFAULT_MAX_TOTAL_ROWS : maxTotalRows;
    } else {
      return null;
    }
  }

  public IndexTuningConfig withBasePersistDirectory(File dir)
  {
    return new IndexTuningConfig(
        targetPartitionSize,
        maxRowsInMemory,
        maxBytesInMemory,
        maxTotalRows,
        numShards,
        indexSpec,
        maxPendingPersists,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        reportParseExceptions,
        pushTimeout,
        dir,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numFilesPerMerge
    );
  }

  @JsonProperty
  public Integer getTargetPartitionSize()
  {
    return targetPartitionSize;
  }

  @JsonProperty
  @Override
  public int getMaxRowsInMemory()
  {
    return maxRowsInMemory;
  }

  @JsonProperty
  @Override
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  @JsonProperty
  @Override
  @Nullable
  public Long getMaxTotalRows()
  {
    return maxTotalRows;
  }

  @JsonProperty
  public Integer getNumShards()
  {
    return numShards;
  }

  @JsonProperty
  @Override
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @Override
  public File getBasePersistDirectory()
  {
    return basePersistDirectory;
  }

  @JsonProperty
  @Override
  public int getMaxPendingPersists()
  {
    return maxPendingPersists;
  }

  /**
   * Always returns true, doesn't affect the version being built.
   */
  @Deprecated
  @JsonProperty
  public boolean isBuildV9Directly()
  {
    return true;
  }

  @JsonProperty
  public boolean isForceExtendableShardSpecs()
  {
    return forceExtendableShardSpecs;
  }

  @JsonProperty
  public boolean isForceGuaranteedRollup()
  {
    return forceGuaranteedRollup;
  }

  @JsonProperty
  @Override
  public boolean isReportParseExceptions()
  {
    return reportParseExceptions;
  }

  @JsonProperty
  public long getPushTimeout()
  {
    return pushTimeout;
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
  public Period getIntermediatePersistPeriod()
  {
    return new Period(Integer.MAX_VALUE); // intermediate persist doesn't make much sense for batch jobs
  }

  @Nullable
  @Override
  @JsonProperty
  public SegmentWriteOutMediumFactory getSegmentWriteOutMediumFactory()
  {
    return segmentWriteOutMediumFactory;
  }

  @Override
  public int getNumFilesPerMerge()
  {
    return numFilesPerMerge;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    IndexTuningConfig that = (IndexTuningConfig) o;
    return maxRowsInMemory == that.maxRowsInMemory &&
           Objects.equals(maxTotalRows, that.maxTotalRows) &&
           maxPendingPersists == that.maxPendingPersists &&
           forceExtendableShardSpecs == that.forceExtendableShardSpecs &&
           forceGuaranteedRollup == that.forceGuaranteedRollup &&
           reportParseExceptions == that.reportParseExceptions &&
           pushTimeout == that.pushTimeout &&
           Objects.equals(targetPartitionSize, that.targetPartitionSize) &&
           Objects.equals(numShards, that.numShards) &&
           Objects.equals(indexSpec, that.indexSpec) &&
           Objects.equals(basePersistDirectory, that.basePersistDirectory) &&
           Objects.equals(segmentWriteOutMediumFactory, that.segmentWriteOutMediumFactory) &&
           logParseExceptions == that.logParseExceptions &&
           maxParseExceptions == that.maxParseExceptions &&
           maxSavedParseExceptions == that.maxSavedParseExceptions &&
           numFilesPerMerge == that.numFilesPerMerge;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        targetPartitionSize,
        maxRowsInMemory,
        maxTotalRows,
        numShards,
        indexSpec,
        basePersistDirectory,
        maxPendingPersists,
        forceExtendableShardSpecs,
        forceGuaranteedRollup,
        reportParseExceptions,
        pushTimeout,
        segmentWriteOutMediumFactory,
        logParseExceptions,
        maxParseExceptions,
        maxSavedParseExceptions,
        numFilesPerMerge
    );
  }

  public static class Builder
  {
    private Integer targetPartitionSize;
    private Integer maxRowsInMemory;
    private Long maxBytesInMemory;
    private Long maxTotalRows;
    private Integer numShards;
    private IndexSpec indexSpec;
    private File basePersistDirectory;
    private Integer maxPendingPersists;
    private Boolean forceExtendableShardSpecs;
    private Boolean forceGuaranteedRollup;
    private Boolean reportParseExceptions;
    private Long pushTimeout;
    private Boolean logParseExceptions;
    private Integer maxParseExceptions;
    private Integer maxSavedParseExceptions;
    private Integer numFilesPerMerge;
    private SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

    public IndexTuningConfig build()
    {
      return new IndexTuningConfig(
          targetPartitionSize,
          maxRowsInMemory,
          maxBytesInMemory,
          maxTotalRows,
          null,
          numShards,
          indexSpec,
          maxPendingPersists,
          true,
          forceExtendableShardSpecs,
          forceGuaranteedRollup,
          reportParseExceptions,
          null,
          pushTimeout,
          segmentWriteOutMediumFactory,
          logParseExceptions,
          maxParseExceptions,
          maxSavedParseExceptions,
          numFilesPerMerge
      );
    }

    public Builder setTargetPartitionSize(int targetPartitionSize)
    {
      this.targetPartitionSize = targetPartitionSize;
      return this;
    }

    public Builder setMaxRowsInMemory(int maxRowsInMemory)
    {
      this.maxRowsInMemory = maxRowsInMemory;
      return this;
    }

    public Builder setMaxBytesInMemory(long maxBytesInMemory)
    {
      this.maxBytesInMemory = maxBytesInMemory;
      return this;
    }

    public Builder setMaxTotalRows(long maxTotalRows)
    {
      this.maxTotalRows = maxTotalRows;
      return this;
    }

    public Builder setNumShards(int numShards)
    {
      this.numShards = numShards;
      return this;
    }

    public Builder setIndexSpec(IndexSpec indexSpec)
    {
      this.indexSpec = indexSpec;
      return this;
    }

    public Builder setBasePersistDirectory(File basePersistDirectory)
    {
      this.basePersistDirectory = basePersistDirectory;
      return this;
    }

    public Builder setMaxPendingPersists(int maxPendingPersists)
    {
      this.maxPendingPersists = maxPendingPersists;
      return this;
    }

    public Builder setForceExtendableShardSpecs(boolean forceExtendableShardSpecs)
    {
      this.forceExtendableShardSpecs = forceExtendableShardSpecs;
      return this;
    }

    public Builder setForceGuaranteedRollup(boolean forceGuaranteedRollup)
    {
      this.forceGuaranteedRollup = forceGuaranteedRollup;
      return this;
    }

    public Builder setReportParseExceptions(boolean reportParseExceptions)
    {
      this.reportParseExceptions = reportParseExceptions;
      return this;
    }

    public Builder setPushTimeout(long pushTimeout)
    {
      this.pushTimeout = pushTimeout;
      return this;
    }

    public Builder setLogParseExceptions(boolean logParseExceptions)
    {
      this.logParseExceptions = logParseExceptions;
      return this;
    }

    public Builder setMaxParseExceptions(int maxParseExceptions)
    {
      this.maxParseExceptions = maxParseExceptions;
      return this;
    }

    public Builder setMaxSavedParseExceptions(int maxSavedParseExceptions)
    {
      this.maxSavedParseExceptions = maxSavedParseExceptions;
      return this;
    }

    public Builder setNumFilesPerMerge(int numFilesPerMerge)
    {
      this.numFilesPerMerge = numFilesPerMerge;
      return this;
    }

    public Builder setSegmentWriteOutMediumFactory(SegmentWriteOutMediumFactory factory)
    {
      this.segmentWriteOutMediumFactory = factory;
      return this;
    }
  }
}
