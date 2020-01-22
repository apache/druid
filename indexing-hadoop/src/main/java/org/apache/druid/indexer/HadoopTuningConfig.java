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

package org.apache.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.DimensionBasedPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("hadoop")
public class HadoopTuningConfig implements TuningConfig
{
  private static final DimensionBasedPartitionsSpec DEFAULT_PARTITIONS_SPEC = HashedPartitionsSpec.defaultSpec();
  private static final Map<Long, List<HadoopyShardSpec>> DEFAULT_SHARD_SPECS = ImmutableMap.of();
  private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  private static final int DEFAULT_ROW_FLUSH_BOUNDARY = TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY;
  private static final boolean DEFAULT_USE_COMBINER = false;
  private static final int DEFAULT_NUM_BACKGROUND_PERSIST_THREADS = 0;

  public static HadoopTuningConfig makeDefaultTuningConfig()
  {
    return new HadoopTuningConfig(
        null,
        DateTimes.nowUtc().toString(),
        DEFAULT_PARTITIONS_SPEC,
        DEFAULT_SHARD_SPECS,
        DEFAULT_INDEX_SPEC,
        DEFAULT_INDEX_SPEC,
        DEFAULT_ROW_FLUSH_BOUNDARY,
        0L,
        false,
        true,
        false,
        false,
        null,
        false,
        false,
        null,
        true,
        DEFAULT_NUM_BACKGROUND_PERSIST_THREADS,
        false,
        false,
        null,
        null,
        null,
        null
    );
  }
  @Nullable
  private final String workingPath;
  private final String version;
  private final DimensionBasedPartitionsSpec partitionsSpec;
  private final Map<Long, List<HadoopyShardSpec>> shardSpecs;
  private final IndexSpec indexSpec;
  private final IndexSpec indexSpecForIntermediatePersists;
  private final int rowFlushBoundary;
  private final long maxBytesInMemory;
  private final boolean leaveIntermediate;
  private final boolean cleanupOnFailure;
  private final boolean overwriteFiles;
  private final boolean ignoreInvalidRows;
  private final Map<String, String> jobProperties;
  private final boolean combineText;
  private final boolean useCombiner;
  private final int numBackgroundPersistThreads;
  private final boolean forceExtendableShardSpecs;
  private final boolean useExplicitVersion;
  private final List<String> allowedHadoopPrefix;
  private final boolean logParseExceptions;
  private final int maxParseExceptions;
  private final boolean useYarnRMJobStatusFallback;

  @JsonCreator
  public HadoopTuningConfig(
      final @JsonProperty("workingPath") @Nullable String workingPath,
      final @JsonProperty("version") @Nullable String version,
      final @JsonProperty("partitionsSpec") @Nullable DimensionBasedPartitionsSpec partitionsSpec,
      final @JsonProperty("shardSpecs") @Nullable Map<Long, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      final @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      final @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      final @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") @Nullable Boolean cleanupOnFailure,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @Deprecated @JsonProperty("ignoreInvalidRows") @Nullable Boolean ignoreInvalidRows,
      final @JsonProperty("jobProperties") @Nullable Map<String, String> jobProperties,
      final @JsonProperty("combineText") boolean combineText,
      final @JsonProperty("useCombiner") @Nullable Boolean useCombiner,
      // See https://github.com/apache/druid/pull/1922
      final @JsonProperty("rowFlushBoundary") @Nullable Integer maxRowsInMemoryCOMPAT,
      // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
      final @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      final @JsonProperty("numBackgroundPersistThreads") @Nullable Integer numBackgroundPersistThreads,
      final @JsonProperty("forceExtendableShardSpecs") boolean forceExtendableShardSpecs,
      final @JsonProperty("useExplicitVersion") boolean useExplicitVersion,
      final @JsonProperty("allowedHadoopPrefix") @Nullable List<String> allowedHadoopPrefix,
      final @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      final @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      final @JsonProperty("useYarnRMJobStatusFallback") @Nullable Boolean useYarnRMJobStatusFallback
  )
  {
    this.workingPath = workingPath;
    this.version = version == null ? DateTimes.nowUtc().toString() : version;
    this.partitionsSpec = partitionsSpec == null ? DEFAULT_PARTITIONS_SPEC : partitionsSpec;
    this.shardSpecs = shardSpecs == null ? DEFAULT_SHARD_SPECS : shardSpecs;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.indexSpecForIntermediatePersists = indexSpecForIntermediatePersists == null ?
                                            this.indexSpec : indexSpecForIntermediatePersists;
    this.rowFlushBoundary = maxRowsInMemory == null ? maxRowsInMemoryCOMPAT == null
                                                      ? DEFAULT_ROW_FLUSH_BOUNDARY
                                                      : maxRowsInMemoryCOMPAT : maxRowsInMemory;
    // initializing this to 0, it will be lazily initialized to a value
    // @see server.src.main.java.org.apache.druid.segment.indexing.TuningConfigs#getMaxBytesInMemoryOrDefault(long)
    this.maxBytesInMemory = maxBytesInMemory == null ? 0 : maxBytesInMemory;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = cleanupOnFailure == null ? true : cleanupOnFailure;
    this.overwriteFiles = overwriteFiles;
    this.jobProperties = (jobProperties == null
                          ? ImmutableMap.of()
                          : ImmutableMap.copyOf(jobProperties));
    this.combineText = combineText;
    this.useCombiner = useCombiner == null ? DEFAULT_USE_COMBINER : useCombiner;
    this.numBackgroundPersistThreads = numBackgroundPersistThreads == null
                                       ? DEFAULT_NUM_BACKGROUND_PERSIST_THREADS
                                       : numBackgroundPersistThreads;
    this.forceExtendableShardSpecs = forceExtendableShardSpecs;
    Preconditions.checkArgument(this.numBackgroundPersistThreads >= 0, "Not support persistBackgroundCount < 0");
    this.useExplicitVersion = useExplicitVersion;
    this.allowedHadoopPrefix = allowedHadoopPrefix == null ? ImmutableList.of() : allowedHadoopPrefix;

    this.ignoreInvalidRows = ignoreInvalidRows == null ? false : ignoreInvalidRows;
    if (maxParseExceptions != null) {
      this.maxParseExceptions = maxParseExceptions;
    } else {
      if (!this.ignoreInvalidRows) {
        this.maxParseExceptions = 0;
      } else {
        this.maxParseExceptions = TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS;
      }
    }
    this.logParseExceptions = logParseExceptions == null ? TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS : logParseExceptions;

    this.useYarnRMJobStatusFallback = useYarnRMJobStatusFallback == null ? true : useYarnRMJobStatusFallback;
  }

  @Nullable
  @JsonProperty
  public String getWorkingPath()
  {
    return workingPath;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  @JsonProperty
  public DimensionBasedPartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  @JsonProperty
  public Map<Long, List<HadoopyShardSpec>> getShardSpecs()
  {
    return shardSpecs;
  }

  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @JsonProperty
  public IndexSpec getIndexSpecForIntermediatePersists()
  {
    return indexSpecForIntermediatePersists;
  }

  @JsonProperty("maxRowsInMemory")
  public int getRowFlushBoundary()
  {
    return rowFlushBoundary;
  }

  @JsonProperty
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  @JsonProperty
  public boolean isLeaveIntermediate()
  {
    return leaveIntermediate;
  }

  @JsonProperty
  public Boolean isCleanupOnFailure()
  {
    return cleanupOnFailure;
  }

  @JsonProperty
  public boolean isOverwriteFiles()
  {
    return overwriteFiles;
  }

  @JsonProperty
  public Boolean isIgnoreInvalidRows()
  {
    return ignoreInvalidRows;
  }

  @JsonProperty
  public Map<String, String> getJobProperties()
  {
    return jobProperties;
  }

  @JsonProperty
  public boolean isCombineText()
  {
    return combineText;
  }

  @JsonProperty
  public boolean getUseCombiner()
  {
    return useCombiner;
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
  public int getNumBackgroundPersistThreads()
  {
    return numBackgroundPersistThreads;
  }

  @JsonProperty
  public boolean isForceExtendableShardSpecs()
  {
    return forceExtendableShardSpecs;
  }

  @JsonProperty
  public boolean isUseExplicitVersion()
  {
    return useExplicitVersion;
  }

  @JsonProperty("allowedHadoopPrefix")
  public List<String> getUserAllowedHadoopPrefix()
  {
    // Just the user-specified list. More are added in HadoopDruidIndexerConfig.
    return allowedHadoopPrefix;
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
  public boolean isUseYarnRMJobStatusFallback()
  {
    return useYarnRMJobStatusFallback;
  }

  public HadoopTuningConfig withWorkingPath(String path)
  {
    return new HadoopTuningConfig(
        path,
        version,
        partitionsSpec,
        shardSpecs,
        indexSpec,
        indexSpecForIntermediatePersists,
        rowFlushBoundary,
        maxBytesInMemory,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        useCombiner,
        null,
        true,
        numBackgroundPersistThreads,
        forceExtendableShardSpecs,
        useExplicitVersion,
        allowedHadoopPrefix,
        logParseExceptions,
        maxParseExceptions,
        useYarnRMJobStatusFallback
    );
  }

  public HadoopTuningConfig withVersion(String ver)
  {
    return new HadoopTuningConfig(
        workingPath,
        ver,
        partitionsSpec,
        shardSpecs,
        indexSpec,
        indexSpecForIntermediatePersists,
        rowFlushBoundary,
        maxBytesInMemory,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        useCombiner,
        null,
        true,
        numBackgroundPersistThreads,
        forceExtendableShardSpecs,
        useExplicitVersion,
        allowedHadoopPrefix,
        logParseExceptions,
        maxParseExceptions,
        useYarnRMJobStatusFallback
    );
  }

  public HadoopTuningConfig withShardSpecs(Map<Long, List<HadoopyShardSpec>> specs)
  {
    return new HadoopTuningConfig(
        workingPath,
        version,
        partitionsSpec,
        specs,
        indexSpec,
        indexSpecForIntermediatePersists,
        rowFlushBoundary,
        maxBytesInMemory,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        useCombiner,
        null,
        true,
        numBackgroundPersistThreads,
        forceExtendableShardSpecs,
        useExplicitVersion,
        allowedHadoopPrefix,
        logParseExceptions,
        maxParseExceptions,
        useYarnRMJobStatusFallback
    );
  }
}
