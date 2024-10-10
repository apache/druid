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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.Configs;
import org.apache.druid.indexer.partitions.DimensionBasedPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.incremental.AppendableIndexSpec;
import org.apache.druid.segment.indexing.TuningConfig;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("hadoop")
public class HadoopTuningConfig implements TuningConfig
{
  public static final int DEFAULT_DETERMINE_PARTITIONS_SAMPLING_FACTOR = 1;

  private static final DimensionBasedPartitionsSpec DEFAULT_PARTITIONS_SPEC = HashedPartitionsSpec.defaultSpec();
  private static final Map<Long, List<HadoopyShardSpec>> DEFAULT_SHARD_SPECS = ImmutableMap.of();
  private static final IndexSpec DEFAULT_INDEX_SPEC = IndexSpec.DEFAULT;
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
        DEFAULT_APPENDABLE_INDEX,
        DEFAULT_MAX_ROWS_IN_MEMORY_BATCH,
        0L,
        false,
        false,
        true,
        false,
        false,
        null,
        false,
        false,
        null,
        DEFAULT_NUM_BACKGROUND_PERSIST_THREADS,
        false,
        false,
        null,
        null,
        null,
        null,
        null,
        DEFAULT_DETERMINE_PARTITIONS_SAMPLING_FACTOR
    );
  }
  @Nullable
  private final String workingPath;
  private final String version;
  private final DimensionBasedPartitionsSpec partitionsSpec;
  private final Map<Long, List<HadoopyShardSpec>> shardSpecs;
  private final IndexSpec indexSpec;
  private final IndexSpec indexSpecForIntermediatePersists;
  private final AppendableIndexSpec appendableIndexSpec;
  private final int maxRowsInMemory;
  private final long maxBytesInMemory;
  private final boolean useMaxMemoryEstimates;
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
  private final long awaitSegmentAvailabilityTimeoutMillis;
  // The sample parameter is only used for range partition spec now. When using range
  // partition spec, we need launch many mapper and one reducer to do global sorting and
  // find the upper and lower bound for every segment. This mr job may cost a lot of time
  // if the input data is large. So we can sample the input data and make the mr job run
  // faster. After all, we don't need a segment size which exactly equals targetRowsPerSegment.
  // For example, if we ingest 10,000,000,000 rows and the targetRowsPerSegment is 5,000,000,
  // we can sample by 500, so the mr job need only process 20,000,000 rows, this helps save
  // a lot of time.
  private final int determinePartitionsSamplingFactor;

  @JsonCreator
  public HadoopTuningConfig(
      final @JsonProperty("workingPath") @Nullable String workingPath,
      final @JsonProperty("version") @Nullable String version,
      final @JsonProperty("partitionsSpec") @Nullable DimensionBasedPartitionsSpec partitionsSpec,
      final @JsonProperty("shardSpecs") @Nullable Map<Long, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
      final @JsonProperty("indexSpecForIntermediatePersists") @Nullable IndexSpec indexSpecForIntermediatePersists,
      final @JsonProperty("appendableIndexSpec") @Nullable AppendableIndexSpec appendableIndexSpec,
      final @JsonProperty("maxRowsInMemory") @Nullable Integer maxRowsInMemory,
      final @JsonProperty("maxBytesInMemory") @Nullable Long maxBytesInMemory,
      final @JsonProperty("useMaxMemoryEstimates") @Nullable Boolean useMaxMemoryEstimates,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") @Nullable Boolean cleanupOnFailure,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @Deprecated @JsonProperty("ignoreInvalidRows") @Nullable Boolean ignoreInvalidRows,
      final @JsonProperty("jobProperties") @Nullable Map<String, String> jobProperties,
      final @JsonProperty("combineText") boolean combineText,
      final @JsonProperty("useCombiner") @Nullable Boolean useCombiner,
      // See https://github.com/apache/druid/pull/1922
      final @JsonProperty("rowFlushBoundary") @Nullable Integer maxRowsInMemoryCOMPAT,
      final @JsonProperty("numBackgroundPersistThreads") @Nullable Integer numBackgroundPersistThreads,
      final @JsonProperty("forceExtendableShardSpecs") boolean forceExtendableShardSpecs,
      final @JsonProperty("useExplicitVersion") boolean useExplicitVersion,
      final @JsonProperty("allowedHadoopPrefix") @Nullable List<String> allowedHadoopPrefix,
      final @JsonProperty("logParseExceptions") @Nullable Boolean logParseExceptions,
      final @JsonProperty("maxParseExceptions") @Nullable Integer maxParseExceptions,
      final @JsonProperty("useYarnRMJobStatusFallback") @Nullable Boolean useYarnRMJobStatusFallback,
      final @JsonProperty("awaitSegmentAvailabilityTimeoutMillis") @Nullable Long awaitSegmentAvailabilityTimeoutMillis,
      final @JsonProperty("determinePartitionsSamplingFactor") @Nullable Integer determinePartitionsSamplingFactor
  )
  {
    this.workingPath = workingPath;
    this.version = Configs.valueOrDefault(version, DateTimes.nowUtc().toString());
    this.partitionsSpec = Configs.valueOrDefault(partitionsSpec, DEFAULT_PARTITIONS_SPEC);
    this.shardSpecs = Configs.valueOrDefault(shardSpecs, DEFAULT_SHARD_SPECS);
    this.indexSpec = Configs.valueOrDefault(indexSpec, DEFAULT_INDEX_SPEC);
    this.indexSpecForIntermediatePersists = Configs.valueOrDefault(
        indexSpecForIntermediatePersists,
        this.indexSpec
    );
    this.maxRowsInMemory = Configs.valueOrDefault(
        maxRowsInMemory,
        Configs.valueOrDefault(maxRowsInMemoryCOMPAT, DEFAULT_MAX_ROWS_IN_MEMORY_BATCH)
    );
    this.useMaxMemoryEstimates = Configs.valueOrDefault(useMaxMemoryEstimates, false);
    this.appendableIndexSpec = Configs.valueOrDefault(appendableIndexSpec, DEFAULT_APPENDABLE_INDEX);
    // initializing this to 0, it will be lazily initialized to a value
    // @see #getMaxBytesInMemoryOrDefault()
    this.maxBytesInMemory = Configs.valueOrDefault(maxBytesInMemory, 0);
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = Configs.valueOrDefault(cleanupOnFailure, true);
    this.overwriteFiles = overwriteFiles;
    this.jobProperties = (jobProperties == null
                          ? ImmutableMap.of()
                          : ImmutableMap.copyOf(jobProperties));
    this.combineText = combineText;
    this.useCombiner = Configs.valueOrDefault(useCombiner, DEFAULT_USE_COMBINER);
    this.numBackgroundPersistThreads = Configs.valueOrDefault(
        numBackgroundPersistThreads,
        DEFAULT_NUM_BACKGROUND_PERSIST_THREADS
    );
    this.forceExtendableShardSpecs = forceExtendableShardSpecs;
    Preconditions.checkArgument(this.numBackgroundPersistThreads >= 0, "Not support persistBackgroundCount < 0");
    this.useExplicitVersion = useExplicitVersion;
    this.allowedHadoopPrefix = Configs.valueOrDefault(allowedHadoopPrefix, Collections.emptyList());

    this.ignoreInvalidRows = Configs.valueOrDefault(ignoreInvalidRows, false);
    this.maxParseExceptions = Configs.valueOrDefault(
        maxParseExceptions,
        this.ignoreInvalidRows ? TuningConfig.DEFAULT_MAX_PARSE_EXCEPTIONS : 0
    );
    this.logParseExceptions = Configs.valueOrDefault(logParseExceptions, TuningConfig.DEFAULT_LOG_PARSE_EXCEPTIONS);
    this.useYarnRMJobStatusFallback = Configs.valueOrDefault(useYarnRMJobStatusFallback, true);

    if (awaitSegmentAvailabilityTimeoutMillis == null || awaitSegmentAvailabilityTimeoutMillis < 0) {
      this.awaitSegmentAvailabilityTimeoutMillis = DEFAULT_AWAIT_SEGMENT_AVAILABILITY_TIMEOUT_MILLIS;
    } else {
      this.awaitSegmentAvailabilityTimeoutMillis = awaitSegmentAvailabilityTimeoutMillis;
    }
    if (determinePartitionsSamplingFactor == null || determinePartitionsSamplingFactor < 1) {
      this.determinePartitionsSamplingFactor = 1;
    } else {
      this.determinePartitionsSamplingFactor = determinePartitionsSamplingFactor;
    }
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

  @Override
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

  @Override
  @JsonProperty
  public IndexSpec getIndexSpec()
  {
    return indexSpec;
  }

  @Override
  @JsonProperty
  public IndexSpec getIndexSpecForIntermediatePersists()
  {
    return indexSpecForIntermediatePersists;
  }

  @JsonProperty
  @Override
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

  @JsonProperty
  @Override
  public long getMaxBytesInMemory()
  {
    return maxBytesInMemory;
  }

  @JsonProperty
  public boolean isUseMaxMemoryEstimates()
  {
    return useMaxMemoryEstimates;
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

  @JsonProperty
  public long getAwaitSegmentAvailabilityTimeoutMillis()
  {
    return awaitSegmentAvailabilityTimeoutMillis;
  }

  @JsonProperty
  public int getDeterminePartitionsSamplingFactor()
  {
    return determinePartitionsSamplingFactor;
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
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        useMaxMemoryEstimates,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        useCombiner,
        null,
        numBackgroundPersistThreads,
        forceExtendableShardSpecs,
        useExplicitVersion,
        allowedHadoopPrefix,
        logParseExceptions,
        maxParseExceptions,
        useYarnRMJobStatusFallback,
        awaitSegmentAvailabilityTimeoutMillis,
        determinePartitionsSamplingFactor
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
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        useMaxMemoryEstimates,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        useCombiner,
        null,
        numBackgroundPersistThreads,
        forceExtendableShardSpecs,
        useExplicitVersion,
        allowedHadoopPrefix,
        logParseExceptions,
        maxParseExceptions,
        useYarnRMJobStatusFallback,
        awaitSegmentAvailabilityTimeoutMillis,
        determinePartitionsSamplingFactor
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
        appendableIndexSpec,
        maxRowsInMemory,
        maxBytesInMemory,
        useMaxMemoryEstimates,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        useCombiner,
        null,
        numBackgroundPersistThreads,
        forceExtendableShardSpecs,
        useExplicitVersion,
        allowedHadoopPrefix,
        logParseExceptions,
        maxParseExceptions,
        useYarnRMJobStatusFallback,
        awaitSegmentAvailabilityTimeoutMillis,
        determinePartitionsSamplingFactor
    );
  }
}
