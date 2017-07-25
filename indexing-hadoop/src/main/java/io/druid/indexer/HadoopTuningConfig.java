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

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.indexer.partitions.HashedPartitionsSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.segment.IndexSpec;
import io.druid.segment.indexing.TuningConfig;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("hadoop")
public class HadoopTuningConfig implements TuningConfig
{
  private static final PartitionsSpec DEFAULT_PARTITIONS_SPEC = HashedPartitionsSpec.makeDefaultHashedPartitionsSpec();
  private static final Map<Long, List<HadoopyShardSpec>> DEFAULT_SHARD_SPECS = ImmutableMap.of();
  private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
  private static final int DEFAULT_ROW_FLUSH_BOUNDARY = 75000;
  private static final boolean DEFAULT_USE_COMBINER = false;
  private static final int DEFAULT_NUM_BACKGROUND_PERSIST_THREADS = 0;

  public static HadoopTuningConfig makeDefaultTuningConfig()
  {
    return new HadoopTuningConfig(
        null,
        new DateTime().toString(),
        DEFAULT_PARTITIONS_SPEC,
        DEFAULT_SHARD_SPECS,
        DEFAULT_INDEX_SPEC,
        DEFAULT_ROW_FLUSH_BOUNDARY,
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
        null
    );
  }

  private final String workingPath;
  private final String version;
  private final PartitionsSpec partitionsSpec;
  private final Map<Long, List<HadoopyShardSpec>> shardSpecs;
  private final IndexSpec indexSpec;
  private final int rowFlushBoundary;
  private final boolean leaveIntermediate;
  private final Boolean cleanupOnFailure;
  private final boolean overwriteFiles;
  private final boolean ignoreInvalidRows;
  private final Map<String, String> jobProperties;
  private final boolean combineText;
  private final boolean useCombiner;
  private final int numBackgroundPersistThreads;
  private final boolean forceExtendableShardSpecs;
  private final boolean useExplicitVersion;
  private final List<String> allowedHadoopPrefix;

  @JsonCreator
  public HadoopTuningConfig(
      final @JsonProperty("workingPath") String workingPath,
      final @JsonProperty("version") String version,
      final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      final @JsonProperty("shardSpecs") Map<Long, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("indexSpec") IndexSpec indexSpec,
      final @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") Boolean cleanupOnFailure,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
      final @JsonProperty("jobProperties") Map<String, String> jobProperties,
      final @JsonProperty("combineText") boolean combineText,
      final @JsonProperty("useCombiner") Boolean useCombiner,
      // See https://github.com/druid-io/druid/pull/1922
      final @JsonProperty("rowFlushBoundary") Integer maxRowsInMemoryCOMPAT,
      // This parameter is left for compatibility when reading existing configs, to be removed in Druid 0.12.
      final @JsonProperty("buildV9Directly") Boolean buildV9Directly,
      final @JsonProperty("numBackgroundPersistThreads") Integer numBackgroundPersistThreads,
      final @JsonProperty("forceExtendableShardSpecs") boolean forceExtendableShardSpecs,
      final @JsonProperty("useExplicitVersion") boolean useExplicitVersion,
      final @JsonProperty("allowedHadoopPrefix") List<String> allowedHadoopPrefix
  )
  {
    this.workingPath = workingPath;
    this.version = version == null ? new DateTime().toString() : version;
    this.partitionsSpec = partitionsSpec == null ? DEFAULT_PARTITIONS_SPEC : partitionsSpec;
    this.shardSpecs = shardSpecs == null ? DEFAULT_SHARD_SPECS : shardSpecs;
    this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
    this.rowFlushBoundary = maxRowsInMemory == null ? maxRowsInMemoryCOMPAT == null
                                                      ? DEFAULT_ROW_FLUSH_BOUNDARY
                                                      : maxRowsInMemoryCOMPAT : maxRowsInMemory;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = cleanupOnFailure == null ? true : cleanupOnFailure;
    this.overwriteFiles = overwriteFiles;
    this.ignoreInvalidRows = ignoreInvalidRows;
    this.jobProperties = (jobProperties == null
                          ? ImmutableMap.<String, String>of()
                          : ImmutableMap.copyOf(jobProperties));
    this.combineText = combineText;
    this.useCombiner = useCombiner == null ? DEFAULT_USE_COMBINER : useCombiner.booleanValue();
    this.numBackgroundPersistThreads = numBackgroundPersistThreads == null
                                       ? DEFAULT_NUM_BACKGROUND_PERSIST_THREADS
                                       : numBackgroundPersistThreads;
    this.forceExtendableShardSpecs = forceExtendableShardSpecs;
    Preconditions.checkArgument(this.numBackgroundPersistThreads >= 0, "Not support persistBackgroundCount < 0");
    this.useExplicitVersion = useExplicitVersion;
    this.allowedHadoopPrefix = allowedHadoopPrefix == null ? ImmutableList.of() : allowedHadoopPrefix;
  }

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
  public PartitionsSpec getPartitionsSpec()
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

  @JsonProperty("maxRowsInMemory")
  public int getRowFlushBoundary()
  {
    return rowFlushBoundary;
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
  public boolean isIgnoreInvalidRows()
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

  public HadoopTuningConfig withWorkingPath(String path)
  {
    return new HadoopTuningConfig(
        path,
        version,
        partitionsSpec,
        shardSpecs,
        indexSpec,
        rowFlushBoundary,
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
        allowedHadoopPrefix
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
        rowFlushBoundary,
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
        allowedHadoopPrefix
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
        rowFlushBoundary,
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
        allowedHadoopPrefix
    );
  }

  @JsonProperty("allowedHadoopPrefix")
  public List<String> getUserAllowedHadoopPrefix()
  {
    // Just the user-specified list. More are added in HadoopDruidIndexerConfig.
    return allowedHadoopPrefix;
  }
}
