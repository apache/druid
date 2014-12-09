/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableMap;
import io.druid.indexer.partitions.HashedPartitionsSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.segment.indexing.TuningConfig;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
@JsonTypeName("hadoop")
public class HadoopTuningConfig implements TuningConfig
{
  private static final String DEFAULT_WORKING_PATH = "/tmp/druid-indexing";
  private static final PartitionsSpec DEFAULT_PARTITIONS_SPEC = HashedPartitionsSpec.makeDefaultHashedPartitionsSpec();
  private static final Map<DateTime, List<HadoopyShardSpec>> DEFAULT_SHARD_SPECS = ImmutableMap.<DateTime, List<HadoopyShardSpec>>of();
  private static final int DEFAULT_ROW_FLUSH_BOUNDARY = 80000;
  private static final int DEFAULT_BUFFER_SIZE = 128 * 1024 * 1024;
  private static final float DEFAULT_AGG_BUFFER_RATIO = 0.5f;

  public static HadoopTuningConfig makeDefaultTuningConfig()
  {
    return new HadoopTuningConfig(
        DEFAULT_WORKING_PATH,
        new DateTime().toString(),
        DEFAULT_PARTITIONS_SPEC,
        DEFAULT_SHARD_SPECS,
        DEFAULT_ROW_FLUSH_BOUNDARY,
        false,
        true,
        false,
        false,
        null,
        false,
        false,
        false,
        DEFAULT_BUFFER_SIZE,
        DEFAULT_AGG_BUFFER_RATIO
    );
  }

  private final String workingPath;
  private final String version;
  private final PartitionsSpec partitionsSpec;
  private final Map<DateTime, List<HadoopyShardSpec>> shardSpecs;
  private final int rowFlushBoundary;
  private final boolean leaveIntermediate;
  private final Boolean cleanupOnFailure;
  private final boolean overwriteFiles;
  private final boolean ignoreInvalidRows;
  private final Map<String, String> jobProperties;
  private final boolean combineText;
  private final boolean persistInHeap;
  private final boolean ingestOffheap;
  private final int bufferSize;
  private final float aggregationBufferRatio;

  @JsonCreator
  public HadoopTuningConfig(
      final @JsonProperty("workingPath") String workingPath,
      final @JsonProperty("version") String version,
      final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      final @JsonProperty("shardSpecs") Map<DateTime, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("maxRowsInMemory") Integer maxRowsInMemory,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") Boolean cleanupOnFailure,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
      final @JsonProperty("jobProperties") Map<String, String> jobProperties,
      final @JsonProperty("combineText") boolean combineText,
      final @JsonProperty("persistInHeap") boolean persistInHeap,
      final @JsonProperty("ingestOffheap") boolean ingestOffheap,
      final @JsonProperty("bufferSize") Integer bufferSize,
      final @JsonProperty("aggregationBufferRatio") Float aggregationBufferRatio
  )
  {
    this.workingPath = workingPath == null ? DEFAULT_WORKING_PATH : workingPath;
    this.version = version == null ? new DateTime().toString() : version;
    this.partitionsSpec = partitionsSpec == null ? DEFAULT_PARTITIONS_SPEC : partitionsSpec;
    this.shardSpecs = shardSpecs == null ? DEFAULT_SHARD_SPECS : shardSpecs;
    this.rowFlushBoundary = maxRowsInMemory == null ? DEFAULT_ROW_FLUSH_BOUNDARY : maxRowsInMemory;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = cleanupOnFailure == null ? true : cleanupOnFailure;
    this.overwriteFiles = overwriteFiles;
    this.ignoreInvalidRows = ignoreInvalidRows;
    this.jobProperties = (jobProperties == null
                          ? ImmutableMap.<String, String>of()
                          : ImmutableMap.copyOf(jobProperties));
    this.combineText = combineText;
    this.persistInHeap = persistInHeap;
    this.ingestOffheap = ingestOffheap;
    this.bufferSize = bufferSize == null ? DEFAULT_BUFFER_SIZE : bufferSize;
    this.aggregationBufferRatio = aggregationBufferRatio == null ? DEFAULT_AGG_BUFFER_RATIO : aggregationBufferRatio;
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
  public Map<DateTime, List<HadoopyShardSpec>> getShardSpecs()
  {
    return shardSpecs;
  }

  @JsonProperty
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
  public boolean isPersistInHeap()
  {
    return persistInHeap;
  }

  @JsonProperty
  public boolean isIngestOffheap(){
    return ingestOffheap;
  }

  @JsonProperty
  public int getBufferSize(){
    return bufferSize;
  }

  @JsonProperty
  public float getAggregationBufferRatio()
  {
    return aggregationBufferRatio;
  }

  public HadoopTuningConfig withWorkingPath(String path)
  {
    return new HadoopTuningConfig(
        path,
        version,
        partitionsSpec,
        shardSpecs,
        rowFlushBoundary,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        persistInHeap,
        ingestOffheap,
        bufferSize,
        aggregationBufferRatio
    );
  }

  public HadoopTuningConfig withVersion(String ver)
  {
    return new HadoopTuningConfig(
        workingPath,
        ver,
        partitionsSpec,
        shardSpecs,
        rowFlushBoundary,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        persistInHeap,
        ingestOffheap,
        bufferSize,
        aggregationBufferRatio
    );
  }

  public HadoopTuningConfig withShardSpecs(Map<DateTime, List<HadoopyShardSpec>> specs)
  {
    return new HadoopTuningConfig(
        workingPath,
        version,
        partitionsSpec,
        specs,
        rowFlushBoundary,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows,
        jobProperties,
        combineText,
        persistInHeap,
        ingestOffheap,
        bufferSize,
        aggregationBufferRatio
    );
  }
}
