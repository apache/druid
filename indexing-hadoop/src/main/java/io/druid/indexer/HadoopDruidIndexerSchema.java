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
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.impl.DataSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.granularity.GranularitySpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.rollup.DataRollupSpec;
import io.druid.indexer.updater.DbUpdaterJobSpec;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class HadoopDruidIndexerSchema
{
  private final String dataSource;
  private final TimestampSpec timestampSpec;
  private final DataSpec dataSpec;
  private final GranularitySpec granularitySpec;
  private final Map<String, Object> pathSpec; // This cannot just be a PathSpec object
  private final String workingPath;
  private final String segmentOutputPath;
  private final String version;
  private final PartitionsSpec partitionsSpec;
  private final boolean leaveIntermediate;
  private final boolean cleanupOnFailure;
  private final Map<DateTime, List<HadoopyShardSpec>> shardSpecs;
  private final boolean overwriteFiles;
  private final DataRollupSpec rollupSpec;
  private final DbUpdaterJobSpec updaterJobSpec;
  private final boolean ignoreInvalidRows;
  private final Map<String, String> jobProperties;

  @JsonCreator
  public HadoopDruidIndexerSchema(
      final @JsonProperty("dataSource") String dataSource,
      final @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      final @JsonProperty("dataSpec") DataSpec dataSpec,
      final @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      final @JsonProperty("pathSpec") Map<String, Object> pathSpec,
      final @JsonProperty("workingPath") String workingPath,
      final @JsonProperty("segmentOutputPath") String segmentOutputPath,
      final @JsonProperty("version") String version,
      final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") Boolean cleanupOnFailure,
      final @JsonProperty("shardSpecs") Map<DateTime, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("rollupSpec") DataRollupSpec rollupSpec,
      final @JsonProperty("updaterJobSpec") DbUpdaterJobSpec updaterJobSpec,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
      final @JsonProperty("jobProperties") Map<String, String> jobProperties,
      // These fields are deprecated and will be removed in the future
      final @JsonProperty("timestampColumn") String timestampColumn,
      final @JsonProperty("timestampFormat") String timestampFormat
  )
  {
    this.dataSource = dataSource;
    this.timestampSpec = (timestampSpec == null) ? new TimestampSpec(timestampColumn, timestampFormat) : timestampSpec;
    this.dataSpec = dataSpec;
    this.granularitySpec = granularitySpec;
    this.pathSpec = pathSpec;
    this.workingPath = workingPath;
    this.segmentOutputPath = segmentOutputPath;
    this.version = version == null ? new DateTime().toString() : version;
    this.partitionsSpec = partitionsSpec;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = (cleanupOnFailure == null ? true : cleanupOnFailure);
    this.shardSpecs = (shardSpecs == null ? ImmutableMap.<DateTime, List<HadoopyShardSpec>>of() : shardSpecs);
    this.overwriteFiles = overwriteFiles;
    this.rollupSpec = rollupSpec;
    this.updaterJobSpec = updaterJobSpec;
    this.ignoreInvalidRows = ignoreInvalidRows;
    this.jobProperties = (jobProperties == null
                          ? ImmutableMap.<String, String>of()
                          : ImmutableMap.copyOf(jobProperties));
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  @JsonProperty
  public DataSpec getDataSpec()
  {
    return dataSpec;
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  @JsonProperty
  public Map<String, Object> getPathSpec()
  {
    return pathSpec;
  }

  @JsonProperty
  public String getWorkingPath()
  {
    return workingPath;
  }

  @JsonProperty
  public String getSegmentOutputPath()
  {
    return segmentOutputPath;
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
  public boolean isLeaveIntermediate()
  {
    return leaveIntermediate;
  }

  @JsonProperty
  public boolean isCleanupOnFailure()
  {
    return cleanupOnFailure;
  }

  @JsonProperty
  public Map<DateTime, List<HadoopyShardSpec>> getShardSpecs()
  {
    return shardSpecs;
  }

  @JsonProperty
  public boolean isOverwriteFiles()
  {
    return overwriteFiles;
  }

  @JsonProperty
  public DataRollupSpec getRollupSpec()
  {
    return rollupSpec;
  }

  @JsonProperty
  public DbUpdaterJobSpec getUpdaterJobSpec()
  {
    return updaterJobSpec;
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
}
