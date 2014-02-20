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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.segment.indexing.DriverConfig;
import org.joda.time.DateTime;

import java.util.List;
import java.util.Map;

/**
 */
public class HadoopDriverConfig implements DriverConfig
{
  private final String workingPath;
  private final String version;
  private final PartitionsSpec partitionsSpec;
  private final Map<DateTime, List<HadoopyShardSpec>> shardSpecs;
  private final int rowFlushBoundary;
  private final boolean leaveIntermediate;
  private final Boolean cleanupOnFailure;
  private final boolean overwriteFiles;
  private final boolean ignoreInvalidRows;

  public HadoopDriverConfig(
      final @JsonProperty("workingPath") String workingPath,
      final @JsonProperty("version") String version,
      final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      final @JsonProperty("shardSpecs") Map<DateTime, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("rowFlushBoundary") int rowFlushBoundary,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") Boolean cleanupOnFailure,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows
  )
  {
    this.workingPath = workingPath;
    this.version = version == null ? new DateTime().toString() : version;
    this.partitionsSpec = partitionsSpec;
    this.shardSpecs = shardSpecs == null ? ImmutableMap.<DateTime, List<HadoopyShardSpec>>of() : shardSpecs;
    this.rowFlushBoundary = rowFlushBoundary;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = (cleanupOnFailure == null ? true : cleanupOnFailure);
    this.overwriteFiles = overwriteFiles;
    this.ignoreInvalidRows = ignoreInvalidRows;
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

  public HadoopDriverConfig withWorkingPath(String path)
  {
    return new HadoopDriverConfig(
        path,
        version,
        partitionsSpec,
        shardSpecs,
        rowFlushBoundary,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows
    );
  }

  public HadoopDriverConfig withVersion(String ver)
  {
    return new HadoopDriverConfig(
        workingPath,
        ver,
        partitionsSpec,
        shardSpecs,
        rowFlushBoundary,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows
    );
  }

  public HadoopDriverConfig withShardSpecs(Map<DateTime, List<HadoopyShardSpec>> specs)
  {
    return new HadoopDriverConfig(
        workingPath,
        version,
        partitionsSpec,
        specs,
        rowFlushBoundary,
        leaveIntermediate,
        cleanupOnFailure,
        overwriteFiles,
        ignoreInvalidRows
    );
  }
}
