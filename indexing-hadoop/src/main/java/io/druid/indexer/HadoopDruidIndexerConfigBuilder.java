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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.impl.DataSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.granularity.GranularitySpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.path.PathSpec;
import io.druid.indexer.rollup.DataRollupSpec;
import io.druid.indexer.updater.DbUpdaterJobSpec;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.DateTime;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 */
public class HadoopDruidIndexerConfigBuilder
{
  public static HadoopDruidIndexerConfig fromSchema(HadoopDruidIndexerSchema schema)
  {
    return HadoopDruidIndexerConfig.jsonMapper.convertValue(schema, HadoopDruidIndexerConfig.class);
  }

  public static HadoopDruidIndexerSchema toSchema(HadoopDruidIndexerConfig config){
    return HadoopDruidIndexerConfig.jsonMapper.convertValue(config, HadoopDruidIndexerSchema.class);
  }

  public static HadoopDruidIndexerConfig fromMap(Map<String, Object> argSpec)
  {
    return HadoopDruidIndexerConfig.jsonMapper.convertValue(argSpec, HadoopDruidIndexerConfig.class);
  }

  @SuppressWarnings("unchecked")
  public static HadoopDruidIndexerConfig fromFile(File file)
  {
    try {
      return fromMap(
          (Map<String, Object>) HadoopDruidIndexerConfig.jsonMapper.readValue(
              file, new TypeReference<Map<String, Object>>()
          {
          }
          )
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  @SuppressWarnings("unchecked")
  public static HadoopDruidIndexerConfig fromString(String str)
  {
    try {
      return fromMap(
          (Map<String, Object>) HadoopDruidIndexerConfig.jsonMapper.readValue(
              str, new TypeReference<Map<String, Object>>()
          {
          }
          )
      );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static HadoopDruidIndexerConfig fromConfiguration(Configuration conf)
  {
    final HadoopDruidIndexerConfig retVal = fromString(conf.get(HadoopDruidIndexerConfig.CONFIG_PROPERTY));
    retVal.verify();
    return retVal;
  }

  private volatile String dataSource;
  private volatile TimestampSpec timestampSpec;
  private volatile DataSpec dataSpec;
  private volatile GranularitySpec granularitySpec;
  private volatile PathSpec pathSpec;
  private volatile String workingPath;
  private volatile String segmentOutputPath;
  private volatile String version;
  private volatile PartitionsSpec partitionsSpec;
  private volatile boolean leaveIntermediate;
  private volatile boolean cleanupOnFailure;
  private volatile Map<DateTime, List<HadoopyShardSpec>> shardSpecs;
  private volatile boolean overwriteFiles;
  private volatile DataRollupSpec rollupSpec;
  private volatile DbUpdaterJobSpec updaterJobSpec;
  private volatile boolean ignoreInvalidRows;

  public HadoopDruidIndexerConfigBuilder()
  {
    this.dataSource = null;
    this.timestampSpec = null;
    this.dataSpec = null;
    this.granularitySpec = null;
    this.pathSpec = null;
    this.workingPath = null;
    this.segmentOutputPath = null;
    this.version = new DateTime().toString();
    this.partitionsSpec = null;
    this.leaveIntermediate = false;
    this.cleanupOnFailure = true;
    this.shardSpecs = ImmutableMap.of();
    this.overwriteFiles = false;
    this.rollupSpec = null;
    this.updaterJobSpec = null;
    this.ignoreInvalidRows = false;
  }

  public HadoopDruidIndexerConfigBuilder withDataSource(String dataSource)
  {
    this.dataSource = dataSource;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withTimestampSpec(TimestampSpec timestampSpec)
  {
    this.timestampSpec = timestampSpec;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withDataSpec(DataSpec dataSpec)
  {
    this.dataSpec = dataSpec;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withGranularitySpec(GranularitySpec granularitySpec)
  {
    this.granularitySpec = granularitySpec;
    return this;

  }

  public HadoopDruidIndexerConfigBuilder withPathSpec(PathSpec pathSpec)
  {
    this.pathSpec = pathSpec;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withWorkingPath(String workingPath)
  {
    this.workingPath = workingPath;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withSegmentOutputPath(String segmentOutputPath)
  {
    this.segmentOutputPath = segmentOutputPath;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withVersion(String version)
  {
    this.version = version;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withPartitionsSpec(PartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withLeaveIntermediate(boolean leaveIntermediate)
  {
    this.leaveIntermediate = leaveIntermediate;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withCleanupOnFailure(boolean cleanupOnFailure)
  {
    this.cleanupOnFailure = cleanupOnFailure;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withShardSpecs(Map<DateTime, List<HadoopyShardSpec>> shardSpecs)
  {
    this.shardSpecs = shardSpecs;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withOverwriteFiles(boolean overwriteFiles)
  {
    this.overwriteFiles = overwriteFiles;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withRollupSpec(DataRollupSpec rollupSpec)
  {
    this.rollupSpec = rollupSpec;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withUpdaterJobSpec(DbUpdaterJobSpec updaterJobSpec)
  {
    this.updaterJobSpec = updaterJobSpec;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withIgnoreInvalidRows(boolean ignoreInvalidRows)
  {
    this.ignoreInvalidRows = ignoreInvalidRows;
    return this;
  }

  public HadoopDruidIndexerConfigBuilder withSchema(HadoopDruidIndexerSchema schema)
  {
    this.dataSource = schema.getDataSource();
    this.timestampSpec = schema.getTimestampSpec();
    this.dataSpec = schema.getDataSpec();
    this.granularitySpec = schema.getGranularitySpec();
    this.pathSpec = HadoopDruidIndexerConfig.jsonMapper.convertValue(schema.getPathSpec(), PathSpec.class);
    this.workingPath = schema.getWorkingPath();
    this.segmentOutputPath = schema.getSegmentOutputPath();
    this.version = schema.getVersion();
    this.partitionsSpec = schema.getPartitionsSpec();
    this.leaveIntermediate = schema.isLeaveIntermediate();
    this.cleanupOnFailure = schema.isCleanupOnFailure();
    this.shardSpecs = schema.getShardSpecs();
    this.overwriteFiles = schema.isOverwriteFiles();
    this.rollupSpec = schema.getRollupSpec();
    this.updaterJobSpec = schema.getUpdaterJobSpec();
    this.ignoreInvalidRows = schema.isIgnoreInvalidRows();

    return this;
  }

  public HadoopDruidIndexerConfig build()
  {
    return new HadoopDruidIndexerConfig(
        dataSource,
        timestampSpec,
        dataSpec,
        granularitySpec,
        pathSpec,
        workingPath,
        segmentOutputPath,
        version,
        partitionsSpec,
        leaveIntermediate,
        cleanupOnFailure,
        shardSpecs,
        overwriteFiles,
        rollupSpec,
        updaterJobSpec,
        ignoreInvalidRows,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}
