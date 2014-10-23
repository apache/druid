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
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import io.druid.data.input.impl.DataSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import io.druid.indexer.rollup.DataRollupSpec;
import io.druid.indexer.updater.DbUpdaterJobSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IngestionSpec;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class HadoopIngestionSpec extends IngestionSpec<HadoopIOConfig, HadoopTuningConfig>
{
  private final DataSchema dataSchema;
  private final HadoopIOConfig ioConfig;
  private final HadoopTuningConfig tuningConfig;

  @JsonCreator
  public HadoopIngestionSpec(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") HadoopIOConfig ioConfig,
      @JsonProperty("tuningConfig") HadoopTuningConfig tuningConfig,
      // All deprecated
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
      final @JsonProperty("combineText") boolean combineText,
      // These fields are deprecated and will be removed in the future
      final @JsonProperty("timestampColumn") String timestampColumn,
      final @JsonProperty("timestampFormat") String timestampFormat,
      final @JsonProperty("intervals") List<Interval> intervals,
      final @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      final @JsonProperty("partitionDimension") String partitionDimension,
      final @JsonProperty("targetPartitionSize") Long targetPartitionSize
  )
  {
    super(dataSchema, ioConfig, tuningConfig);

    if (dataSchema != null) {
      this.dataSchema = dataSchema;
      this.ioConfig = ioConfig;
      this.tuningConfig = tuningConfig == null ? HadoopTuningConfig.makeDefaultTuningConfig() : tuningConfig;
    } else { // Backwards compatibility
      TimestampSpec theTimestampSpec = (timestampSpec == null)
                                       ? new TimestampSpec(timestampColumn, timestampFormat)
                                       : timestampSpec;
      List<String> dimensionExclusions = Lists.newArrayList();

      dimensionExclusions.add(theTimestampSpec.getTimestampColumn());
      if (rollupSpec != null) {
        for (AggregatorFactory aggregatorFactory : rollupSpec.getAggs()) {
          dimensionExclusions.add(aggregatorFactory.getName());
        }
      }

      PartitionsSpec thePartitionSpec;
      if (partitionsSpec != null) {
        Preconditions.checkArgument(
            partitionDimension == null && targetPartitionSize == null,
            "Cannot mix partitionsSpec with partitionDimension/targetPartitionSize"
        );
        thePartitionSpec = partitionsSpec;
      } else {
        // Backwards compatibility
        thePartitionSpec = new SingleDimensionPartitionsSpec(partitionDimension, targetPartitionSize, null, false);
      }

      GranularitySpec theGranularitySpec = null;
      if (granularitySpec != null) {
        Preconditions.checkArgument(
            segmentGranularity == null && intervals == null,
            "Cannot mix granularitySpec with segmentGranularity/intervals"
        );
        theGranularitySpec = granularitySpec;
        if (rollupSpec != null) {
          theGranularitySpec = theGranularitySpec.withQueryGranularity(rollupSpec.rollupGranularity);
        }
      } else {
        // Backwards compatibility
        if (segmentGranularity != null && intervals != null) {
          theGranularitySpec = new UniformGranularitySpec(
              segmentGranularity,
              rollupSpec == null ? null : rollupSpec.rollupGranularity,
              intervals,
              segmentGranularity
          );
        }
      }

      this.dataSchema = new DataSchema(
          dataSource,
          new StringInputRowParser(
              dataSpec == null ? null : dataSpec.toParseSpec(theTimestampSpec, dimensionExclusions),
              null, null, null, null
          ),
          rollupSpec == null
          ? new AggregatorFactory[]{}
          : rollupSpec.getAggs().toArray(new AggregatorFactory[rollupSpec.getAggs().size()]),
          theGranularitySpec
      );

      this.ioConfig = new HadoopIOConfig(
          pathSpec,
          updaterJobSpec,
          segmentOutputPath
      );

      this.tuningConfig = new HadoopTuningConfig(
          workingPath,
          version,
          thePartitionSpec,
          shardSpecs,
          rollupSpec == null ? 50000 : rollupSpec.rowFlushBoundary,
          leaveIntermediate,
          cleanupOnFailure,
          overwriteFiles,
          ignoreInvalidRows,
          jobProperties,
          combineText
      );
    }
  }

  @JsonProperty("dataSchema")
  @Override
  public DataSchema getDataSchema()
  {
    return dataSchema;
  }

  @JsonProperty("ioConfig")
  @Override
  public HadoopIOConfig getIOConfig()
  {
    return ioConfig;
  }

  @JsonProperty("tuningConfig")
  @Override
  public HadoopTuningConfig getTuningConfig()
  {
    return tuningConfig;
  }

  public HadoopIngestionSpec withDataSchema(DataSchema schema)
  {
    return new HadoopIngestionSpec(
        schema,
        ioConfig,
        tuningConfig,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null,
        false,
        null,
        null,
        false,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  public HadoopIngestionSpec withIOConfig(HadoopIOConfig config)
  {
    return new HadoopIngestionSpec(
        dataSchema,
        config,
        tuningConfig,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null,
        false,
        null,
        null,
        false,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }

  public HadoopIngestionSpec withTuningConfig(HadoopTuningConfig config)
  {
    return new HadoopIngestionSpec(
        dataSchema,
        ioConfig,
        config,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        null,
        false,
        null,
        null,
        false,
        null,
        null,
        false,
        null,
        false,
        null,
        null,
        null,
        null,
        null,
        null
    );
  }
}