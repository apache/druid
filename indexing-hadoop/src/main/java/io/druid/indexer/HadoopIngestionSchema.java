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
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.metamx.common.Granularity;
import io.druid.data.input.impl.DataSpec;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.ParseSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import io.druid.indexer.rollup.DataRollupSpec;
import io.druid.indexer.updater.DbUpdaterJobSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IngestionSchema;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Map;

/**
 */
public class HadoopIngestionSchema implements IngestionSchema
{
  public static HadoopIngestionSchema convertLegacy(
      String dataSource,
      TimestampSpec timestampSpec,
      DataSpec dataSpec,
      GranularitySpec granularitySpec,
      Map<String, Object> pathSpec,
      String workingPath,
      String segmentOutputPath,
      String version,
      PartitionsSpec partitionsSpec,
      boolean leaveIntermediate,
      Boolean cleanupOnFailure,
      Map<DateTime, List<HadoopyShardSpec>> shardSpecs,
      boolean overwriteFiles,
      DataRollupSpec rollupSpec,
      DbUpdaterJobSpec updaterJobSpec,
      boolean ignoreInvalidRows,
      // These fields are deprecated and will be removed in the future
      String timestampColumn,
      String timestampFormat,
      List<Interval> intervals,
      Granularity segmentGranularity,
      String partitionDimension,
      Long targetPartitionSize
  )
  {
    return new HadoopIngestionSchema(
        null, null, null,
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
        timestampColumn,
        timestampFormat,
        intervals,
        segmentGranularity,
        partitionDimension,
        targetPartitionSize
    );
  }

  private final DataSchema dataSchema;
  private final HadoopIOConfig ioConfig;
  private final HadoopDriverConfig driverConfig;

  @JsonCreator
  public HadoopIngestionSchema(
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("ioConfig") HadoopIOConfig ioConfig,
      @JsonProperty("driverConfig") HadoopDriverConfig driverConfig,
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
      // These fields are deprecated and will be removed in the future
      final @JsonProperty("timestampColumn") String timestampColumn,
      final @JsonProperty("timestampFormat") String timestampFormat,
      final @JsonProperty("intervals") List<Interval> intervals,
      final @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      final @JsonProperty("partitionDimension") String partitionDimension,
      final @JsonProperty("targetPartitionSize") Long targetPartitionSize
  )
  {
    if (dataSchema != null) {
      this.dataSchema = dataSchema;
      this.ioConfig = ioConfig;
      this.driverConfig = driverConfig;
    } else { // Backwards compatibility
      TimestampSpec theTimestampSpec = (timestampSpec == null)
                                       ? new TimestampSpec(timestampColumn, timestampFormat)
                                       : timestampSpec;
      List<String> dimensionExclusions = Lists.newArrayList();
      dimensionExclusions.add(theTimestampSpec.getTimestampColumn());
      for (AggregatorFactory aggregatorFactory : rollupSpec.getAggs()) {
        dimensionExclusions.add(aggregatorFactory.getName());
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

      if (dataSpec.hasCustomDimensions()) {
        dimensionExclusions = null;
      } else {
        dimensionExclusions = Lists.newArrayList();
        dimensionExclusions.add(theTimestampSpec.getTimestampColumn());
        dimensionExclusions.addAll(
            Lists.transform(
                rollupSpec.getAggs(), new Function<AggregatorFactory, String>()
            {
              @Override
              public String apply(AggregatorFactory aggregatorFactory)
              {
                return aggregatorFactory.getName();
              }
            }
            )
        );
      }

      GranularitySpec theGranularitySpec = null;
      if (granularitySpec != null) {
        Preconditions.checkArgument(
            segmentGranularity == null && intervals == null,
            "Cannot mix granularitySpec with segmentGranularity/intervals"
        );
        theGranularitySpec = granularitySpec;
      } else {
        // Backwards compatibility
        if (segmentGranularity != null && intervals != null) {
          theGranularitySpec = new UniformGranularitySpec(segmentGranularity, null, intervals, segmentGranularity);
        }
      }

      this.dataSchema = new DataSchema(
          dataSource,
          new StringInputRowParser(
              new ParseSpec(
                  theTimestampSpec,
                  new DimensionsSpec(
                      dataSpec.getDimensions(),
                      dimensionExclusions,
                      dataSpec.getSpatialDimensions()
                  )
              )
              {
              },
              null, null, null, null
          ),
          rollupSpec.getAggs().toArray(new AggregatorFactory[rollupSpec.getAggs().size()]),
          theGranularitySpec
      );

      this.ioConfig = new HadoopIOConfig(
          pathSpec,
          updaterJobSpec,
          segmentOutputPath
      );

      this.driverConfig = new HadoopDriverConfig(
          workingPath,
          version,
          thePartitionSpec,
          shardSpecs,
          leaveIntermediate,
          cleanupOnFailure,
          overwriteFiles,
          ignoreInvalidRows
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

  @JsonProperty("driverConfig")
  @Override
  public HadoopDriverConfig getDriverConfig()
  {
    return driverConfig;
  }

  public HadoopIngestionSchema withDriverConfig(HadoopDriverConfig config)
  {
    return new HadoopIngestionSchema(
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
        null,
        null,
        null,
        null,
        null
    );
  }

  public HadoopIngestionSchema withDataSchema(DataSchema schema)
  {
    return new HadoopIngestionSchema(
        schema,
        ioConfig,
        driverConfig,
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
        null,
        null,
        null,
        null,
        null
    );
  }
}
