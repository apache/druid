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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.DataSpec;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.data.input.impl.ToLowercaseDataSpec;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.indexer.granularity.GranularitySpec;
import io.druid.indexer.granularity.UniformGranularitySpec;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import io.druid.indexer.path.PathSpec;
import io.druid.indexer.rollup.DataRollupSpec;
import io.druid.indexer.updater.DbUpdaterJobSpec;
import io.druid.initialization.Initialization;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.server.DruidNode;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

/**
 */
public class HadoopDruidIndexerConfig
{
  private static final Logger log = new Logger(HadoopDruidIndexerConfig.class);
  private static final Injector injector;

  public static final String CONFIG_PROPERTY = "druid.indexer.config";
  public static final Charset javaNativeCharset = Charset.forName("Unicode");
  public static final Splitter tabSplitter = Splitter.on("\t");
  public static final Joiner tabJoiner = Joiner.on("\t");
  public static final ObjectMapper jsonMapper;

  static {
    injector = Initialization.makeInjectorWithModules(
        Initialization.makeStartupInjector(),
        ImmutableList.<Object>of(
            new Module()
            {
              @Override
              public void configure(Binder binder)
              {
                JsonConfigProvider.bindInstance(
                    binder, Key.get(DruidNode.class, Self.class), new DruidNode("hadoop-indexer", "localhost", -1)
                );
              }
            }
        )
    );
    jsonMapper = injector.getInstance(ObjectMapper.class);
  }

  public static enum IndexJobCounters
  {
    INVALID_ROW_COUNTER
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

  @JsonCreator
  public HadoopDruidIndexerConfig(
      final @JsonProperty("dataSource") String dataSource,
      final @JsonProperty("timestampSpec") TimestampSpec timestampSpec,
      final @JsonProperty("dataSpec") DataSpec dataSpec,
      final @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      final @JsonProperty("pathSpec") PathSpec pathSpec,
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
    this.dataSource = dataSource;
    this.timestampSpec = (timestampSpec == null) ? new TimestampSpec(timestampColumn, timestampFormat) : timestampSpec;
    this.dataSpec = dataSpec;
    this.pathSpec = pathSpec;
    this.workingPath = workingPath;
    this.segmentOutputPath = segmentOutputPath;
    this.version = version == null ? new DateTime().toString() : version;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = (cleanupOnFailure == null ? true : cleanupOnFailure);
    this.shardSpecs = (shardSpecs == null ? ImmutableMap.<DateTime, List<HadoopyShardSpec>>of() : shardSpecs);
    this.overwriteFiles = overwriteFiles;
    this.rollupSpec = rollupSpec;
    this.updaterJobSpec = updaterJobSpec;
    this.ignoreInvalidRows = ignoreInvalidRows;

    if (partitionsSpec != null) {
      Preconditions.checkArgument(
          partitionDimension == null && targetPartitionSize == null,
          "Cannot mix partitionsSpec with partitionDimension/targetPartitionSize"
      );
      this.partitionsSpec = partitionsSpec;
    } else {
      // Backwards compatibility
      this.partitionsSpec = new SingleDimensionPartitionsSpec(partitionDimension, targetPartitionSize, null, false);
    }

    if (granularitySpec != null) {
      Preconditions.checkArgument(
          segmentGranularity == null && intervals == null,
          "Cannot mix granularitySpec with segmentGranularity/intervals"
      );
      this.granularitySpec = granularitySpec;
    } else {
      // Backwards compatibility
      if (segmentGranularity != null && intervals != null) {
        this.granularitySpec = new UniformGranularitySpec(segmentGranularity, intervals);
      }
    }
  }

  /**
   * Default constructor does nothing. The caller is expected to use the various setX methods.
   */
  public HadoopDruidIndexerConfig()
  {
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  public void setDataSource(String dataSource)
  {
    this.dataSource = dataSource.toLowerCase();
  }

  @JsonProperty
  public TimestampSpec getTimestampSpec()
  {
    return timestampSpec;
  }

  public void setTimestampSpec(TimestampSpec timestampSpec)
  {
    this.timestampSpec = timestampSpec;
  }

  @JsonProperty
  public DataSpec getDataSpec()
  {
    return dataSpec;
  }

  public void setDataSpec(DataSpec dataSpec)
  {
    this.dataSpec = new ToLowercaseDataSpec(dataSpec);
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  public void setGranularitySpec(GranularitySpec granularitySpec)
  {
    this.granularitySpec = granularitySpec;
  }

  @JsonProperty
  public PathSpec getPathSpec()
  {
    return pathSpec;
  }

  public void setPathSpec(PathSpec pathSpec)
  {
    this.pathSpec = pathSpec;
  }

  @JsonProperty
  public String getWorkingPath()
  {
    return workingPath;
  }

  public void setWorkingPath(String workingPath)
  {
    this.workingPath = workingPath;
  }

  @JsonProperty
  public String getSegmentOutputPath()
  {
    return segmentOutputPath;
  }

  public void setSegmentOutputPath(String segmentOutputPath)
  {
    this.segmentOutputPath = segmentOutputPath;
  }

  @JsonProperty
  public String getVersion()
  {
    return version;
  }

  public void setVersion(String version)
  {
    this.version = version;
  }

  @JsonProperty
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  public void setPartitionsSpec(PartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
  }

  @JsonProperty
  public boolean isLeaveIntermediate()
  {
    return leaveIntermediate;
  }

  public void setLeaveIntermediate(boolean leaveIntermediate)
  {
    this.leaveIntermediate = leaveIntermediate;
  }

  @JsonProperty
  public boolean isCleanupOnFailure()
  {
    return cleanupOnFailure;
  }

  public void setCleanupOnFailure(boolean cleanupOnFailure)
  {
    this.cleanupOnFailure = cleanupOnFailure;
  }

  @JsonProperty
  public Map<DateTime, List<HadoopyShardSpec>> getShardSpecs()
  {
    return shardSpecs;
  }

  public void setShardSpecs(Map<DateTime, List<HadoopyShardSpec>> shardSpecs)
  {
    this.shardSpecs = Collections.unmodifiableMap(shardSpecs);
  }

  @JsonProperty
  public boolean isOverwriteFiles()
  {
    return overwriteFiles;
  }

  public void setOverwriteFiles(boolean overwriteFiles)
  {
    this.overwriteFiles = overwriteFiles;
  }

  @JsonProperty
  public DataRollupSpec getRollupSpec()
  {
    return rollupSpec;
  }

  public void setRollupSpec(DataRollupSpec rollupSpec)
  {
    this.rollupSpec = rollupSpec;
  }

  @JsonProperty
  public DbUpdaterJobSpec getUpdaterJobSpec()
  {
    return updaterJobSpec;
  }

  public void setUpdaterJobSpec(DbUpdaterJobSpec updaterJobSpec)
  {
    this.updaterJobSpec = updaterJobSpec;
  }

  @JsonProperty
  public boolean isIgnoreInvalidRows()
  {
    return ignoreInvalidRows;
  }

  public void setIgnoreInvalidRows(boolean ignoreInvalidRows)
  {
    this.ignoreInvalidRows = ignoreInvalidRows;
  }

  public Optional<List<Interval>> getIntervals()
  {
    Optional<SortedSet<Interval>> setOptional = getGranularitySpec().bucketIntervals();
    if (setOptional.isPresent()) {
      return Optional.of((List<Interval>) JodaUtils.condenseIntervals(setOptional.get()));
    } else {
      return Optional.absent();
    }
  }

  public boolean isDeterminingPartitions()
  {
    return partitionsSpec.isDeterminingPartitions();
  }

  public Long getTargetPartitionSize()
  {
    return partitionsSpec.getTargetPartitionSize();
  }

  public long getMaxPartitionSize()
  {
    return partitionsSpec.getMaxPartitionSize();
  }

  public boolean isUpdaterJobSpecSet()
  {
    return (updaterJobSpec != null);
  }

  public StringInputRowParser getParser()
  {
    final List<String> dimensionExclusions;

    if (getDataSpec().hasCustomDimensions()) {
      dimensionExclusions = null;
    } else {
      dimensionExclusions = Lists.newArrayList();
      dimensionExclusions.add(timestampSpec.getTimestampColumn());
      dimensionExclusions.addAll(
          Lists.transform(
              getRollupSpec().getAggs(), new Function<AggregatorFactory, String>()
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

    return new StringInputRowParser(getTimestampSpec(), getDataSpec(), dimensionExclusions);
  }

  public HadoopyShardSpec getShardSpec(Bucket bucket)
  {
    return shardSpecs.get(bucket.time).get(bucket.partitionNum);
  }

  public Job addInputPaths(Job job) throws IOException
  {
    return getPathSpec().addInputPaths(this, job);
  }

  /********************************************
   Granularity/Bucket Helper Methods
   ********************************************/

  /**
   * Get the proper bucket for some input row.
   *
   * @param inputRow an InputRow
   *
   * @return the Bucket that this row belongs to
   */
  public Optional<Bucket> getBucket(InputRow inputRow)
  {
    final Optional<Interval> timeBucket = getGranularitySpec().bucketInterval(new DateTime(inputRow.getTimestampFromEpoch()));
    if (!timeBucket.isPresent()) {
      return Optional.absent();
    }

    final List<HadoopyShardSpec> shards = shardSpecs.get(timeBucket.get().getStart());
    if (shards == null || shards.isEmpty()) {
      return Optional.absent();
    }

    for (final HadoopyShardSpec hadoopyShardSpec : shards) {
      final ShardSpec actualSpec = hadoopyShardSpec.getActualSpec();
      if (actualSpec.isInChunk(inputRow)) {
        return Optional.of(
            new Bucket(
                hadoopyShardSpec.getShardNum(),
                timeBucket.get().getStart(),
                actualSpec.getPartitionNum()
            )
        );
      }
    }

    throw new ISE("row[%s] doesn't fit in any shard[%s]", inputRow, shards);
  }

  public Optional<Set<Interval>> getSegmentGranularIntervals()
  {
    return Optional.fromNullable((Set<Interval>) granularitySpec.bucketIntervals().orNull());
  }

  public Optional<Iterable<Bucket>> getAllBuckets()
  {
    Optional<Set<Interval>> intervals = getSegmentGranularIntervals();
    if (intervals.isPresent()) {
      return Optional.of(
          (Iterable<Bucket>) FunctionalIterable
              .create(intervals.get())
              .transformCat(
                  new Function<Interval, Iterable<Bucket>>()
                  {
                    @Override
                    public Iterable<Bucket> apply(Interval input)
                    {
                      final DateTime bucketTime = input.getStart();
                      final List<HadoopyShardSpec> specs = shardSpecs.get(bucketTime);
                      if (specs == null) {
                        return ImmutableList.of();
                      }

                      return FunctionalIterable
                          .create(specs)
                          .transform(
                              new Function<HadoopyShardSpec, Bucket>()
                              {
                                int i = 0;

                                @Override
                                public Bucket apply(HadoopyShardSpec input)
                                {
                                  return new Bucket(input.getShardNum(), bucketTime, i++);
                                }
                              }
                          );
                    }
                  }
              )
      );
    } else {
      return Optional.absent();
    }
  }

    /******************************************
     Path helper logic
     ******************************************/

    /**
     * Make the intermediate path for this job run.
     *
     * @return the intermediate path for this job run.
     */

  public Path makeIntermediatePath()
  {
    return new Path(String.format("%s/%s/%s", getWorkingPath(), getDataSource(), getVersion().replace(":", "")));
  }

  public Path makeSegmentPartitionInfoPath(Interval bucketInterval)
  {
    return new Path(
        String.format(
            "%s/%s_%s/partitions.json",
            makeIntermediatePath(),
            ISODateTimeFormat.basicDateTime().print(bucketInterval.getStart()),
            ISODateTimeFormat.basicDateTime().print(bucketInterval.getEnd())
        )
    );
  }

  public Path makeIntervalInfoPath()
  {
    return new Path(
        String.format(
            "%s/intervals.json",
            makeIntermediatePath()
        )
    );
  }

  public Path makeDescriptorInfoDir()
  {
    return new Path(makeIntermediatePath(), "segmentDescriptorInfo");
  }

  public Path makeGroupedDataDir()
  {
    return new Path(makeIntermediatePath(), "groupedData");
  }

  public Path makeDescriptorInfoPath(DataSegment segment)
  {
    return new Path(makeDescriptorInfoDir(), String.format("%s.json", segment.getIdentifier().replace(":", "")));
  }

  public Path makeSegmentOutputPath(FileSystem fileSystem, Bucket bucket)
  {
    final Interval bucketInterval = getGranularitySpec().bucketInterval(bucket.time).get();
    if (fileSystem instanceof DistributedFileSystem) {
      return new Path(
          String.format(
              "%s/%s/%s_%s/%s/%s",
              getSegmentOutputPath(),
              getDataSource(),
              bucketInterval.getStart().toString(ISODateTimeFormat.basicDateTime()),
              bucketInterval.getEnd().toString(ISODateTimeFormat.basicDateTime()),
              getVersion().replace(":", "_"),
              bucket.partitionNum
          )
      );
    }
    return new Path(
        String.format(
            "%s/%s/%s_%s/%s/%s",
            getSegmentOutputPath(),
            getDataSource(),
            bucketInterval.getStart().toString(),
            bucketInterval.getEnd().toString(),
            getVersion(),
            bucket.partitionNum
        )
    );
  }

  public void intoConfiguration(Job job)
  {
    Configuration conf = job.getConfiguration();

    try {
      conf.set(HadoopDruidIndexerConfig.CONFIG_PROPERTY, HadoopDruidIndexerConfig.jsonMapper.writeValueAsString(this));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public void verify()
  {
    try {
      log.info("Running with config:%n%s", jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(this));
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    Preconditions.checkNotNull(dataSource, "dataSource");
    Preconditions.checkNotNull(dataSpec, "dataSpec");
    Preconditions.checkNotNull(timestampSpec, "timestampSpec");
    Preconditions.checkNotNull(granularitySpec, "granularitySpec");
    Preconditions.checkNotNull(pathSpec, "pathSpec");
    Preconditions.checkNotNull(workingPath, "workingPath");
    Preconditions.checkNotNull(segmentOutputPath, "segmentOutputPath");
    Preconditions.checkNotNull(version, "version");
    Preconditions.checkNotNull(rollupSpec, "rollupSpec");
  }
}
