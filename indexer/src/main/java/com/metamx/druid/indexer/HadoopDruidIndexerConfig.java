/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
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
import com.metamx.common.Granularity;
import com.metamx.common.ISE;
import com.metamx.common.MapUtils;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import com.metamx.druid.RegisteringNode;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.serde.Registererer;
import com.metamx.druid.indexer.data.DataSpec;
import com.metamx.druid.indexer.data.StringInputRowParser;
import com.metamx.druid.indexer.data.TimestampSpec;
import com.metamx.druid.indexer.data.ToLowercaseDataSpec;
import com.metamx.druid.indexer.granularity.GranularitySpec;
import com.metamx.druid.indexer.granularity.UniformGranularitySpec;
import com.metamx.druid.indexer.partitions.PartitionsSpec;
import com.metamx.druid.indexer.path.PathSpec;
import com.metamx.druid.indexer.rollup.DataRollupSpec;
import com.metamx.druid.indexer.updater.UpdaterJobSpec;
import com.metamx.druid.input.InputRow;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.shard.ShardSpec;
import com.metamx.druid.utils.JodaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;

import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class HadoopDruidIndexerConfig
{
  public static final Charset javaNativeCharset = Charset.forName("Unicode");

  public static final Splitter tagSplitter = Splitter.on("\u0001");
  public static final Joiner tagJoiner = Joiner.on("\u0001");
  public static final Splitter tabSplitter = Splitter.on("\t");
  public static final Joiner tabJoiner = Joiner.on("\t");
  public static final ObjectMapper jsonMapper;

  static {
    jsonMapper = new DefaultObjectMapper();
    jsonMapper.configure(JsonGenerator.Feature.ESCAPE_NON_ASCII, true);
  }

  public static enum IndexJobCounters
  {
    INVALID_ROW_COUNTER
  }

  public static HadoopDruidIndexerConfig fromMap(Map<String, Object> argSpec)
  {
    List<Registererer> registererers = Lists.transform(
        MapUtils.getList(argSpec, "registererers", ImmutableList.of()),
        new Function<Object, Registererer>()
        {
          @Override
          public Registererer apply(@Nullable Object input)
          {
            try {
              return (Registererer) Class.forName((String) input).newInstance();
            }
            catch (Exception e) {
              throw Throwables.propagate(e);
            }
          }
        }
    );

    if (!registererers.isEmpty()) {
      RegisteringNode.registerHandlers(registererers, Arrays.asList(jsonMapper));
    }

    return jsonMapper.convertValue(argSpec, HadoopDruidIndexerConfig.class);
  }

  @SuppressWarnings("unchecked")
  public static HadoopDruidIndexerConfig fromFile(File file)
  {
    try {
      return fromMap((Map<String, Object>) jsonMapper.readValue(file, new TypeReference<Map<String, Object>>(){}));
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
          (Map<String, Object>) jsonMapper.readValue(
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
    final HadoopDruidIndexerConfig retVal = fromString(conf.get(CONFIG_PROPERTY));
    retVal.verify();
    return retVal;
  }

  private static final Logger log = new Logger(HadoopDruidIndexerConfig.class);

  private static final String CONFIG_PROPERTY = "druid.indexer.config";

  private volatile String dataSource;
  private volatile String timestampColumnName;
  private volatile String timestampFormat;
  private volatile DataSpec dataSpec;
  @Deprecated
  private volatile Granularity segmentGranularity;
  private volatile GranularitySpec granularitySpec;
  private volatile PathSpec pathSpec;
  private volatile String jobOutputDir;
  private volatile String segmentOutputDir;
  private volatile String version = new DateTime().toString();
  private volatile PartitionsSpec partitionsSpec;
  private volatile boolean leaveIntermediate = false;
  private volatile boolean cleanupOnFailure = true;
  private volatile Map<DateTime, List<HadoopyShardSpec>> shardSpecs = ImmutableMap.of();
  private volatile boolean overwriteFiles = false;
  private volatile DataRollupSpec rollupSpec;
  private volatile UpdaterJobSpec updaterJobSpec;
  private volatile boolean ignoreInvalidRows = false;
  private volatile List<String> registererers = Lists.newArrayList();

  @JsonCreator
  public HadoopDruidIndexerConfig(
      final @JsonProperty("intervals") List<Interval> intervals,
      final @JsonProperty("dataSource") String dataSource,
      final @JsonProperty("timestampColumn") String timestampColumnName,
      final @JsonProperty("timestampFormat") String timestampFormat,
      final @JsonProperty("dataSpec") DataSpec dataSpec,
      final @JsonProperty("segmentGranularity") Granularity segmentGranularity,
      final @JsonProperty("granularitySpec") GranularitySpec granularitySpec,
      final @JsonProperty("pathSpec") PathSpec pathSpec,
      final @JsonProperty("workingPath") String jobOutputDir,
      final @JsonProperty("segmentOutputPath") String segmentOutputDir,
      final @JsonProperty("version") String version,
      final @JsonProperty("partitionDimension") String partitionDimension,
      final @JsonProperty("targetPartitionSize") Long targetPartitionSize,
      final @JsonProperty("partitionsSpec") PartitionsSpec partitionsSpec,
      final @JsonProperty("leaveIntermediate") boolean leaveIntermediate,
      final @JsonProperty("cleanupOnFailure") boolean cleanupOnFailure,
      final @JsonProperty("shardSpecs") Map<DateTime, List<HadoopyShardSpec>> shardSpecs,
      final @JsonProperty("overwriteFiles") boolean overwriteFiles,
      final @JsonProperty("rollupSpec") DataRollupSpec rollupSpec,
      final @JsonProperty("updaterJobSpec") UpdaterJobSpec updaterJobSpec,
      final @JsonProperty("ignoreInvalidRows") boolean ignoreInvalidRows,
      final @JsonProperty("registererers") List<String> registererers
  )
  {
    this.dataSource = dataSource;
    this.timestampColumnName = timestampColumnName;
    this.timestampFormat = timestampFormat;
    this.dataSpec = dataSpec;
    this.granularitySpec = granularitySpec;
    this.pathSpec = pathSpec;
    this.jobOutputDir = jobOutputDir;
    this.segmentOutputDir = segmentOutputDir;
    this.version = version == null ? new DateTime().toString() : version;
    this.partitionsSpec = partitionsSpec;
    this.leaveIntermediate = leaveIntermediate;
    this.cleanupOnFailure = cleanupOnFailure;
    this.shardSpecs = shardSpecs;
    this.overwriteFiles = overwriteFiles;
    this.rollupSpec = rollupSpec;
    this.updaterJobSpec = updaterJobSpec;
    this.ignoreInvalidRows = ignoreInvalidRows;
    this.registererers = registererers;

    if(partitionsSpec != null) {
      Preconditions.checkArgument(
          partitionDimension == null && targetPartitionSize == null,
          "Cannot mix partitionsSpec with partitionDimension/targetPartitionSize"
      );

      this.partitionsSpec = partitionsSpec;
    } else {
      // Backwards compatibility
      this.partitionsSpec = new PartitionsSpec(partitionDimension, targetPartitionSize, false);
    }

    if(granularitySpec != null) {
      Preconditions.checkArgument(
          segmentGranularity == null && intervals == null,
          "Cannot mix granularitySpec with segmentGranularity/intervals"
      );
    } else {
      // Backwards compatibility
      this.segmentGranularity = segmentGranularity;
      if(segmentGranularity != null && intervals != null) {
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

  public List<Interval> getIntervals()
  {
    return JodaUtils.condenseIntervals(getGranularitySpec().bucketIntervals());
  }

  @Deprecated
  public void setIntervals(List<Interval> intervals)
  {
    Preconditions.checkState(this.granularitySpec == null, "Cannot mix setIntervals with granularitySpec");
    Preconditions.checkState(this.segmentGranularity != null, "Cannot use setIntervals without segmentGranularity");

    // For backwards compatibility
    this.granularitySpec = new UniformGranularitySpec(this.segmentGranularity, intervals);
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

  @JsonProperty("timestampColumn")
  public String getTimestampColumnName()
  {
    return timestampColumnName;
  }

  public void setTimestampColumnName(String timestampColumnName)
  {
    this.timestampColumnName = timestampColumnName;
  }

  @JsonProperty()
  public String getTimestampFormat()
  {
    return timestampFormat;
  }

  public void setTimestampFormat(String timestampFormat)
  {
    this.timestampFormat = timestampFormat;
  }

  public TimestampSpec getTimestampSpec()
  {
    return new TimestampSpec(timestampColumnName, timestampFormat);
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

  public StringInputRowParser getParser()
  {
    final List<String> dimensionExclusions;

    if(getDataSpec().hasCustomDimensions()) {
      dimensionExclusions = null;
    } else {
      dimensionExclusions = Lists.newArrayList();
      dimensionExclusions.add(getTimestampColumnName());
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
  public PartitionsSpec getPartitionsSpec()
  {
    return partitionsSpec;
  }

  public void setPartitionsSpec(PartitionsSpec partitionsSpec)
  {
    this.partitionsSpec = partitionsSpec;
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

  @JsonProperty("workingPath")
  public String getJobOutputDir()
  {
    return jobOutputDir;
  }

  public void setJobOutputDir(String jobOutputDir)
  {
    this.jobOutputDir = jobOutputDir;
  }

  @JsonProperty("segmentOutputPath")
  public String getSegmentOutputDir()
  {
    return segmentOutputDir;
  }

  public void setSegmentOutputDir(String segmentOutputDir)
  {
    this.segmentOutputDir = segmentOutputDir;
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

  public String getPartitionDimension()
  {
    return partitionsSpec.getPartitionDimension();
  }

  public boolean partitionByDimension()
  {
    return partitionsSpec.isDeterminingPartitions();
  }

  public Long getTargetPartitionSize()
  {
    return partitionsSpec.getTargetPartitionSize();
  }

  public boolean isUpdaterJobSpecSet()
  {
    return (updaterJobSpec != null);
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
  public UpdaterJobSpec getUpdaterJobSpec()
  {
    return updaterJobSpec;
  }

  public void setUpdaterJobSpec(UpdaterJobSpec updaterJobSpec)
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

  @JsonProperty
  public List<String> getRegistererers()
  {
    return registererers;
  }

  public void setRegistererers(List<String> registererers)
  {
    this.registererers = registererers;
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

  public Set<Interval> getSegmentGranularIntervals()
  {
    return granularitySpec.bucketIntervals();
  }

  public Iterable<Bucket> getAllBuckets()
  {
    return FunctionalIterable
        .create(getSegmentGranularIntervals())
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
        );
  }

  public HadoopyShardSpec getShardSpec(Bucket bucket)
  {
    return shardSpecs.get(bucket.time).get(bucket.partitionNum);
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
    return new Path(String.format("%s/%s/%s", getJobOutputDir(), dataSource, getVersion().replace(":", "")));
  }

  public Path makeSegmentPartitionInfoPath(Bucket bucket)
  {
    final Interval bucketInterval = getGranularitySpec().bucketInterval(bucket.time).get();

    return new Path(
        String.format(
            "%s/%s_%s/partitions.json",
            makeIntermediatePath(),
            ISODateTimeFormat.basicDateTime().print(bucketInterval.getStart()),
            ISODateTimeFormat.basicDateTime().print(bucketInterval.getEnd())
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

  public Path makeSegmentOutputPath(Bucket bucket)
  {
    final Interval bucketInterval = getGranularitySpec().bucketInterval(bucket.time).get();

    return new Path(
        String.format(
            "%s/%s/%s_%s/%s/%s",
            getSegmentOutputDir(),
            dataSource,
            bucketInterval.getStart().toString(),
            bucketInterval.getEnd().toString(),
            getVersion(),
            bucket.partitionNum
        )
    );
  }

  public Job addInputPaths(Job job) throws IOException
  {
    return pathSpec.addInputPaths(this, job);
  }

  public void intoConfiguration(Job job)
  {
    Configuration conf = job.getConfiguration();

    try {
      conf.set(CONFIG_PROPERTY, jsonMapper.writeValueAsString(this));
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
    Preconditions.checkNotNull(timestampColumnName, "timestampColumn");
    Preconditions.checkNotNull(timestampFormat, "timestampFormat");
    Preconditions.checkNotNull(granularitySpec, "granularitySpec");
    Preconditions.checkNotNull(pathSpec, "pathSpec");
    Preconditions.checkNotNull(jobOutputDir, "workingPath");
    Preconditions.checkNotNull(segmentOutputDir, "segmentOutputPath");
    Preconditions.checkNotNull(version, "version");
    Preconditions.checkNotNull(rollupSpec, "rollupSpec");

    final int nIntervals = getIntervals().size();
    Preconditions.checkArgument(nIntervals > 0, "intervals.size()[%s] <= 0", nIntervals);
  }
}
