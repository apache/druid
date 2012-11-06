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
import com.metamx.druid.index.v1.serde.Registererer;
import com.metamx.druid.indexer.data.DataSpec;
import com.metamx.druid.indexer.granularity.GranularitySpec;
import com.metamx.druid.indexer.granularity.UniformGranularitySpec;
import com.metamx.druid.indexer.path.PathSpec;
import com.metamx.druid.indexer.rollup.DataRollupSpec;
import com.metamx.druid.indexer.updater.UpdaterJobSpec;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.shard.ShardSpec;
import com.metamx.druid.utils.JodaUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
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
    if (argSpec.containsKey("registerers")) {
      List<Registererer> registererers = Lists.transform(
          MapUtils.getList(argSpec, "registerers"),
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
      for (Registererer registererer : registererers) {
        registererer.register();
      }
    }

    final HadoopDruidIndexerConfig retVal = jsonMapper.convertValue(argSpec, HadoopDruidIndexerConfig.class);
    retVal.verify();
    return retVal;
  }

  @SuppressWarnings("unchecked")
  public static HadoopDruidIndexerConfig fromFile(File file)
  {
    try {
      return fromMap(
          (Map<String, Object>) jsonMapper.readValue(
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
    return fromString(conf.get(CONFIG_PROPERTY));
  }

  private static final Logger log = new Logger(HadoopDruidIndexerConfig.class);

  private static final String CONFIG_PROPERTY = "druid.indexer.config";

  @Deprecated
  private volatile List<Interval> intervals;
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
  private volatile DateTime version;
  private volatile String partitionDimension;
  private volatile Long targetPartitionSize;
  private volatile boolean leaveIntermediate = false;
  private volatile boolean cleanupOnFailure = true;
  private volatile Map<DateTime, List<HadoopyShardSpec>> shardSpecs = ImmutableMap.of();
  private volatile boolean overwriteFiles = false;
  private volatile DataRollupSpec rollupSpec;
  private volatile UpdaterJobSpec updaterJobSpec;
  private volatile boolean ignoreInvalidRows = false;

  public List<Interval> getIntervals()
  {
    return JodaUtils.condenseIntervals(getGranularitySpec().bucketIntervals());
  }

  @Deprecated
  @JsonProperty
  public void setIntervals(List<Interval> intervals)
  {
    Preconditions.checkState(this.granularitySpec == null, "Use setGranularitySpec");

    // For backwards compatibility
    this.intervals = intervals;
    if (this.segmentGranularity != null) {
      this.granularitySpec = new UniformGranularitySpec(this.segmentGranularity, this.intervals);
    }
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
    this.timestampColumnName = timestampColumnName.toLowerCase();
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

  @JsonProperty
  public DataSpec getDataSpec()
  {
    return dataSpec;
  }

  public void setDataSpec(DataSpec dataSpec)
  {
    this.dataSpec = dataSpec;
  }

  @Deprecated
  @JsonProperty
  public void setSegmentGranularity(Granularity segmentGranularity)
  {
    Preconditions.checkState(this.granularitySpec == null, "Use setGranularitySpec");

    // For backwards compatibility
    this.segmentGranularity = segmentGranularity;
    if (this.intervals != null) {
      this.granularitySpec = new UniformGranularitySpec(this.segmentGranularity, this.intervals);
    }
  }

  @JsonProperty
  public GranularitySpec getGranularitySpec()
  {
    return granularitySpec;
  }

  public void setGranularitySpec(GranularitySpec granularitySpec)
  {
    Preconditions.checkState(this.intervals == null, "Use setGranularitySpec instead of setIntervals");
    Preconditions.checkState(
        this.segmentGranularity == null,
        "Use setGranularitySpec instead of setSegmentGranularity"
    );

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
  public DateTime getVersion()
  {
    return version;
  }

  public void setVersion(DateTime version)
  {
    this.version = version == null ? new DateTime() : version;
  }

  @JsonProperty
  public String getPartitionDimension()
  {
    return partitionDimension;
  }

  public void setPartitionDimension(String partitionDimension)
  {
    this.partitionDimension = (partitionDimension == null) ? partitionDimension : partitionDimension.toLowerCase();
  }

  public boolean partitionByDimension()
  {
    return partitionDimension != null;
  }

  @JsonProperty
  public Long getTargetPartitionSize()
  {
    return targetPartitionSize;
  }

  public void setTargetPartitionSize(Long targetPartitionSize)
  {
    this.targetPartitionSize = targetPartitionSize;
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

/********************************************
 Granularity/Bucket Helper Methods
 ********************************************/

  /**
   * Get the proper bucket for this "row"
   *
   * @param theMap a Map that represents a "row", keys are column names, values are, well, values
   *
   * @return the Bucket that this row belongs to
   */
  public Optional<Bucket> getBucket(Map<String, String> theMap)
  {
    final Optional<Interval> timeBucket = getGranularitySpec().bucketInterval(
        new DateTime(
            theMap.get(
                getTimestampColumnName()
            )
        )
    );
    if (!timeBucket.isPresent()) {
      return Optional.absent();
    }

    final List<HadoopyShardSpec> shards = shardSpecs.get(timeBucket.get().getStart());
    if (shards == null || shards.isEmpty()) {
      return Optional.absent();
    }

    for (final HadoopyShardSpec hadoopyShardSpec : shards) {
      final ShardSpec actualSpec = hadoopyShardSpec.getActualSpec();
      if (actualSpec.isInChunk(theMap)) {
        return Optional.of(
            new Bucket(
                hadoopyShardSpec.getShardNum(),
                timeBucket.get().getStart(),
                actualSpec.getPartitionNum()
            )
        );
      }
    }

    throw new ISE("row[%s] doesn't fit in any shard[%s]", theMap, shards);
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
    return new Path(String.format("%s/%s/%s", getJobOutputDir(), dataSource, getVersion().toString().replace(":", "")));
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

  public Path makeSegmentOutputPath(Bucket bucket)
  {
    final Interval bucketInterval = getGranularitySpec().bucketInterval(bucket.time).get();

    return new Path(
        String.format(
            "%s/%s_%s/%s/%s",
            getSegmentOutputDir(),
            bucketInterval.getStart().toString(),
            bucketInterval.getEnd().toString(),
            getVersion().toString(),
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

    if (partitionByDimension()) {
      Preconditions.checkNotNull(partitionDimension);
      Preconditions.checkNotNull(targetPartitionSize);
    }
  }
}
