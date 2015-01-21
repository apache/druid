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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Maps;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.granularity.QueryGranularity;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.path.PathSpec;
import io.druid.initialization.Initialization;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.server.DruidNode;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.ShardSpecLookup;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
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
        GuiceInjectors.makeStartupInjector(),
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

  public static HadoopDruidIndexerConfig fromSpec(HadoopIngestionSpec spec)
  {
    return new HadoopDruidIndexerConfig(spec, null);
  }

  public static HadoopDruidIndexerConfig fromMap(Map<String, Object> argSpec)
  {
    //backwards compatibility
    if (argSpec.containsKey("schema") || argSpec.containsKey("spec")) {
      return HadoopDruidIndexerConfig.jsonMapper.convertValue(argSpec, HadoopDruidIndexerConfig.class);
    } else {
      return new HadoopDruidIndexerConfig(
          HadoopDruidIndexerConfig.jsonMapper.convertValue(
              argSpec,
              HadoopIngestionSpec.class
          ),
          null
      );
    }
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

  private volatile HadoopIngestionSpec spec;
  private volatile PathSpec pathSpec;
  private volatile Map<DateTime, ShardSpecLookup> shardSpecLookups = Maps.newHashMap();
  private volatile Map<ShardSpec, HadoopyShardSpec> hadoopShardSpecLookup = Maps.newHashMap();
  private final QueryGranularity rollupGran;

  @JsonCreator
  public HadoopDruidIndexerConfig(
      final @JsonProperty("spec") HadoopIngestionSpec spec,
      final @JsonProperty("schema") HadoopIngestionSpec schema // backwards compat
  )
  {
    if (spec != null) {
      this.spec = spec;
    } else {
      this.spec = schema;
    }
    this.pathSpec = jsonMapper.convertValue(this.spec.getIOConfig().getPathSpec(), PathSpec.class);
    for (Map.Entry<DateTime, List<HadoopyShardSpec>> entry : spec.getTuningConfig().getShardSpecs().entrySet()) {
      if (entry.getValue() == null || entry.getValue().isEmpty()) {
        continue;
      }
      final ShardSpec actualSpec = entry.getValue().get(0).getActualSpec();
      shardSpecLookups.put(
          entry.getKey(), actualSpec.getLookup(
              Lists.transform(
                  entry.getValue(), new Function<HadoopyShardSpec, ShardSpec>()
                  {
                    @Override
                    public ShardSpec apply(HadoopyShardSpec input)
                    {
                      return input.getActualSpec();
                    }
                  }
              )
          )
      );
      for (HadoopyShardSpec hadoopyShardSpec : entry.getValue()) {
        hadoopShardSpecLookup.put(hadoopyShardSpec.getActualSpec(), hadoopyShardSpec);
      }
    }
    this.rollupGran = this.spec.getDataSchema().getGranularitySpec().getQueryGranularity();
  }

  @JsonProperty
  public HadoopIngestionSpec getSpec()
  {
    return spec;
  }

  public String getDataSource()
  {
    return spec.getDataSchema().getDataSource();
  }

  public GranularitySpec getGranularitySpec()
  {
    return spec.getDataSchema().getGranularitySpec();
  }

  public void setGranularitySpec(GranularitySpec granularitySpec)
  {
    this.spec = spec.withDataSchema(spec.getDataSchema().withGranularitySpec(granularitySpec));
    this.pathSpec = jsonMapper.convertValue(spec.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public PartitionsSpec getPartitionsSpec()
  {
    return spec.getTuningConfig().getPartitionsSpec();
  }

  public boolean isOverwriteFiles()
  {
    return spec.getTuningConfig().isOverwriteFiles();
  }

  public boolean isIgnoreInvalidRows()
  {
    return spec.getTuningConfig().isIgnoreInvalidRows();
  }

  public void setVersion(String version)
  {
    this.spec = spec.withTuningConfig(spec.getTuningConfig().withVersion(version));
    this.pathSpec = jsonMapper.convertValue(spec.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public void setShardSpecs(Map<DateTime, List<HadoopyShardSpec>> shardSpecs)
  {
    this.spec = spec.withTuningConfig(spec.getTuningConfig().withShardSpecs(shardSpecs));
    this.pathSpec = jsonMapper.convertValue(spec.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public Optional<List<Interval>> getIntervals()
  {
    Optional<SortedSet<Interval>> setOptional = spec.getDataSchema().getGranularitySpec().bucketIntervals();
    if (setOptional.isPresent()) {
      return Optional.of((List<Interval>) JodaUtils.condenseIntervals(setOptional.get()));
    } else {
      return Optional.absent();
    }
  }

  public boolean isDeterminingPartitions()
  {
    return spec.getTuningConfig().getPartitionsSpec().isDeterminingPartitions();
  }

  public Long getTargetPartitionSize()
  {
    return spec.getTuningConfig().getPartitionsSpec().getTargetPartitionSize();
  }

  public long getMaxPartitionSize()
  {
    return spec.getTuningConfig().getPartitionsSpec().getMaxPartitionSize();
  }

  public boolean isUpdaterJobSpecSet()
  {
    return (spec.getIOConfig().getMetadataUpdateSpec() != null);
  }

  public boolean isCombineText()
  {
    return spec.getTuningConfig().isCombineText();
  }

  public StringInputRowParser getParser()
  {
    return (StringInputRowParser) spec.getDataSchema().getParser();
  }

  public HadoopyShardSpec getShardSpec(Bucket bucket)
  {
    return spec.getTuningConfig().getShardSpecs().get(bucket.time).get(bucket.partitionNum);
  }

  public Job addInputPaths(Job job) throws IOException
  {
    return pathSpec.addInputPaths(this, job);
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
    final Optional<Interval> timeBucket = spec.getDataSchema().getGranularitySpec().bucketInterval(
        new DateTime(
            inputRow.getTimestampFromEpoch()
        )
    );
    if (!timeBucket.isPresent()) {
      return Optional.absent();
    }

    final ShardSpec actualSpec = shardSpecLookups.get(timeBucket.get().getStart())
                                                 .getShardSpec(
                                                     rollupGran.truncate(inputRow.getTimestampFromEpoch()),
                                                     inputRow
                                                 );
    final HadoopyShardSpec hadoopyShardSpec = hadoopShardSpecLookup.get(actualSpec);

    return Optional.of(
        new Bucket(
            hadoopyShardSpec.getShardNum(),
            timeBucket.get().getStart(),
            actualSpec.getPartitionNum()
        )
    );

  }

  public Optional<Set<Interval>> getSegmentGranularIntervals()
  {
    return Optional.fromNullable(
        (Set<Interval>) spec.getDataSchema()
                            .getGranularitySpec()
                            .bucketIntervals()
                            .orNull()
    );
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
                      final List<HadoopyShardSpec> specs = spec.getTuningConfig().getShardSpecs().get(bucketTime);
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
    return new Path(
        String.format(
            "%s/%s/%s",
            spec.getTuningConfig().getWorkingPath(),
            spec.getDataSchema().getDataSource(),
            spec.getTuningConfig().getVersion().replace(":", "")
        )
    );
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
    final Interval bucketInterval = spec.getDataSchema().getGranularitySpec().bucketInterval(bucket.time).get();
    if (fileSystem instanceof DistributedFileSystem) {
      return new Path(
          String.format(
              "%s/%s/%s_%s/%s/%s",
              spec.getIOConfig().getSegmentOutputPath(),
              spec.getDataSchema().getDataSource(),
              bucketInterval.getStart().toString(ISODateTimeFormat.basicDateTime()),
              bucketInterval.getEnd().toString(ISODateTimeFormat.basicDateTime()),
              spec.getTuningConfig().getVersion().replace(":", "_"),
              bucket.partitionNum
          )
      );
    }
    return new Path(
        String.format(
            "%s/%s/%s_%s/%s/%s",
            spec.getIOConfig().getSegmentOutputPath(),
            spec.getDataSchema().getDataSource(),
            bucketInterval.getStart().toString(),
            bucketInterval.getEnd().toString(),
            spec.getTuningConfig().getVersion(),
            bucket.partitionNum
        )
    );
  }

  public void addJobProperties(Job job)
  {
    Configuration conf = job.getConfiguration();

    for (final Map.Entry<String, String> entry : spec.getTuningConfig().getJobProperties().entrySet()) {
      conf.set(entry.getKey(), entry.getValue());
    }
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

    Preconditions.checkNotNull(spec.getDataSchema().getDataSource(), "dataSource");
    Preconditions.checkNotNull(spec.getDataSchema().getParser().getParseSpec(), "parseSpec");
    Preconditions.checkNotNull(spec.getDataSchema().getParser().getParseSpec().getTimestampSpec(), "timestampSpec");
    Preconditions.checkNotNull(spec.getDataSchema().getGranularitySpec(), "granularitySpec");
    Preconditions.checkNotNull(pathSpec, "pathSpec");
    Preconditions.checkNotNull(spec.getTuningConfig().getWorkingPath(), "workingPath");
    Preconditions.checkNotNull(spec.getIOConfig().getSegmentOutputPath(), "segmentOutputPath");
    Preconditions.checkNotNull(spec.getTuningConfig().getVersion(), "version");
  }
}
