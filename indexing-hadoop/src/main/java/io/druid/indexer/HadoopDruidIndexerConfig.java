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

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.metamx.common.ISE;
import com.metamx.common.guava.FunctionalIterable;
import com.metamx.common.logger.Logger;
import io.druid.common.utils.JodaUtils;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Self;
import io.druid.indexer.partitions.PartitionsSpec;
import io.druid.indexer.path.PathSpec;
import io.druid.initialization.Initialization;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
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

  public static HadoopDruidIndexerConfig fromSchema(HadoopIngestionSpec schema)
  {
    return new HadoopDruidIndexerConfig(injector.getInstance(ColumnConfig.class), schema);
  }

  public static HadoopDruidIndexerConfig fromMap(Map<String, Object> argSpec)
  {
    //backwards compatibility
    if (argSpec.containsKey("schema")) {
      return HadoopDruidIndexerConfig.jsonMapper.convertValue(argSpec, HadoopDruidIndexerConfig.class);
    } else {
      return new HadoopDruidIndexerConfig(
          injector.getInstance(ColumnConfig.class),
          HadoopDruidIndexerConfig.jsonMapper.convertValue(
              argSpec,
              HadoopIngestionSpec.class
          )
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

  private volatile HadoopIngestionSpec schema;
  private volatile PathSpec pathSpec;
  private volatile ColumnConfig columnConfig;

  @JsonCreator
  public HadoopDruidIndexerConfig(
      @JacksonInject final ColumnConfig columnConfig,
      final @JsonProperty("schema") HadoopIngestionSpec schema
  )
  {
    this.columnConfig = columnConfig;
    this.schema = schema;
    this.pathSpec = jsonMapper.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  @JsonProperty
  public HadoopIngestionSpec getSchema()
  {
    return schema;
  }

  public ColumnConfig getColumnConfig()
  {
    return columnConfig;
  }

  public String getDataSource()
  {
    return schema.getDataSchema().getDataSource();
  }

  public GranularitySpec getGranularitySpec()
  {
    return schema.getDataSchema().getGranularitySpec();
  }

  public void setGranularitySpec(GranularitySpec granularitySpec)
  {
    this.schema = schema.withDataSchema(schema.getDataSchema().withGranularitySpec(granularitySpec));
    this.pathSpec = jsonMapper.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public PartitionsSpec getPartitionsSpec()
  {
    return schema.getTuningConfig().getPartitionsSpec();
  }

  public boolean isOverwriteFiles()
  {
    return schema.getTuningConfig().isOverwriteFiles();
  }

  public boolean isIgnoreInvalidRows()
  {
    return schema.getTuningConfig().isIgnoreInvalidRows();
  }

  public void setVersion(String version)
  {
    this.schema = schema.withTuningConfig(schema.getTuningConfig().withVersion(version));
    this.pathSpec = jsonMapper.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public void setShardSpecs(Map<DateTime, List<HadoopyShardSpec>> shardSpecs)
  {
    this.schema = schema.withTuningConfig(schema.getTuningConfig().withShardSpecs(shardSpecs));
    this.pathSpec = jsonMapper.convertValue(schema.getIOConfig().getPathSpec(), PathSpec.class);
  }

  public Optional<List<Interval>> getIntervals()
  {
    Optional<SortedSet<Interval>> setOptional = schema.getDataSchema().getGranularitySpec().bucketIntervals();
    if (setOptional.isPresent()) {
      return Optional.of((List<Interval>) JodaUtils.condenseIntervals(setOptional.get()));
    } else {
      return Optional.absent();
    }
  }

  public boolean isDeterminingPartitions()
  {
    return schema.getTuningConfig().getPartitionsSpec().isDeterminingPartitions();
  }

  public Long getTargetPartitionSize()
  {
    return schema.getTuningConfig().getPartitionsSpec().getTargetPartitionSize();
  }

  public long getMaxPartitionSize()
  {
    return schema.getTuningConfig().getPartitionsSpec().getMaxPartitionSize();
  }

  public boolean isUpdaterJobSpecSet()
  {
    return (schema.getIOConfig().getMetadataUpdateSpec() != null);
  }

  public boolean isCombineText()
  {
    return schema.getTuningConfig().isCombineText();
  }

  public StringInputRowParser getParser()
  {
    return (StringInputRowParser) schema.getDataSchema().getParser();
  }

  public HadoopyShardSpec getShardSpec(Bucket bucket)
  {
    return schema.getTuningConfig().getShardSpecs().get(bucket.time).get(bucket.partitionNum);
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
    final Optional<Interval> timeBucket = schema.getDataSchema().getGranularitySpec().bucketInterval(
        new DateTime(
            inputRow.getTimestampFromEpoch()
        )
    );
    if (!timeBucket.isPresent()) {
      return Optional.absent();
    }

    final List<HadoopyShardSpec> shards = schema.getTuningConfig().getShardSpecs().get(timeBucket.get().getStart());
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
    return Optional.fromNullable(
        (Set<Interval>) schema.getDataSchema()
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
                      final List<HadoopyShardSpec> specs = schema.getTuningConfig().getShardSpecs().get(bucketTime);
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
            schema.getTuningConfig().getWorkingPath(),
            schema.getDataSchema().getDataSource(),
            schema.getTuningConfig().getVersion().replace(":", "")
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
    final Interval bucketInterval = schema.getDataSchema().getGranularitySpec().bucketInterval(bucket.time).get();
    if (fileSystem instanceof DistributedFileSystem) {
      return new Path(
          String.format(
              "%s/%s/%s_%s/%s/%s",
              schema.getIOConfig().getSegmentOutputPath(),
              schema.getDataSchema().getDataSource(),
              bucketInterval.getStart().toString(ISODateTimeFormat.basicDateTime()),
              bucketInterval.getEnd().toString(ISODateTimeFormat.basicDateTime()),
              schema.getTuningConfig().getVersion().replace(":", "_"),
              bucket.partitionNum
          )
      );
    }
    return new Path(
        String.format(
            "%s/%s/%s_%s/%s/%s",
            schema.getIOConfig().getSegmentOutputPath(),
            schema.getDataSchema().getDataSource(),
            bucketInterval.getStart().toString(),
            bucketInterval.getEnd().toString(),
            schema.getTuningConfig().getVersion(),
            bucket.partitionNum
        )
    );
  }

  public void addJobProperties(Job job)
  {
    Configuration conf = job.getConfiguration();

    for (final Map.Entry<String, String> entry : schema.getTuningConfig().getJobProperties().entrySet()) {
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

    Preconditions.checkNotNull(schema.getDataSchema().getDataSource(), "dataSource");
    Preconditions.checkNotNull(schema.getDataSchema().getParser().getParseSpec(), "parseSpec");
    Preconditions.checkNotNull(schema.getDataSchema().getParser().getParseSpec().getTimestampSpec(), "timestampSpec");
    Preconditions.checkNotNull(schema.getDataSchema().getGranularitySpec(), "granularitySpec");
    Preconditions.checkNotNull(pathSpec, "pathSpec");
    Preconditions.checkNotNull(schema.getTuningConfig().getWorkingPath(), "workingPath");
    Preconditions.checkNotNull(schema.getIOConfig().getSegmentOutputPath(), "segmentOutputPath");
    Preconditions.checkNotNull(schema.getTuningConfig().getVersion(), "version");
  }
}
