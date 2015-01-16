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

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.metamx.common.ISE;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.granularity.QueryGranularity;
import io.druid.indexer.HadoopDruidIndexerConfig;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.index.YeOldePlumberSchool;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IOConfig;
import io.druid.segment.indexing.IngestionSpec;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.TuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.CopyOnWriteArrayList;

public class IndexTask extends AbstractFixedIntervalTask
{
  private static final Logger log = new Logger(IndexTask.class);

  private static String makeId(String id, IndexIngestionSpec ingestionSchema, String dataSource)
  {
    if (id == null) {
      return String.format("index_%s_%s", makeDataSource(ingestionSchema, dataSource), new DateTime().toString());
    }

    return id;
  }

  private static HashFunction hashFunction = Hashing.murmur3_128();

  /**
   * Should we index this inputRow? Decision is based on our interval and shardSpec.
   *
   * @param inputRow the row to check
   *
   * @return true or false
   */
  private static boolean shouldIndex(
      final ShardSpec shardSpec,
      final Interval interval,
      final InputRow inputRow,
      final QueryGranularity rollupGran
  )
  {
    return interval.contains(inputRow.getTimestampFromEpoch())
           && shardSpec.isInChunk(rollupGran.truncate(inputRow.getTimestampFromEpoch()), inputRow);
  }

  private static String makeDataSource(IndexIngestionSpec ingestionSchema, String dataSource)
  {
    if (ingestionSchema != null) {
      return ingestionSchema.getDataSchema().getDataSource();
    } else { // Backwards compatible
      return dataSource;
    }
  }

  private static Interval makeInterval(IndexIngestionSpec ingestionSchema, GranularitySpec granularitySpec)
  {
    GranularitySpec spec;
    if (ingestionSchema != null) {
      spec = ingestionSchema.getDataSchema().getGranularitySpec();
    } else {
      spec = granularitySpec;
    }

    return new Interval(
        spec.bucketIntervals().get().first().getStart(),
        spec.bucketIntervals().get().last().getEnd()
    );
  }

  @JsonIgnore
  private final IndexIngestionSpec ingestionSpec;

  private final ObjectMapper jsonMapper;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("spec") IndexIngestionSpec ingestionSpec,
      // Backwards Compatible
      @JsonProperty("schema") IndexIngestionSpec schema,
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("granularitySpec") final GranularitySpec granularitySpec,
      @JsonProperty("aggregators") final AggregatorFactory[] aggregators,
      @JsonProperty("indexGranularity") final QueryGranularity indexGranularity,
      @JsonProperty("targetPartitionSize") final int targetPartitionSize,
      @JsonProperty("firehose") final FirehoseFactory firehoseFactory,
      @JsonProperty("rowFlushBoundary") final int rowFlushBoundary,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(
        // _not_ the version, just something uniqueish
        makeId(id, ingestionSpec, dataSource),
        makeDataSource(ingestionSpec, dataSource),
        makeInterval(ingestionSpec, granularitySpec)
    );

    if (ingestionSpec != null || schema != null) {
      if (ingestionSpec != null) {
        this.ingestionSpec = ingestionSpec;
      } else {
        this.ingestionSpec = schema;
      }
    } else { // Backwards Compatible
      this.ingestionSpec = new IndexIngestionSpec(
          new DataSchema(
              dataSource,
              firehoseFactory.getParser(),
              aggregators,
              granularitySpec.withQueryGranularity(indexGranularity == null ? QueryGranularity.NONE : indexGranularity)
          ),
          new IndexIOConfig(firehoseFactory),
          new IndexTuningConfig(targetPartitionSize, rowFlushBoundary, null)
      );
    }
    this.jsonMapper = jsonMapper;
  }

  @Override
  public String getType()
  {
    return "index";
  }

  @JsonProperty("spec")
  public IndexIngestionSpec getIngestionSpec()
  {
    return ingestionSpec;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final GranularitySpec granularitySpec = ingestionSpec.getDataSchema().getGranularitySpec();
    final int targetPartitionSize = ingestionSpec.getTuningConfig().getTargetPartitionSize();

    final TaskLock myLock = Iterables.getOnlyElement(getTaskLocks(toolbox));
    final Set<DataSegment> segments = Sets.newHashSet();

    final Set<Interval> validIntervals = Sets.intersection(granularitySpec.bucketIntervals().get(), getDataIntervals());
    if (validIntervals.isEmpty()) {
      throw new ISE("No valid data intervals found. Check your configs!");
    }

    for (final Interval bucket : validIntervals) {
      final List<ShardSpec> shardSpecs;
      if (targetPartitionSize > 0) {
        shardSpecs = determinePartitions(bucket, targetPartitionSize, granularitySpec.getQueryGranularity());
      } else {
        int numShards = ingestionSpec.getTuningConfig().getNumShards();
        if (numShards > 0) {
          shardSpecs = Lists.newArrayList();
          for (int i = 0; i < numShards; i++) {
            shardSpecs.add(new HashBasedNumberedShardSpec(i, numShards, jsonMapper));
          }
        } else {
          shardSpecs = ImmutableList.<ShardSpec>of(new NoneShardSpec());
        }
      }
      for (final ShardSpec shardSpec : shardSpecs) {
        final DataSegment segment = generateSegment(
            toolbox,
            ingestionSpec.getDataSchema(),
            shardSpec,
            bucket,
            myLock.getVersion()
        );
        segments.add(segment);
      }
    }
    toolbox.pushSegments(segments);
    return TaskStatus.success(getId());
  }

  private SortedSet<Interval> getDataIntervals() throws IOException
  {
    final FirehoseFactory firehoseFactory = ingestionSpec.getIOConfig().getFirehoseFactory();
    final GranularitySpec granularitySpec = ingestionSpec.getDataSchema().getGranularitySpec();

    SortedSet<Interval> retVal = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());
    try (Firehose firehose = firehoseFactory.connect(ingestionSpec.getDataSchema().getParser())) {
      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        Interval interval = granularitySpec.getSegmentGranularity()
                                           .bucket(new DateTime(inputRow.getTimestampFromEpoch()));
        retVal.add(interval);
      }
    }

    return retVal;
  }

  private List<ShardSpec> determinePartitions(
      final Interval interval,
      final int targetPartitionSize,
      final QueryGranularity queryGranularity
  ) throws IOException
  {
    log.info("Determining partitions for interval[%s] with targetPartitionSize[%d]", interval, targetPartitionSize);

    final FirehoseFactory firehoseFactory = ingestionSpec.getIOConfig().getFirehoseFactory();

    // The implementation of this determine partitions stuff is less than optimal.  Should be done better.
    // Use HLL to estimate number of rows
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    // Load data
    try (Firehose firehose = firehoseFactory.connect(ingestionSpec.getDataSchema().getParser())) {
      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        if (interval.contains(inputRow.getTimestampFromEpoch())) {
          final List<Object> groupKey = Rows.toGroupKey(
              queryGranularity.truncate(inputRow.getTimestampFromEpoch()),
              inputRow
          );
          collector.add(
              hashFunction.hashBytes(HadoopDruidIndexerConfig.jsonMapper.writeValueAsBytes(groupKey))
                          .asBytes()
          );
        }
      }
    }

    final double numRows = collector.estimateCardinality();
    log.info("Estimated approximately [%,f] rows of data.", numRows);

    int numberOfShards = (int) Math.ceil(numRows / targetPartitionSize);
    if ((double) numberOfShards > numRows) {
      numberOfShards = (int) numRows;
    }
    log.info("Will require [%,d] shard(s).", numberOfShards);

    // ShardSpecs we will return
    final List<ShardSpec> shardSpecs = Lists.newArrayList();

    if (numberOfShards == 1) {
      shardSpecs.add(new NoneShardSpec());
    } else {
      for (int i = 0; i < numberOfShards; ++i) {
        shardSpecs.add(
            new HashBasedNumberedShardSpec(
                i,
                numberOfShards,
                HadoopDruidIndexerConfig.jsonMapper
            )
        );
      }
    }

    return shardSpecs;
  }

  private DataSegment generateSegment(
      final TaskToolbox toolbox,
      final DataSchema schema,
      final ShardSpec shardSpec,
      final Interval interval,
      final String version
  ) throws IOException
  {
    // Set up temporary directory.
    final File tmpDir = new File(
        toolbox.getTaskWorkDir(),
        String.format(
            "%s_%s_%s_%s_%s",
            this.getDataSource(),
            interval.getStart(),
            interval.getEnd(),
            version,
            shardSpec.getPartitionNum()
        )
    );

    final FirehoseFactory firehoseFactory = ingestionSpec.getIOConfig().getFirehoseFactory();
    final int rowFlushBoundary = ingestionSpec.getTuningConfig().getRowFlushBoundary();

    // We need to track published segments.
    final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<DataSegment>();
    final DataSegmentPusher wrappedDataSegmentPusher = new DataSegmentPusher()
    {
      @Override
      public String getPathForHadoop(String dataSource)
      {
        return toolbox.getSegmentPusher().getPathForHadoop(dataSource);
      }

      @Override
      public DataSegment push(File file, DataSegment segment) throws IOException
      {
        final DataSegment pushedSegment = toolbox.getSegmentPusher().push(file, segment);
        pushedSegments.add(pushedSegment);
        return pushedSegment;
      }
    };

    // Create firehose + plumber
    final FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    final Firehose firehose = firehoseFactory.connect(ingestionSpec.getDataSchema().getParser());
    final Plumber plumber = new YeOldePlumberSchool(
        interval,
        version,
        wrappedDataSegmentPusher,
        tmpDir
    ).findPlumber(schema, new RealtimeTuningConfig(null, null, null, null, null, null, null, shardSpec), metrics);

    // rowFlushBoundary for this job
    final int myRowFlushBoundary = rowFlushBoundary > 0
                                   ? rowFlushBoundary
                                   : toolbox.getConfig().getDefaultRowFlushBoundary();
    final QueryGranularity rollupGran = ingestionSpec.getDataSchema().getGranularitySpec().getQueryGranularity();
    try {
      plumber.startJob();

      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();

        if (shouldIndex(shardSpec, interval, inputRow, rollupGran)) {
          int numRows = plumber.add(inputRow);
          if (numRows == -1) {
            throw new ISE(
                String.format(
                    "Was expecting non-null sink for timestamp[%s]",
                    new DateTime(inputRow.getTimestampFromEpoch())
                )
            );
          }
          metrics.incrementProcessed();

          if (numRows >= myRowFlushBoundary) {
            plumber.persist(firehose.commit());
          }
        } else {
          metrics.incrementThrownAway();
        }
      }
    }
    finally {
      firehose.close();
    }

    plumber.persist(firehose.commit());

    try {
      plumber.finishJob();
    }
    finally {
      log.info(
          "Task[%s] interval[%s] partition[%d] took in %,d rows (%,d processed, %,d unparseable, %,d thrown away)"
          + " and output %,d rows",
          getId(),
          interval,
          shardSpec.getPartitionNum(),
          metrics.processed() + metrics.unparseable() + metrics.thrownAway(),
          metrics.processed(),
          metrics.unparseable(),
          metrics.thrownAway(),
          metrics.rowOutput()
      );
    }

    // We expect a single segment to have been created.
    return Iterables.getOnlyElement(pushedSegments);
  }

  public static class IndexIngestionSpec extends IngestionSpec<IndexIOConfig, IndexTuningConfig>
  {
    private final DataSchema dataSchema;
    private final IndexIOConfig ioConfig;
    private final IndexTuningConfig tuningConfig;

    @JsonCreator
    public IndexIngestionSpec(
        @JsonProperty("dataSchema") DataSchema dataSchema,
        @JsonProperty("ioConfig") IndexIOConfig ioConfig,
        @JsonProperty("tuningConfig") IndexTuningConfig tuningConfig
    )
    {
      super(dataSchema, ioConfig, tuningConfig);

      this.dataSchema = dataSchema;
      this.ioConfig = ioConfig;
      this.tuningConfig = tuningConfig == null ? new IndexTuningConfig(0, 0, null) : tuningConfig;
    }

    @Override
    @JsonProperty("dataSchema")
    public DataSchema getDataSchema()
    {
      return dataSchema;
    }

    @Override
    @JsonProperty("ioConfig")
    public IndexIOConfig getIOConfig()
    {
      return ioConfig;
    }

    @Override
    @JsonProperty("tuningConfig")
    public IndexTuningConfig getTuningConfig()
    {
      return tuningConfig;
    }
  }

  @JsonTypeName("index")
  public static class IndexIOConfig implements IOConfig
  {
    private final FirehoseFactory firehoseFactory;

    @JsonCreator
    public IndexIOConfig(
        @JsonProperty("firehose") FirehoseFactory firehoseFactory
    )
    {
      this.firehoseFactory = firehoseFactory;
    }

    @JsonProperty("firehose")
    public FirehoseFactory getFirehoseFactory()
    {
      return firehoseFactory;
    }
  }

  @JsonTypeName("index")
  public static class IndexTuningConfig implements TuningConfig
  {
    private static final int DEFAULT_TARGET_PARTITION_SIZE = 5000000;
    private static final int DEFAULT_ROW_FLUSH_BOUNDARY = 500000;

    private final int targetPartitionSize;
    private final int rowFlushBoundary;
    private final int numShards;

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") int targetPartitionSize,
        @JsonProperty("rowFlushBoundary") int rowFlushBoundary,
        @JsonProperty("numShards") @Nullable Integer numShards
    )
    {
      this.targetPartitionSize = targetPartitionSize == 0 ? DEFAULT_TARGET_PARTITION_SIZE : targetPartitionSize;
      this.rowFlushBoundary = rowFlushBoundary == 0 ? DEFAULT_ROW_FLUSH_BOUNDARY : rowFlushBoundary;
      this.numShards = numShards == null ? -1 : numShards;
      Preconditions.checkArgument(
          this.targetPartitionSize == -1 || this.numShards == -1,
          "targetPartitionsSize and shardCount both cannot be set"
      );
    }

    @JsonProperty
    public int getTargetPartitionSize()
    {
      return targetPartitionSize;
    }

    @JsonProperty
    public int getRowFlushBoundary()
    {
      return rowFlushBoundary;
    }

    @JsonProperty
    public int getNumShards()
    {
      return numShards;
    }
  }
}
