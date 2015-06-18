/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.indexing.common.task;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
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
import io.druid.query.aggregation.hyperloglog.HyperLogLogCollector;
import io.druid.segment.IndexSpec;
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

  private static String makeId(String id, IndexIngestionSpec ingestionSchema)
  {
    if (id == null) {
      return String.format("index_%s_%s", makeDataSource(ingestionSchema), new DateTime().toString());
    }

    return id;
  }

  private static String makeDataSource(IndexIngestionSpec ingestionSchema)
  {
    return ingestionSchema.getDataSchema().getDataSource();
  }

  private static Interval makeInterval(IndexIngestionSpec ingestionSchema)
  {
    GranularitySpec spec = ingestionSchema.getDataSchema().getGranularitySpec();

    return new Interval(
        spec.bucketIntervals().get().first().getStart(),
        spec.bucketIntervals().get().last().getEnd()
    );
  }

  static RealtimeTuningConfig convertTuningConfig(ShardSpec spec, IndexTuningConfig config)
  {
    return new RealtimeTuningConfig(
        config.getRowFlushBoundary(),
        null,
        null,
        null,
        null,
        null,
        null,
        spec,
        config.getIndexSpec(),
        null,
        null,
        null
    );
  }

  @JsonIgnore
  private final IndexIngestionSpec ingestionSchema;

  private final ObjectMapper jsonMapper;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("spec") IndexIngestionSpec ingestionSchema,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(
        // _not_ the version, just something uniqueish
        makeId(id, ingestionSchema),
        makeDataSource(ingestionSchema),
        makeInterval(ingestionSchema)
    );


    this.ingestionSchema = ingestionSchema;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public String getType()
  {
    return "index";
  }

  @JsonProperty("spec")
  public IndexIngestionSpec getIngestionSchema()
  {
    return ingestionSchema;
  }

  @Override
  public TaskStatus run(TaskToolbox toolbox) throws Exception
  {
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();
    final int targetPartitionSize = ingestionSchema.getTuningConfig().getTargetPartitionSize();

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
        int numShards = ingestionSchema.getTuningConfig().getNumShards();
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
            ingestionSchema.getDataSchema(),
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
    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    final GranularitySpec granularitySpec = ingestionSchema.getDataSchema().getGranularitySpec();

    SortedSet<Interval> retVal = Sets.newTreeSet(Comparators.intervalsByStartThenEnd());
    int unparsed = 0;
    try (Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser())) {
      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        DateTime dt = new DateTime(inputRow.getTimestampFromEpoch());
        Optional<Interval> interval = granularitySpec.bucketInterval(dt);
        if (interval.isPresent()) {
          retVal.add(interval.get());
        } else {
          unparsed++;
        }
      }
    }
    if (unparsed > 0) {
      log.warn("Unable to to find a matching interval for [%,d] events", unparsed);
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

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

    // The implementation of this determine partitions stuff is less than optimal.  Should be done better.
    // Use HLL to estimate number of rows
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();

    // Load data
    try (Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser())) {
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

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    final int rowFlushBoundary = ingestionSchema.getTuningConfig().getRowFlushBoundary();

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
    final Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser());
    final Plumber plumber = new YeOldePlumberSchool(
        interval,
        version,
        wrappedDataSegmentPusher,
        tmpDir
    ).findPlumber(
        schema,
        convertTuningConfig(shardSpec, ingestionSchema.getTuningConfig()),
        metrics
    );

    // rowFlushBoundary for this job
    final int myRowFlushBoundary = rowFlushBoundary > 0
                                   ? rowFlushBoundary
                                   : toolbox.getConfig().getDefaultRowFlushBoundary();
    final QueryGranularity rollupGran = ingestionSchema.getDataSchema().getGranularitySpec().getQueryGranularity();
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
      this.tuningConfig = tuningConfig == null ? new IndexTuningConfig(0, 0, null, null) : tuningConfig;
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
    private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();

    private final int targetPartitionSize;
    private final int rowFlushBoundary;
    private final int numShards;
    private final IndexSpec indexSpec;

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") int targetPartitionSize,
        @JsonProperty("rowFlushBoundary") int rowFlushBoundary,
        @JsonProperty("numShards") @Nullable Integer numShards,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec
    )
    {
      this.targetPartitionSize = targetPartitionSize == 0 ? DEFAULT_TARGET_PARTITION_SIZE : targetPartitionSize;
      Preconditions.checkArgument(rowFlushBoundary >= 0, "rowFlushBoundary should be positive or zero");
      this.rowFlushBoundary = rowFlushBoundary == 0 ? DEFAULT_ROW_FLUSH_BOUNDARY : rowFlushBoundary;
      this.numShards = numShards == null ? -1 : numShards;
      this.indexSpec = indexSpec == null ? DEFAULT_INDEX_SPEC : indexSpec;
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

    @JsonProperty
    public IndexSpec getIndexSpec()
    {
      return indexSpec;
    }
  }
}
