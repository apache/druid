/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.index.YeOldePlumberSchool;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.guava.Comparators;
import io.druid.java.util.common.logger.Logger;
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
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.NumberedShardSpec;
import io.druid.timeline.partition.ShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
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

  static RealtimeTuningConfig convertTuningConfig(
      ShardSpec shardSpec,
      int rowFlushBoundary,
      IndexSpec indexSpec,
      boolean buildV9Directly
  )
  {
    return new RealtimeTuningConfig(
        rowFlushBoundary,
        null,
        null,
        null,
        null,
        null,
        null,
        shardSpec,
        indexSpec,
        buildV9Directly,
        0,
        0,
        true,
        null
    );
  }

  @JsonIgnore
  private final IndexIngestionSpec ingestionSchema;

  private final ObjectMapper jsonMapper;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("spec") IndexIngestionSpec ingestionSchema,
      @JacksonInject ObjectMapper jsonMapper,
      @JsonProperty("context") Map<String, Object> context
  )
  {
    super(
        // _not_ the version, just something uniqueish
        makeId(id, ingestionSchema),
        taskResource,
        makeDataSource(ingestionSchema),
        makeInterval(ingestionSchema),
        context
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
            shardSpecs.add(new HashBasedNumberedShardSpec(i, numShards, null, jsonMapper));
          }
        } else {
          shardSpecs = ImmutableList.<ShardSpec>of(NoneShardSpec.instance());
        }
      }
      for (final ShardSpec shardSpec : shardSpecs) {
        // ShardSpec to be published.
        final ShardSpec shardSpecForPublishing;
        if (ingestionSchema.getTuningConfig().isForceExtendableShardSpecs()) {
          shardSpecForPublishing = new NumberedShardSpec(
              shardSpec.getPartitionNum(),
              shardSpecs.size()
          );
        } else {
          shardSpecForPublishing = shardSpec;
        }

        final DataSegment segment = generateSegment(
            toolbox,
            ingestionSchema.getDataSchema(),
            shardSpec,
            shardSpecForPublishing,
            bucket,
            myLock.getVersion()
        );
        segments.add(segment);
      }
    }
    toolbox.publishSegments(segments);
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
              hashFunction.hashBytes(jsonMapper.writeValueAsBytes(groupKey)).asBytes()
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
      shardSpecs.add(NoneShardSpec.instance());
    } else {
      for (int i = 0; i < numberOfShards; ++i) {
        shardSpecs.add(new HashBasedNumberedShardSpec(i, numberOfShards, null, jsonMapper));
      }
    }

    return shardSpecs;
  }

  private DataSegment generateSegment(
      final TaskToolbox toolbox,
      final DataSchema schema,
      final ShardSpec shardSpecForPartitioning,
      final ShardSpec shardSpecForPublishing,
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
            shardSpecForPartitioning.getPartitionNum()
        )
    );

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();
    final int rowFlushBoundary = ingestionSchema.getTuningConfig().getRowFlushBoundary();

    // We need to track published segments.
    final List<DataSegment> pushedSegments = new CopyOnWriteArrayList<DataSegment>();
    final DataSegmentPusher wrappedDataSegmentPusher = new DataSegmentPusher()
    {
      @Deprecated
      @Override
      public String getPathForHadoop(String dataSource)
      {
        return getPathForHadoop();
      }

      @Override
      public String getPathForHadoop()
      {
        return toolbox.getSegmentPusher().getPathForHadoop();
      }

      @Override
      public DataSegment push(File file, DataSegment segment) throws IOException
      {
        final DataSegment pushedSegment = toolbox.getSegmentPusher().push(file, segment);
        pushedSegments.add(pushedSegment);
        return pushedSegment;
      }
    };

    // rowFlushBoundary for this job
    final int myRowFlushBoundary = rowFlushBoundary > 0
                                   ? rowFlushBoundary
                                   : toolbox.getConfig().getDefaultRowFlushBoundary();

    // Create firehose + plumber
    final FireDepartmentMetrics metrics = new FireDepartmentMetrics();
    final Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser());
    final Supplier<Committer> committerSupplier = Committers.supplierFromFirehose(firehose);
    final Plumber plumber = new YeOldePlumberSchool(
        interval,
        version,
        wrappedDataSegmentPusher,
        tmpDir,
        toolbox.getIndexMerger(),
        toolbox.getIndexMergerV9(),
        toolbox.getIndexIO()
    ).findPlumber(
        schema,
        convertTuningConfig(
            shardSpecForPublishing,
            myRowFlushBoundary,
            ingestionSchema.getTuningConfig().getIndexSpec(),
            ingestionSchema.tuningConfig.getBuildV9Directly()
        ),
        metrics
    );

    final QueryGranularity rollupGran = ingestionSchema.getDataSchema().getGranularitySpec().getQueryGranularity();
    try {
      plumber.startJob();

      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();

        if (shouldIndex(shardSpecForPartitioning, interval, inputRow, rollupGran)) {
          int numRows = plumber.add(inputRow, committerSupplier);
          if (numRows == -1) {
            throw new ISE(
                String.format(
                    "Was expecting non-null sink for timestamp[%s]",
                    new DateTime(inputRow.getTimestampFromEpoch())
                )
            );
          }
          metrics.incrementProcessed();
        } else {
          metrics.incrementThrownAway();
        }
      }
    }
    finally {
      firehose.close();
    }

    plumber.persist(committerSupplier.get());

    try {
      plumber.finishJob();
    }
    finally {
      log.info(
          "Task[%s] interval[%s] partition[%d] took in %,d rows (%,d processed, %,d unparseable, %,d thrown away)"
          + " and output %,d rows",
          getId(),
          interval,
          shardSpecForPartitioning.getPartitionNum(),
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
      this.tuningConfig = tuningConfig == null ? new IndexTuningConfig(0, 0, null, null, null, false) : tuningConfig;
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
    private static final int DEFAULT_ROW_FLUSH_BOUNDARY = 75000;
    private static final IndexSpec DEFAULT_INDEX_SPEC = new IndexSpec();
    private static final Boolean DEFAULT_BUILD_V9_DIRECTLY = Boolean.FALSE;

    private final int targetPartitionSize;
    private final int rowFlushBoundary;
    private final int numShards;
    private final IndexSpec indexSpec;
    private final Boolean buildV9Directly;
    private final boolean forceExtendableShardSpecs;

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") int targetPartitionSize,
        @JsonProperty("rowFlushBoundary") int rowFlushBoundary,
        @JsonProperty("numShards") @Nullable Integer numShards,
        @JsonProperty("indexSpec") @Nullable IndexSpec indexSpec,
        @JsonProperty("buildV9Directly") Boolean buildV9Directly,
        @JsonProperty("forceExtendableShardSpecs") boolean forceExtendableShardSpecs
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
      this.buildV9Directly = buildV9Directly == null ? DEFAULT_BUILD_V9_DIRECTLY : buildV9Directly;
      this.forceExtendableShardSpecs = forceExtendableShardSpecs;
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

    @JsonProperty
    public Boolean getBuildV9Directly()
    {
      return buildV9Directly;
    }

    @JsonProperty
    public boolean isForceExtendableShardSpecs()
    {
      return forceExtendableShardSpecs;
    }
  }
}
