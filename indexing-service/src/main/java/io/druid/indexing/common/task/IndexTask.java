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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultiset;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.guava.Comparators;
import com.metamx.common.logger.Logger;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseFactory;
import io.druid.data.input.InputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.indexing.common.TaskLock;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.TaskToolbox;
import io.druid.indexing.common.index.YeOldePlumberSchool;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.IngestionSpec;
import io.druid.segment.indexing.TuningConfig;
import io.druid.segment.indexing.IOConfig;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.plumber.Plumber;
import io.druid.segment.realtime.plumber.Sink;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.joda.time.DateTime;
import org.joda.time.Interval;

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

  private static String makeId(String id, IndexIngestionSpec ingestionSchema, String dataSource)
  {
    if (id == null) {
      return String.format("index_%s_%s", makeDataSource(ingestionSchema, dataSource), new DateTime().toString());
    }

    return id;
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
  private final IndexIngestionSpec ingestionSchema;

  @JsonCreator
  public IndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("schema") IndexIngestionSpec ingestionSchema,
      // Backwards Compatible
      @JsonProperty("dataSource") final String dataSource,
      @JsonProperty("granularitySpec") final GranularitySpec granularitySpec,
      @JsonProperty("aggregators") final AggregatorFactory[] aggregators,
      @JsonProperty("indexGranularity") final QueryGranularity indexGranularity,
      @JsonProperty("targetPartitionSize") final int targetPartitionSize,
      @JsonProperty("firehose") final FirehoseFactory firehoseFactory,
      @JsonProperty("rowFlushBoundary") final int rowFlushBoundary
  )
  {
    super(
        // _not_ the version, just something uniqueish
        makeId(id, ingestionSchema, dataSource),
        makeDataSource(ingestionSchema, dataSource),
        makeInterval(ingestionSchema, granularitySpec)
    );

    if (ingestionSchema != null) {
      this.ingestionSchema = ingestionSchema;
    } else { // Backwards Compatible
      this.ingestionSchema = new IndexIngestionSpec(
          new DataSchema(
              dataSource,
              firehoseFactory.getParser(),
              aggregators,
              granularitySpec.withQueryGranularity(indexGranularity == null ? QueryGranularity.NONE : indexGranularity)
          ),
          new IndexIOConfig(firehoseFactory),
          new IndexTuningConfig(targetPartitionSize, rowFlushBoundary)
      );
    }
  }

  @Override
  public String getType()
  {
    return "index";
  }

  @JsonProperty("schema")
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
        shardSpecs = determinePartitions(bucket, targetPartitionSize);
      } else {
        shardSpecs = ImmutableList.<ShardSpec>of(new NoneShardSpec());
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
    try (Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser())) {
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
      final int targetPartitionSize
  ) throws IOException
  {
    log.info("Determining partitions for interval[%s] with targetPartitionSize[%d]", interval, targetPartitionSize);

    final FirehoseFactory firehoseFactory = ingestionSchema.getIOConfig().getFirehoseFactory();

    // The implementation of this determine partitions stuff is less than optimal.  Should be done better.

    // Blacklist dimensions that have multiple values per row
    final Set<String> unusableDimensions = com.google.common.collect.Sets.newHashSet();
    // Track values of all non-blacklisted dimensions
    final Map<String, TreeMultiset<String>> dimensionValueMultisets = Maps.newHashMap();

    // Load data
    try (Firehose firehose = firehoseFactory.connect(ingestionSchema.getDataSchema().getParser())) {
      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();
        if (interval.contains(inputRow.getTimestampFromEpoch())) {
          // Extract dimensions from event
          for (final String dim : inputRow.getDimensions()) {
            final List<String> dimValues = inputRow.getDimension(dim);
            if (!unusableDimensions.contains(dim)) {
              if (dimValues.size() == 1) {
                // Track this value
                TreeMultiset<String> dimensionValueMultiset = dimensionValueMultisets.get(dim);
                if (dimensionValueMultiset == null) {
                  dimensionValueMultiset = TreeMultiset.create();
                  dimensionValueMultisets.put(dim, dimensionValueMultiset);
                }
                dimensionValueMultiset.add(dimValues.get(0));
              } else {
                // Only single-valued dimensions can be used for partitions
                unusableDimensions.add(dim);
                dimensionValueMultisets.remove(dim);
              }
            }
          }
        }
      }
    }

    // ShardSpecs we will return
    final List<ShardSpec> shardSpecs = Lists.newArrayList();

    // Select highest-cardinality dimension
    Ordering<Map.Entry<String, TreeMultiset<String>>> byCardinalityOrdering = new Ordering<Map.Entry<String, TreeMultiset<String>>>()
    {
      @Override
      public int compare(
          Map.Entry<String, TreeMultiset<String>> left,
          Map.Entry<String, TreeMultiset<String>> right
      )
      {
        return Ints.compare(left.getValue().elementSet().size(), right.getValue().elementSet().size());
      }
    };

    if (dimensionValueMultisets.isEmpty()) {
      // No suitable partition dimension. We'll make one big segment and hope for the best.
      log.info("No suitable partition dimension found");
      shardSpecs.add(new NoneShardSpec());
    } else {
      // Find best partition dimension (heuristic: highest cardinality).
      final Map.Entry<String, TreeMultiset<String>> partitionEntry =
          byCardinalityOrdering.max(dimensionValueMultisets.entrySet());

      final String partitionDim = partitionEntry.getKey();
      final TreeMultiset<String> partitionDimValues = partitionEntry.getValue();

      log.info(
          "Partitioning on dimension[%s] with cardinality[%d] over rows[%d]",
          partitionDim,
          partitionDimValues.elementSet().size(),
          partitionDimValues.size()
      );

      // Iterate over unique partition dimension values in sorted order
      String currentPartitionStart = null;
      int currentPartitionSize = 0;
      for (final String partitionDimValue : partitionDimValues.elementSet()) {
        currentPartitionSize += partitionDimValues.count(partitionDimValue);
        if (currentPartitionSize >= targetPartitionSize) {
          final ShardSpec shardSpec = new SingleDimensionShardSpec(
              partitionDim,
              currentPartitionStart,
              partitionDimValue,
              shardSpecs.size()
          );

          log.info("Adding shard: %s", shardSpec);
          shardSpecs.add(shardSpec);

          currentPartitionSize = partitionDimValues.count(partitionDimValue);
          currentPartitionStart = partitionDimValue;
        }
      }

      if (currentPartitionSize > 0) {
        // One last shard to go
        final ShardSpec shardSpec;

        if (shardSpecs.isEmpty()) {
          shardSpec = new NoneShardSpec();
        } else {
          shardSpec = new SingleDimensionShardSpec(
              partitionDim,
              currentPartitionStart,
              null,
              shardSpecs.size()
          );
        }

        log.info("Adding shard: %s", shardSpec);
        shardSpecs.add(shardSpec);
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
    ).findPlumber(schema, new RealtimeTuningConfig(null, null, null, null, null, null, null, shardSpec), metrics);

    // rowFlushBoundary for this job
    final int myRowFlushBoundary = rowFlushBoundary > 0
                                   ? rowFlushBoundary
                                   : toolbox.getConfig().getDefaultRowFlushBoundary();

    try {
      plumber.startJob();

      while (firehose.hasMore()) {
        final InputRow inputRow = firehose.nextRow();

        if (shouldIndex(shardSpec, interval, inputRow)) {
          final Sink sink = plumber.getSink(inputRow.getTimestampFromEpoch());
          if (sink == null) {
            throw new NullPointerException(
                String.format(
                    "Was expecting non-null sink for timestamp[%s]",
                    new DateTime(inputRow.getTimestampFromEpoch())
                )
            );
          }

          int numRows = sink.add(inputRow);
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

  /**
   * Should we index this inputRow? Decision is based on our interval and shardSpec.
   *
   * @param inputRow the row to check
   *
   * @return true or false
   */
  private boolean shouldIndex(
      final ShardSpec shardSpec,
      final Interval interval,
      final InputRow inputRow
  )
  {
    return interval.contains(inputRow.getTimestampFromEpoch()) && shardSpec.isInChunk(inputRow);
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
      this.tuningConfig = tuningConfig;
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
    private final int targetPartitionSize;
    private final int rowFlushBoundary;

    @JsonCreator
    public IndexTuningConfig(
        @JsonProperty("targetPartitionSize") int targetPartitionSize,
        @JsonProperty("rowFlushBoundary") int rowFlushBoundary
    )
    {
      this.targetPartitionSize = targetPartitionSize;
      this.rowFlushBoundary = rowFlushBoundary;
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
  }
}
