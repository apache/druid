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

package io.druid.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.Closeables;
import io.druid.collections.CombiningIterable;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.granularity.Granularity;
import io.druid.java.util.common.guava.nary.BinaryFn;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.partition.NoneShardSpec;
import io.druid.timeline.partition.ShardSpec;
import io.druid.timeline.partition.SingleDimensionShardSpec;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Determines appropriate ShardSpecs for a job by determining whether or not partitioning is necessary, and if so,
 * choosing the best dimension that satisfies the criteria:
 * <p/>
 * <ul>
 * <li>Must have exactly one value per row.</li>
 * <li>Must not generate oversized partitions. A dimension with N rows having the same value will necessarily
 * put all those rows in the same partition, and that partition may be much larger than the target size.</li>
 * </ul>
 * <p/>
 * "Best" means a very high cardinality dimension, or, if none exist, the dimension that minimizes variation of
 * segment size relative to the target.
 */
public class DeterminePartitionsJob implements Jobby
{
  private static final Logger log = new Logger(DeterminePartitionsJob.class);

  private static final Joiner TAB_JOINER = HadoopDruidIndexerConfig.TAB_JOINER;
  private static final Splitter TAB_SPLITTER = HadoopDruidIndexerConfig.TAB_SPLITTER;

  private final HadoopDruidIndexerConfig config;

  public DeterminePartitionsJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
  }

  @Override
  public boolean run()
  {
    try {
      /*
       * Group by (timestamp, dimensions) so we can correctly count dimension values as they would appear
       * in the final segment.
       */

      if (!(config.getPartitionsSpec() instanceof SingleDimensionPartitionsSpec)) {
        throw new ISE(
            "DeterminePartitionsJob can only be run for SingleDimensionPartitionsSpec, partitionSpec found [%s]",
            config.getPartitionsSpec()
        );
      }

      if (!config.getPartitionsSpec().isAssumeGrouped()) {
        final Job groupByJob = Job.getInstance(
            new Configuration(),
            StringUtils.format("%s-determine_partitions_groupby-%s", config.getDataSource(), config.getIntervals())
        );

        JobHelper.injectSystemProperties(groupByJob);
        config.addJobProperties(groupByJob);

        groupByJob.setMapperClass(DeterminePartitionsGroupByMapper.class);
        groupByJob.setMapOutputKeyClass(BytesWritable.class);
        groupByJob.setMapOutputValueClass(NullWritable.class);
        groupByJob.setCombinerClass(DeterminePartitionsGroupByReducer.class);
        groupByJob.setReducerClass(DeterminePartitionsGroupByReducer.class);
        groupByJob.setOutputKeyClass(BytesWritable.class);
        groupByJob.setOutputValueClass(NullWritable.class);
        groupByJob.setOutputFormatClass(SequenceFileOutputFormat.class);
        JobHelper.setupClasspath(
            JobHelper.distributedClassPath(config.getWorkingPath()),
            JobHelper.distributedClassPath(config.makeIntermediatePath()),
            groupByJob
        );

        config.addInputPaths(groupByJob);
        config.intoConfiguration(groupByJob);
        FileOutputFormat.setOutputPath(groupByJob, config.makeGroupedDataDir());

        groupByJob.submit();
        log.info("Job %s submitted, status available at: %s", groupByJob.getJobName(), groupByJob.getTrackingURL());

        if (!groupByJob.waitForCompletion(true)) {
          log.error("Job failed: %s", groupByJob.getJobID());
          return false;
        }
      } else {
        log.info("Skipping group-by job.");
      }

      /*
       * Read grouped data and determine appropriate partitions.
       */
      final Job dimSelectionJob = Job.getInstance(
          new Configuration(),
          StringUtils.format("%s-determine_partitions_dimselection-%s", config.getDataSource(), config.getIntervals())
      );

      dimSelectionJob.getConfiguration().set("io.sort.record.percent", "0.19");

      JobHelper.injectSystemProperties(dimSelectionJob);
      config.addJobProperties(dimSelectionJob);

      if (!config.getPartitionsSpec().isAssumeGrouped()) {
        // Read grouped data from the groupByJob.
        dimSelectionJob.setMapperClass(DeterminePartitionsDimSelectionPostGroupByMapper.class);
        dimSelectionJob.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.addInputPath(dimSelectionJob, config.makeGroupedDataDir());
      } else {
        // Directly read the source data, since we assume it's already grouped.
        dimSelectionJob.setMapperClass(DeterminePartitionsDimSelectionAssumeGroupedMapper.class);
        config.addInputPaths(dimSelectionJob);
      }

      SortableBytes.useSortableBytesAsMapOutputKey(dimSelectionJob);
      dimSelectionJob.setMapOutputValueClass(Text.class);
      dimSelectionJob.setCombinerClass(DeterminePartitionsDimSelectionCombiner.class);
      dimSelectionJob.setReducerClass(DeterminePartitionsDimSelectionReducer.class);
      dimSelectionJob.setOutputKeyClass(BytesWritable.class);
      dimSelectionJob.setOutputValueClass(Text.class);
      dimSelectionJob.setOutputFormatClass(DeterminePartitionsDimSelectionOutputFormat.class);
      dimSelectionJob.setPartitionerClass(DeterminePartitionsDimSelectionPartitioner.class);
      dimSelectionJob.setNumReduceTasks(config.getGranularitySpec().bucketIntervals().get().size());
      JobHelper.setupClasspath(
          JobHelper.distributedClassPath(config.getWorkingPath()),
          JobHelper.distributedClassPath(config.makeIntermediatePath()),
          dimSelectionJob
      );

      config.intoConfiguration(dimSelectionJob);
      FileOutputFormat.setOutputPath(dimSelectionJob, config.makeIntermediatePath());

      dimSelectionJob.submit();
      log.info(
          "Job %s submitted, status available at: %s",
          dimSelectionJob.getJobName(),
          dimSelectionJob.getTrackingURL()
      );

      if (!dimSelectionJob.waitForCompletion(true)) {
        log.error("Job failed: %s", dimSelectionJob.getJobID().toString());
        return false;
      }

      /*
       * Load partitions determined by the previous job.
       */

      log.info("Job completed, loading up partitions for intervals[%s].", config.getSegmentGranularIntervals());
      FileSystem fileSystem = null;
      Map<Long, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap();
      int shardCount = 0;
      for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
        final Path partitionInfoPath = config.makeSegmentPartitionInfoPath(segmentGranularity);
        if (fileSystem == null) {
          fileSystem = partitionInfoPath.getFileSystem(dimSelectionJob.getConfiguration());
        }
        if (Utils.exists(dimSelectionJob, fileSystem, partitionInfoPath)) {
          List<ShardSpec> specs = config.JSON_MAPPER.readValue(
              Utils.openInputStream(dimSelectionJob, partitionInfoPath), new TypeReference<List<ShardSpec>>()
              {
              }
          );

          List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithExpectedSize(specs.size());
          for (int i = 0; i < specs.size(); ++i) {
            actualSpecs.add(new HadoopyShardSpec(specs.get(i), shardCount++));
            log.info("DateTime[%s], partition[%d], spec[%s]", segmentGranularity, i, actualSpecs.get(i));
          }

          shardSpecs.put(segmentGranularity.getStartMillis(), actualSpecs);
        } else {
          log.info("Path[%s] didn't exist!?", partitionInfoPath);
        }
      }
      config.setShardSpecs(shardSpecs);

      return true;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static class DeterminePartitionsGroupByMapper extends HadoopDruidIndexerMapper<BytesWritable, NullWritable>
  {
    private Granularity rollupGranularity = null;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      super.setup(context);
      rollupGranularity = getConfig().getGranularitySpec().getQueryGranularity();
    }

    @Override
    protected void innerMap(
        InputRow inputRow,
        Object value,
        Context context,
        boolean reportParseExceptions
    ) throws IOException, InterruptedException
    {
      final List<Object> groupKey = Rows.toGroupKey(
          rollupGranularity.bucketStart(inputRow.getTimestamp()).getMillis(),
          inputRow
      );
      context.write(
          new BytesWritable(HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsBytes(groupKey)),
          NullWritable.get()
      );
    }
  }

  public static class DeterminePartitionsGroupByReducer
      extends Reducer<BytesWritable, NullWritable, BytesWritable, NullWritable>
  {
    @Override
    protected void reduce(
        BytesWritable key,
        Iterable<NullWritable> values,
        Context context
    ) throws IOException, InterruptedException
    {
      context.write(key, NullWritable.get());
    }
  }

  /**
   * This DimSelection mapper runs on data generated by our GroupBy job.
   */
  public static class DeterminePartitionsDimSelectionPostGroupByMapper
      extends Mapper<BytesWritable, NullWritable, BytesWritable, Text>
  {
    private DeterminePartitionsDimSelectionMapperHelper helper;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      final HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
      SingleDimensionPartitionsSpec spec = (SingleDimensionPartitionsSpec) config.getPartitionsSpec();
      helper = new DeterminePartitionsDimSelectionMapperHelper(config, spec.getPartitionDimension());
    }

    @Override
    protected void map(
        BytesWritable key, NullWritable value, Context context
    ) throws IOException, InterruptedException
    {
      final List<Object> timeAndDims = HadoopDruidIndexerConfig.JSON_MAPPER.readValue(key.getBytes(), List.class);

      final DateTime timestamp = new DateTime(timeAndDims.get(0));
      final Map<String, Iterable<String>> dims = (Map<String, Iterable<String>>) timeAndDims.get(1);

      helper.emitDimValueCounts(context, timestamp, dims);
    }
  }

  /**
   * This DimSelection mapper runs on raw input data that we assume has already been grouped.
   */
  public static class DeterminePartitionsDimSelectionAssumeGroupedMapper
      extends HadoopDruidIndexerMapper<BytesWritable, Text>
  {
    private DeterminePartitionsDimSelectionMapperHelper helper;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      super.setup(context);
      final HadoopDruidIndexerConfig config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
      final SingleDimensionPartitionsSpec spec = (SingleDimensionPartitionsSpec) config.getPartitionsSpec();
      helper = new DeterminePartitionsDimSelectionMapperHelper(config, spec.getPartitionDimension());
    }

    @Override
    protected void innerMap(
        InputRow inputRow,
        Object value,
        Context context,
        boolean reportParseExceptions
    ) throws IOException, InterruptedException
    {
      final Map<String, Iterable<String>> dims = Maps.newHashMap();
      for (final String dim : inputRow.getDimensions()) {
        dims.put(dim, inputRow.getDimension(dim));
      }
      helper.emitDimValueCounts(context, new DateTime(inputRow.getTimestampFromEpoch()), dims);
    }
  }

  /**
   * Since we have two slightly different DimSelectionMappers, this class encapsulates the shared logic for
   * emitting dimension value counts.
   */
  public static class DeterminePartitionsDimSelectionMapperHelper
  {
    private final HadoopDruidIndexerConfig config;
    private final String partitionDimension;
    private final Map<Long, Integer> intervalIndexes;

    public DeterminePartitionsDimSelectionMapperHelper(HadoopDruidIndexerConfig config, String partitionDimension)
    {
      this.config = config;
      this.partitionDimension = partitionDimension;

      final ImmutableMap.Builder<Long, Integer> timeIndexBuilder = ImmutableMap.builder();
      int idx = 0;
      for (final Interval bucketInterval : config.getGranularitySpec().bucketIntervals().get()) {
        timeIndexBuilder.put(bucketInterval.getStartMillis(), idx);
        idx++;
      }

      this.intervalIndexes = timeIndexBuilder.build();
    }

    public void emitDimValueCounts(
        TaskInputOutputContext<?, ?, BytesWritable, Text> context,
        DateTime timestamp,
        Map<String, Iterable<String>> dims
    ) throws IOException, InterruptedException
    {
      final Optional<Interval> maybeInterval = config.getGranularitySpec().bucketInterval(timestamp);

      if (!maybeInterval.isPresent()) {
        throw new ISE("WTF?! No bucket found for timestamp: %s", timestamp);
      }

      final Interval interval = maybeInterval.get();
      final int intervalIndex = intervalIndexes.get(interval.getStartMillis());

      final ByteBuffer buf = ByteBuffer.allocate(4 + 8);
      buf.putInt(intervalIndex);
      buf.putLong(interval.getStartMillis());
      final byte[] groupKey = buf.array();

      // Emit row-counter value.
      write(context, groupKey, new DimValueCount("", "", 1));

      for (final Map.Entry<String, Iterable<String>> dimAndValues : dims.entrySet()) {
        final String dim = dimAndValues.getKey();

        if (partitionDimension == null || partitionDimension.equals(dim)) {
          final Iterable<String> dimValues = dimAndValues.getValue();

          if (Iterables.size(dimValues) == 1) {
            // Emit this value.
            write(context, groupKey, new DimValueCount(dim, Iterables.getOnlyElement(dimValues), 1));
          } else {
            // This dimension is unsuitable for partitioning. Poison it by emitting a negative value.
            write(context, groupKey, new DimValueCount(dim, "", -1));
          }
        }
      }
    }
  }

  public static class DeterminePartitionsDimSelectionPartitioner
      extends Partitioner<BytesWritable, Text> implements Configurable
  {
    private Configuration config;

    @Override
    public int getPartition(BytesWritable bytesWritable, Text text, int numPartitions)
    {
      final ByteBuffer bytes = ByteBuffer.wrap(bytesWritable.getBytes());
      bytes.position(4); // Skip length added by SortableBytes
      final int index = bytes.getInt();
      if (config.get("mapred.job.tracker").equals("local")) {
        return index % numPartitions;
      } else {
        if (index >= numPartitions) {
          throw new ISE(
              "Not enough partitions, index[%,d] >= numPartitions[%,d]. Please increase the number of reducers to the index size or check your config & settings!",
              index,
              numPartitions
          );
        }
      }

      return index;
    }

    @Override
    public Configuration getConf()
    {
      return config;
    }

    @Override
    public void setConf(Configuration config)
    {
      this.config = config;
    }
  }

  private static abstract class DeterminePartitionsDimSelectionBaseReducer
      extends Reducer<BytesWritable, Text, BytesWritable, Text>
  {
    protected volatile HadoopDruidIndexerConfig config = null;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      if (config == null) {
        synchronized (DeterminePartitionsDimSelectionBaseReducer.class) {
          if (config == null) {
            config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
          }
        }
      }
    }

    @Override
    protected void reduce(
        BytesWritable key, Iterable<Text> values, Context context
    ) throws IOException, InterruptedException
    {
      SortableBytes keyBytes = SortableBytes.fromBytesWritable(key);

      final Iterable<DimValueCount> combinedIterable = combineRows(values);
      innerReduce(context, keyBytes, combinedIterable);
    }

    protected abstract void innerReduce(
        Context context, SortableBytes keyBytes, Iterable<DimValueCount> combinedIterable
    ) throws IOException, InterruptedException;

    private Iterable<DimValueCount> combineRows(Iterable<Text> input)
    {
      return new CombiningIterable<>(
          Iterables.transform(
              input,
              new Function<Text, DimValueCount>()
              {
                @Override
                public DimValueCount apply(Text input)
                {
                  return DimValueCount.fromText(input);
                }
              }
          ),
          new Comparator<DimValueCount>()
          {
            @Override
            public int compare(DimValueCount o1, DimValueCount o2)
            {
              return ComparisonChain.start().compare(o1.dim, o2.dim).compare(o1.value, o2.value).result();
            }
          },
          new BinaryFn<DimValueCount, DimValueCount, DimValueCount>()
          {
            @Override
            public DimValueCount apply(DimValueCount arg1, DimValueCount arg2)
            {
              if (arg2 == null) {
                return arg1;
              }

              // Respect "poisoning" (negative values mean we can't use this dimension)
              final int newNumRows = (arg1.numRows >= 0 && arg2.numRows >= 0 ? arg1.numRows + arg2.numRows : -1);
              return new DimValueCount(arg1.dim, arg1.value, newNumRows);
            }
          }
      );
    }
  }

  public static class DeterminePartitionsDimSelectionCombiner extends DeterminePartitionsDimSelectionBaseReducer
  {
    @Override
    protected void innerReduce(
        Context context, SortableBytes keyBytes, Iterable<DimValueCount> combinedIterable
    ) throws IOException, InterruptedException
    {
      for (DimValueCount dvc : combinedIterable) {
        write(context, keyBytes.getGroupKey(), dvc);
      }
    }
  }

  public static class DeterminePartitionsDimSelectionReducer extends DeterminePartitionsDimSelectionBaseReducer
  {
    private static final double SHARD_COMBINE_THRESHOLD = 0.25;
    private static final int HIGH_CARDINALITY_THRESHOLD = 3000000;

    @Override
    protected void innerReduce(
        Context context, SortableBytes keyBytes, Iterable<DimValueCount> combinedIterable
    ) throws IOException, InterruptedException
    {
      final ByteBuffer groupKey = ByteBuffer.wrap(keyBytes.getGroupKey());
      groupKey.position(4); // Skip partition
      final DateTime bucket = new DateTime(groupKey.getLong());
      final PeekingIterator<DimValueCount> iterator = Iterators.peekingIterator(combinedIterable.iterator());

      log.info(
          "Determining partitions for interval: %s",
          config.getGranularitySpec().bucketInterval(bucket).orNull()
      );

      // First DVC should be the total row count indicator
      final DimValueCount firstDvc = iterator.next();
      final int totalRows = firstDvc.numRows;

      if (!firstDvc.dim.equals("") || !firstDvc.value.equals("")) {
        throw new IllegalStateException("WTF?! Expected total row indicator on first k/v pair!");
      }

      // "iterator" will now take us over many candidate dimensions
      DimPartitions currentDimPartitions = null;
      DimPartition currentDimPartition = null;
      String currentDimPartitionStart = null;
      boolean currentDimSkip = false;

      // We'll store possible partitions in here
      final Map<String, DimPartitions> dimPartitionss = Maps.newHashMap();

      while (iterator.hasNext()) {
        final DimValueCount dvc = iterator.next();

        if (currentDimPartitions == null || !currentDimPartitions.dim.equals(dvc.dim)) {
          // Starting a new dimension! Exciting!
          currentDimPartitions = new DimPartitions(dvc.dim);
          currentDimPartition = new DimPartition();
          currentDimPartitionStart = null;
          currentDimSkip = false;
        }

        // Respect poisoning
        if (!currentDimSkip && dvc.numRows < 0) {
          log.info("Cannot partition on multi-value dimension: %s", dvc.dim);
          currentDimSkip = true;
        }

        if (currentDimSkip) {
          continue;
        }

        // See if we need to cut a new partition ending immediately before this dimension value
        if (currentDimPartition.rows > 0 && currentDimPartition.rows + dvc.numRows >= config.getTargetPartitionSize()) {
          final ShardSpec shardSpec = new SingleDimensionShardSpec(
              currentDimPartitions.dim,
              currentDimPartitionStart,
              dvc.value,
              currentDimPartitions.partitions.size()
          );

          log.info(
              "Adding possible shard with %,d rows and %,d unique values: %s",
              currentDimPartition.rows,
              currentDimPartition.cardinality,
              shardSpec
          );

          currentDimPartition.shardSpec = shardSpec;
          currentDimPartitions.partitions.add(currentDimPartition);
          currentDimPartition = new DimPartition();
          currentDimPartitionStart = dvc.value;
        }

        // Update counters
        currentDimPartition.cardinality++;
        currentDimPartition.rows += dvc.numRows;

        if (!iterator.hasNext() || !currentDimPartitions.dim.equals(iterator.peek().dim)) {
          // Finalize the current dimension

          if (currentDimPartition.rows > 0) {
            // One more shard to go
            final ShardSpec shardSpec;

            if (currentDimPartitions.partitions.isEmpty()) {
              shardSpec = NoneShardSpec.instance();
            } else {
              if (currentDimPartition.rows < config.getTargetPartitionSize() * SHARD_COMBINE_THRESHOLD) {
                // Combine with previous shard
                final DimPartition previousDimPartition = currentDimPartitions.partitions.remove(
                    currentDimPartitions.partitions.size() - 1
                );

                final SingleDimensionShardSpec previousShardSpec = (SingleDimensionShardSpec) previousDimPartition.shardSpec;

                shardSpec = new SingleDimensionShardSpec(
                    currentDimPartitions.dim,
                    previousShardSpec.getStart(),
                    null,
                    previousShardSpec.getPartitionNum()
                );

                log.info("Removing possible shard: %s", previousShardSpec);

                currentDimPartition.rows += previousDimPartition.rows;
                currentDimPartition.cardinality += previousDimPartition.cardinality;
              } else {
                // Create new shard
                shardSpec = new SingleDimensionShardSpec(
                    currentDimPartitions.dim,
                    currentDimPartitionStart,
                    null,
                    currentDimPartitions.partitions.size()
                );
              }
            }

            log.info(
                "Adding possible shard with %,d rows and %,d unique values: %s",
                currentDimPartition.rows,
                currentDimPartition.cardinality,
                shardSpec
            );

            currentDimPartition.shardSpec = shardSpec;
            currentDimPartitions.partitions.add(currentDimPartition);
          }

          log.info(
              "Completed dimension[%s]: %,d possible shards with %,d unique values",
              currentDimPartitions.dim,
              currentDimPartitions.partitions.size(),
              currentDimPartitions.getCardinality()
          );

          // Add ourselves to the partitions map
          dimPartitionss.put(currentDimPartitions.dim, currentDimPartitions);
        }
      }

      // Choose best dimension
      if (dimPartitionss.isEmpty()) {
        throw new ISE("No suitable partitioning dimension found!");
      }

      int maxCardinality = Integer.MIN_VALUE;
      long minDistance = Long.MAX_VALUE;
      DimPartitions minDistancePartitions = null;
      DimPartitions maxCardinalityPartitions = null;

      for (final DimPartitions dimPartitions : dimPartitionss.values()) {
        if (dimPartitions.getRows() != totalRows) {
          log.info(
              "Dimension[%s] is not present in all rows (row count %,d != expected row count %,d)",
              dimPartitions.dim,
              dimPartitions.getRows(),
              totalRows
          );

          continue;
        }

        // Make sure none of these shards are oversized
        boolean oversized = false;
        for (final DimPartition partition : dimPartitions.partitions) {
          if (partition.rows > config.getMaxPartitionSize()) {
            log.info("Dimension[%s] has an oversized shard: %s", dimPartitions.dim, partition.shardSpec);
            oversized = true;
          }
        }

        if (oversized) {
          continue;
        }

        final int cardinality = dimPartitions.getCardinality();
        final long distance = dimPartitions.getDistanceSquaredFromTarget(config.getTargetPartitionSize());

        if (cardinality > maxCardinality) {
          maxCardinality = cardinality;
          maxCardinalityPartitions = dimPartitions;
        }

        if (distance < minDistance) {
          minDistance = distance;
          minDistancePartitions = dimPartitions;
        }
      }

      if (maxCardinalityPartitions == null) {
        throw new ISE("No suitable partitioning dimension found!");
      }

      final OutputStream out = Utils.makePathAndOutputStream(
          context,
          config.makeSegmentPartitionInfoPath(config.getGranularitySpec().bucketInterval(bucket).get()),
          config.isOverwriteFiles()
      );

      final DimPartitions chosenPartitions = maxCardinality > HIGH_CARDINALITY_THRESHOLD
                                             ? maxCardinalityPartitions
                                             : minDistancePartitions;

      final List<ShardSpec> chosenShardSpecs = Lists.transform(
          chosenPartitions.partitions, new Function<DimPartition, ShardSpec>()
          {
            @Override
            public ShardSpec apply(DimPartition dimPartition)
            {
              return dimPartition.shardSpec;
            }
          }
      );

      log.info("Chosen partitions:");
      for (ShardSpec shardSpec : chosenShardSpecs) {
        log.info("  %s", HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsString(shardSpec));
      }

      try {
        HadoopDruidIndexerConfig.JSON_MAPPER
            .writerWithType(
                new TypeReference<List<ShardSpec>>()
                {
                }
            )
            .writeValue(out, chosenShardSpecs);
      }
      finally {
        Closeables.close(out, false);
      }
    }
  }

  public static class DeterminePartitionsDimSelectionOutputFormat extends FileOutputFormat
  {
    @Override
    public RecordWriter getRecordWriter(final TaskAttemptContext job) throws IOException, InterruptedException
    {
      return new RecordWriter<SortableBytes, List<ShardSpec>>()
      {
        @Override
        public void write(SortableBytes keyBytes, List<ShardSpec> partitions) throws IOException, InterruptedException
        {
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException
        {

        }
      };
    }

    @Override
    public void checkOutputSpecs(JobContext job) throws IOException
    {
      Path outDir = getOutputPath(job);
      if (outDir == null) {
        throw new InvalidJobConfException("Output directory not set.");
      }
    }
  }

  private static class DimPartitions
  {
    public final String dim;
    public final List<DimPartition> partitions = Lists.newArrayList();

    private DimPartitions(String dim)
    {
      this.dim = dim;
    }

    public int getCardinality()
    {
      int sum = 0;
      for (final DimPartition dimPartition : partitions) {
        sum += dimPartition.cardinality;
      }
      return sum;
    }

    public long getDistanceSquaredFromTarget(long target)
    {
      long distance = 0;
      for (final DimPartition dimPartition : partitions) {
        distance += (dimPartition.rows - target) * (dimPartition.rows - target);
      }

      distance /= partitions.size();
      return distance;
    }

    public int getRows()
    {
      int sum = 0;
      for (final DimPartition dimPartition : partitions) {
        sum += dimPartition.rows;
      }
      return sum;
    }
  }

  private static class DimPartition
  {
    public ShardSpec shardSpec = null;
    public int cardinality = 0;
    public int rows = 0;
  }

  private static class DimValueCount
  {
    public final String dim;
    public final String value;
    public final int numRows;

    private DimValueCount(String dim, String value, int numRows)
    {
      this.dim = dim;
      this.value = value;
      this.numRows = numRows;
    }

    public Text toText()
    {
      return new Text(TAB_JOINER.join(dim, String.valueOf(numRows), value));
    }

    public static DimValueCount fromText(Text text)
    {
      final Iterator<String> splits = TAB_SPLITTER.limit(3).split(text.toString()).iterator();
      final String dim = splits.next();
      final int numRows = Integer.parseInt(splits.next());
      final String value = splits.next();

      return new DimValueCount(dim, value, numRows);
    }
  }

  private static void write(
      TaskInputOutputContext<?, ?, BytesWritable, Text> context,
      final byte[] groupKey,
      DimValueCount dimValueCount
  )
      throws IOException, InterruptedException
  {
    context.write(
        new SortableBytes(
            groupKey, TAB_JOINER.join(dimValueCount.dim, dimValueCount.value).getBytes(
            HadoopDruidIndexerConfig.JAVA_NATIVE_CHARSET
        )
        ).toBytesWritable(),
        dimValueCount.toText()
    );
  }
}
