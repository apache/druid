/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Closeables;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.granularity.QueryGranularity;
import io.druid.indexer.granularity.UniformGranularitySpec;
import io.druid.timeline.partition.HashBasedNumberedShardSpec;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Determines appropriate ShardSpecs for a job by determining approximate cardinality of data set using HyperLogLog
 */
public class DetermineHashedPartitionsJob implements Jobby
{
  private static final int MAX_SHARDS = 128;
  private static final Logger log = new Logger(DetermineHashedPartitionsJob.class);
  private static final int HYPER_LOG_LOG_BIT_SIZE = 20;
  private final HadoopDruidIndexerConfig config;

  public DetermineHashedPartitionsJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
  }

  public boolean run()
  {
    try {
      /*
       * Group by (timestamp, dimensions) so we can correctly count dimension values as they would appear
       * in the final segment.
       */
      long startTime = System.currentTimeMillis();
      final Job groupByJob = new Job(
          new Configuration(),
          String.format("%s-determine_partitions_hashed-%s", config.getDataSource(), config.getIntervals())
      );

      JobHelper.injectSystemProperties(groupByJob);
      groupByJob.setInputFormatClass(TextInputFormat.class);
      groupByJob.setMapperClass(DetermineCardinalityMapper.class);
      groupByJob.setMapOutputKeyClass(LongWritable.class);
      groupByJob.setMapOutputValueClass(BytesWritable.class);
      groupByJob.setReducerClass(DetermineCardinalityReducer.class);
      groupByJob.setOutputKeyClass(NullWritable.class);
      groupByJob.setOutputValueClass(NullWritable.class);
      groupByJob.setOutputFormatClass(SequenceFileOutputFormat.class);
      groupByJob.setNumReduceTasks(1);
      JobHelper.setupClasspath(config, groupByJob);

      config.addInputPaths(groupByJob);
      config.intoConfiguration(groupByJob);
      FileOutputFormat.setOutputPath(groupByJob, config.makeGroupedDataDir());

      groupByJob.submit();
      log.info("Job %s submitted, status available at: %s", groupByJob.getJobName(), groupByJob.getTrackingURL());

      if (!groupByJob.waitForCompletion(true)) {
        log.error("Job failed: %s", groupByJob.getJobID());
        return false;
      }

      /*
       * Load partitions and intervals determined by the previous job.
       */

      log.info("Job completed, loading up partitions for intervals[%s].", config.getSegmentGranularIntervals());
      FileSystem fileSystem = null;
      if (!config.getSegmentGranularIntervals().isPresent()) {
        final Path intervalInfoPath = config.makeIntervalInfoPath();
        fileSystem = intervalInfoPath.getFileSystem(groupByJob.getConfiguration());
        if (!fileSystem.exists(intervalInfoPath)) {
          throw new ISE("Path[%s] didn't exist!?", intervalInfoPath);
        }
        List<Interval> intervals = config.jsonMapper.readValue(
            Utils.openInputStream(groupByJob, intervalInfoPath), new TypeReference<List<Interval>>()
        {
        }
        );
        config.setGranularitySpec(new UniformGranularitySpec(config.getGranularitySpec().getGranularity(), intervals));
        log.info("Determined Intervals for Job [%s]" + config.getSegmentGranularIntervals());
      }
      Map<DateTime, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());
      int shardCount = 0;
      for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
        DateTime bucket = segmentGranularity.getStart();

        final Path partitionInfoPath = config.makeSegmentPartitionInfoPath(segmentGranularity);
        if (fileSystem == null) {
          fileSystem = partitionInfoPath.getFileSystem(groupByJob.getConfiguration());
        }
        if (fileSystem.exists(partitionInfoPath)) {
          Long cardinality = config.jsonMapper.readValue(
              Utils.openInputStream(groupByJob, partitionInfoPath), new TypeReference<Long>()
          {
          }
          );
          int numberOfShards = (int) Math.ceil((double) cardinality / config.getTargetPartitionSize());

          if (numberOfShards > MAX_SHARDS) {
            throw new ISE(
                "Number of shards [%d] exceed the maximum limit of [%d], either targetPartitionSize is too low or data volume is too high",
                numberOfShards,
                MAX_SHARDS
            );
          }

          List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithExpectedSize(numberOfShards);
          if (numberOfShards == 1) {
            actualSpecs.add(new HadoopyShardSpec(new NoneShardSpec(), shardCount++));
          } else {
            for (int i = 0; i < numberOfShards; ++i) {
              actualSpecs.add(new HadoopyShardSpec(new HashBasedNumberedShardSpec(i, numberOfShards), shardCount++));
              log.info("DateTime[%s], partition[%d], spec[%s]", bucket, i, actualSpecs.get(i));
            }
          }

          shardSpecs.put(bucket, actualSpecs);

        } else {
          log.info("Path[%s] didn't exist!?", partitionInfoPath);
        }
      }
      config.setShardSpecs(shardSpecs);
      log.info(
          "DetermineHashedPartitionsJob took %d millis",
          (System.currentTimeMillis() - startTime)
      );

      return true;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public static class DetermineCardinalityMapper extends HadoopDruidIndexerMapper<LongWritable, BytesWritable>
  {
    private static HashFunction hashFunction = Hashing.murmur3_128();
    private QueryGranularity rollupGranularity = null;
    private Map<Interval, HyperLogLog> hyperLogLogs;
    private HadoopDruidIndexerConfig config;
    private boolean determineIntervals;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      super.setup(context);
      rollupGranularity = getConfig().getRollupSpec().getRollupGranularity();
      config = HadoopDruidIndexerConfigBuilder.fromConfiguration(context.getConfiguration());
      Optional<Set<Interval>> intervals = config.getSegmentGranularIntervals();
      if (intervals.isPresent()) {
        determineIntervals = false;
        final ImmutableMap.Builder<Interval, HyperLogLog> builder = ImmutableMap.builder();
        for (final Interval bucketInterval : intervals.get()) {
          builder.put(bucketInterval, new HyperLogLog(HYPER_LOG_LOG_BIT_SIZE));
        }
        hyperLogLogs = builder.build();
      } else {
        determineIntervals = true;
        hyperLogLogs = Maps.newHashMap();
      }
    }

    @Override
    protected void innerMap(
        InputRow inputRow,
        Text text,
        Context context
    ) throws IOException, InterruptedException
    {

      final List<Object> groupKey = Rows.toGroupKey(
          rollupGranularity.truncate(inputRow.getTimestampFromEpoch()),
          inputRow
      );
      Interval interval;
      if (determineIntervals) {
        interval = config.getGranularitySpec().getGranularity().bucket(new DateTime(inputRow.getTimestampFromEpoch()));

        if (!hyperLogLogs.containsKey(interval)) {
          hyperLogLogs.put(interval, new HyperLogLog(HYPER_LOG_LOG_BIT_SIZE));
        }
      } else {
        final Optional<Interval> maybeInterval = config.getGranularitySpec()
                                                       .bucketInterval(new DateTime(inputRow.getTimestampFromEpoch()));

        if (!maybeInterval.isPresent()) {
          throw new ISE("WTF?! No bucket found for timestamp: %s", inputRow.getTimestampFromEpoch());
        }
        interval = maybeInterval.get();
      }
      hyperLogLogs.get(interval)
                  .offerHashed(
                      hashFunction.hashBytes(HadoopDruidIndexerConfig.jsonMapper.writeValueAsBytes(groupKey))
                                  .asLong()
                  );
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException
    {
      setup(context);

      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }

      for (Map.Entry<Interval, HyperLogLog> entry : hyperLogLogs.entrySet()) {
        context.write(
            new LongWritable(entry.getKey().getStartMillis()),
            new BytesWritable(entry.getValue().getBytes())
        );
      }
      cleanup(context);
    }

  }

  public static class DetermineCardinalityReducer
      extends Reducer<LongWritable, BytesWritable, NullWritable, NullWritable>
  {
    private final List<Interval> intervals = Lists.newArrayList();
    protected HadoopDruidIndexerConfig config = null;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      config = HadoopDruidIndexerConfigBuilder.fromConfiguration(context.getConfiguration());
    }

    @Override
    protected void reduce(
        LongWritable key,
        Iterable<BytesWritable> values,
        Context context
    ) throws IOException, InterruptedException
    {
      HyperLogLog aggregate = new HyperLogLog(HYPER_LOG_LOG_BIT_SIZE);
      for (BytesWritable value : values) {
        HyperLogLog logValue = HyperLogLog.Builder.build(value.getBytes());
        try {
          aggregate.addAll(logValue);
        }
        catch (CardinalityMergeException e) {
          e.printStackTrace(); // TODO: check for better handling
        }
      }
      Interval interval = config.getGranularitySpec().getGranularity().bucket(new DateTime(key.get()));
      intervals.add(interval);
      final Path outPath = config.makeSegmentPartitionInfoPath(interval);
      final OutputStream out = Utils.makePathAndOutputStream(
          context, outPath, config.isOverwriteFiles()
      );

      try {
        HadoopDruidIndexerConfig.jsonMapper.writerWithType(
            new TypeReference<Long>()
            {
            }
        ).writeValue(
            out,
            aggregate.cardinality()
        );
      }
      finally {
        Closeables.close(out, false);
      }
    }

    @Override
    public void run(Context context)
        throws IOException, InterruptedException
    {
      super.run(context);
      if (!config.getSegmentGranularIntervals().isPresent()) {
        final Path outPath = config.makeIntervalInfoPath();
        final OutputStream out = Utils.makePathAndOutputStream(
            context, outPath, config.isOverwriteFiles()
        );

        try {
          HadoopDruidIndexerConfig.jsonMapper.writerWithType(
              new TypeReference<List<Interval>>()
              {
              }
          ).writeValue(
              out,
              intervals
          );
        }
        finally {
          Closeables.close(out, false);
        }
      }
    }
  }
}



