/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.Closeables;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.Rows;
import org.apache.druid.hll.HyperLogLogCollector;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.druid.timeline.partition.HashBasedNumberedShardSpec;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * Determines appropriate ShardSpecs for a job by determining approximate cardinality of data set using HyperLogLog
 */
public class DetermineHashedPartitionsJob implements Jobby
{
  private static final Logger log = new Logger(DetermineHashedPartitionsJob.class);
  private final HadoopDruidIndexerConfig config;
  private String failureCause;
  private Job groupByJob;
  private long startTime;

  public DetermineHashedPartitionsJob(
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
      startTime = System.currentTimeMillis();
      groupByJob = Job.getInstance(
          new Configuration(),
          StringUtils.format("%s-determine_partitions_hashed-%s", config.getDataSource(), config.getIntervals())
      );

      JobHelper.injectSystemProperties(groupByJob);
      config.addJobProperties(groupByJob);
      groupByJob.setMapperClass(DetermineCardinalityMapper.class);
      groupByJob.setMapOutputKeyClass(LongWritable.class);
      groupByJob.setMapOutputValueClass(BytesWritable.class);
      groupByJob.setReducerClass(DetermineCardinalityReducer.class);
      groupByJob.setOutputKeyClass(NullWritable.class);
      groupByJob.setOutputValueClass(NullWritable.class);
      groupByJob.setOutputFormatClass(SequenceFileOutputFormat.class);
      groupByJob.setPartitionerClass(DetermineHashedPartitionsPartitioner.class);
      if (!config.getSegmentGranularIntervals().isPresent()) {
        groupByJob.setNumReduceTasks(1);
      } else {
        groupByJob.setNumReduceTasks(config.getSegmentGranularIntervals().get().size());
      }
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

      // Store the jobId in the file
      if (groupByJob.getJobID() != null) {
        JobHelper.writeJobIdToFile(config.getHadoopJobIdFileName(), groupByJob.getJobID().toString());
      }

      if (!groupByJob.waitForCompletion(true)) {
        log.error("Job failed: %s", groupByJob.getJobID());
        failureCause = Utils.getFailureMessage(groupByJob, config.JSON_MAPPER);
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
        if (!Utils.exists(groupByJob, fileSystem, intervalInfoPath)) {
          throw new ISE("Path[%s] didn't exist!?", intervalInfoPath);
        }
        List<Interval> intervals = config.JSON_MAPPER.readValue(
            Utils.openInputStream(groupByJob, intervalInfoPath),
            new TypeReference<List<Interval>>() {}
        );
        config.setGranularitySpec(
            new UniformGranularitySpec(
                config.getGranularitySpec().getSegmentGranularity(),
                config.getGranularitySpec().getQueryGranularity(),
                config.getGranularitySpec().isRollup(),
                intervals
            )
        );
        log.info("Determined Intervals for Job [%s].", config.getSegmentGranularIntervals());
      }
      Map<Long, List<HadoopyShardSpec>> shardSpecs = new TreeMap<>(DateTimeComparator.getInstance());
      int shardCount = 0;
      for (Interval segmentGranularity : config.getSegmentGranularIntervals().get()) {
        DateTime bucket = segmentGranularity.getStart();

        final Path partitionInfoPath = config.makeSegmentPartitionInfoPath(segmentGranularity);
        if (fileSystem == null) {
          fileSystem = partitionInfoPath.getFileSystem(groupByJob.getConfiguration());
        }
        if (Utils.exists(groupByJob, fileSystem, partitionInfoPath)) {
          final Long numRows = config.JSON_MAPPER.readValue(
              Utils.openInputStream(groupByJob, partitionInfoPath),
              Long.class
          );

          log.info("Found approximately [%,d] rows in data.", numRows);

          final int numberOfShards = (int) Math.ceil((double) numRows / config.getTargetPartitionSize());

          log.info("Creating [%,d] shards", numberOfShards);

          List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithExpectedSize(numberOfShards);
          if (numberOfShards == 1) {
            actualSpecs.add(new HadoopyShardSpec(NoneShardSpec.instance(), shardCount++));
          } else {
            for (int i = 0; i < numberOfShards; ++i) {
              actualSpecs.add(
                  new HadoopyShardSpec(
                      new HashBasedNumberedShardSpec(
                          i,
                          numberOfShards,
                          null,
                          HadoopDruidIndexerConfig.JSON_MAPPER
                      ),
                      shardCount++
                  )
              );
              log.info("DateTime[%s], partition[%d], spec[%s]", bucket, i, actualSpecs.get(i));
            }
          }

          shardSpecs.put(bucket.getMillis(), actualSpecs);

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

  @Override
  public Map<String, Object> getStats()
  {
    if (groupByJob == null) {
      return null;
    }

    try {
      Counters jobCounters = groupByJob.getCounters();

      Map<String, Object> metrics = TaskMetricsUtils.makeIngestionRowMetrics(
          jobCounters.findCounter(HadoopDruidIndexerConfig.IndexJobCounters.ROWS_PROCESSED_COUNTER).getValue(),
          jobCounters.findCounter(HadoopDruidIndexerConfig.IndexJobCounters.ROWS_PROCESSED_WITH_ERRORS_COUNTER).getValue(),
          jobCounters.findCounter(HadoopDruidIndexerConfig.IndexJobCounters.ROWS_UNPARSEABLE_COUNTER).getValue(),
          jobCounters.findCounter(HadoopDruidIndexerConfig.IndexJobCounters.ROWS_THROWN_AWAY_COUNTER).getValue()
      );

      return metrics;
    }
    catch (IllegalStateException ise) {
      log.debug("Couldn't get counters due to job state");
      return null;
    }
    catch (Exception e) {
      log.debug(e, "Encountered exception in getStats().");
      return null;
    }
  }

  @Nullable
  @Override
  public String getErrorMessage()
  {
    return failureCause;
  }

  public static class DetermineCardinalityMapper extends HadoopDruidIndexerMapper<LongWritable, BytesWritable>
  {
    private static HashFunction hashFunction = Hashing.murmur3_128();
    private Granularity rollupGranularity = null;
    private Map<Interval, HyperLogLogCollector> hyperLogLogs;
    private HadoopDruidIndexerConfig config;
    private boolean determineIntervals;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      super.setup(context);
      rollupGranularity = getConfig().getGranularitySpec().getQueryGranularity();
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
      Optional<Set<Interval>> intervals = config.getSegmentGranularIntervals();
      if (intervals.isPresent()) {
        determineIntervals = false;
        final ImmutableMap.Builder<Interval, HyperLogLogCollector> builder = ImmutableMap.builder();
        for (final Interval bucketInterval : intervals.get()) {
          builder.put(bucketInterval, HyperLogLogCollector.makeLatestCollector());
        }
        hyperLogLogs = builder.build();
      } else {
        determineIntervals = true;
        hyperLogLogs = new HashMap<>();
      }
    }

    @Override
    protected void innerMap(
        InputRow inputRow,
        Context context
    ) throws IOException
    {

      final List<Object> groupKey = Rows.toGroupKey(
          rollupGranularity.bucketStart(inputRow.getTimestamp()).getMillis(),
          inputRow
      );
      Interval interval;
      if (determineIntervals) {
        interval = config.getGranularitySpec()
                         .getSegmentGranularity()
                         .bucket(DateTimes.utc(inputRow.getTimestampFromEpoch()));

        if (!hyperLogLogs.containsKey(interval)) {
          hyperLogLogs.put(interval, HyperLogLogCollector.makeLatestCollector());
        }
      } else {
        final Optional<Interval> maybeInterval = config.getGranularitySpec()
                                                       .bucketInterval(DateTimes.utc(inputRow.getTimestampFromEpoch()));

        if (!maybeInterval.isPresent()) {
          throw new ISE("WTF?! No bucket found for timestamp: %s", inputRow.getTimestampFromEpoch());
        }
        interval = maybeInterval.get();
      }

      hyperLogLogs
          .get(interval)
          .add(hashFunction.hashBytes(HadoopDruidIndexerConfig.JSON_MAPPER.writeValueAsBytes(groupKey)).asBytes());

      context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.ROWS_PROCESSED_COUNTER).increment(1);
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException
    {
      setup(context);

      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }

      for (Map.Entry<Interval, HyperLogLogCollector> entry : hyperLogLogs.entrySet()) {
        context.write(
            new LongWritable(entry.getKey().getStartMillis()),
            new BytesWritable(entry.getValue().toByteArray())
        );
      }
      cleanup(context);
    }

  }

  public static class DetermineCardinalityReducer
      extends Reducer<LongWritable, BytesWritable, NullWritable, NullWritable>
  {
    private final List<Interval> intervals = new ArrayList<>();
    protected HadoopDruidIndexerConfig config = null;
    private boolean determineIntervals;

    @Override
    protected void setup(Context context)
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
      determineIntervals = !config.getSegmentGranularIntervals().isPresent();
    }

    @Override
    protected void reduce(
        LongWritable key,
        Iterable<BytesWritable> values,
        Context context
    ) throws IOException
    {
      HyperLogLogCollector aggregate = HyperLogLogCollector.makeLatestCollector();
      for (BytesWritable value : values) {
        aggregate.fold(
            HyperLogLogCollector.makeCollector(ByteBuffer.wrap(value.getBytes(), 0, value.getLength()))
        );
      }

      Interval interval;

      if (determineIntervals) {
        interval = config.getGranularitySpec().getSegmentGranularity().bucket(DateTimes.utc(key.get()));
      } else {
        Optional<Interval> intervalOptional = config.getGranularitySpec().bucketInterval(DateTimes.utc(key.get()));

        if (!intervalOptional.isPresent()) {
          throw new ISE("WTF?! No bucket found for timestamp: %s", key.get());
        }
        interval = intervalOptional.get();
      }

      intervals.add(interval);
      final Path outPath = config.makeSegmentPartitionInfoPath(interval);
      final OutputStream out = Utils.makePathAndOutputStream(context, outPath, config.isOverwriteFiles());

      try {
        HadoopDruidIndexerConfig.JSON_MAPPER
            .writerWithType(Long.class)
            .writeValue(out, aggregate.estimateCardinalityRound());
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
      if (determineIntervals) {
        final Path outPath = config.makeIntervalInfoPath();
        final OutputStream out = Utils.makePathAndOutputStream(context, outPath, config.isOverwriteFiles());

        try {
          HadoopDruidIndexerConfig.JSON_MAPPER.writerWithType(
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

  public static class DetermineHashedPartitionsPartitioner
      extends Partitioner<LongWritable, BytesWritable> implements Configurable
  {
    private Configuration config;
    private boolean determineIntervals;
    private Map<LongWritable, Integer> reducerLookup;

    @Override
    public int getPartition(LongWritable interval, BytesWritable text, int numPartitions)
    {

      if ("local".equals(JobHelper.getJobTrackerAddress(config)) || determineIntervals) {
        return 0;
      } else {
        return reducerLookup.get(interval);
      }
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
      HadoopDruidIndexerConfig hadoopConfig = HadoopDruidIndexerConfig.fromConfiguration(config);
      if (hadoopConfig.getSegmentGranularIntervals().isPresent()) {
        determineIntervals = false;
        int reducerNumber = 0;
        ImmutableMap.Builder<LongWritable, Integer> builder = ImmutableMap.builder();
        for (Interval interval : hadoopConfig.getSegmentGranularIntervals().get()) {
          builder.put(new LongWritable(interval.getStartMillis()), reducerNumber++);
        }
        reducerLookup = builder.build();
      } else {
        determineIntervals = true;
      }
    }
  }

}
