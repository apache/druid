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

import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import com.google.common.io.Closeables;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.guava.nary.BinaryFn;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.Parser;
import com.metamx.common.parsers.ParserUtils;
import com.metamx.druid.CombiningIterable;
import com.metamx.druid.shard.NoneShardSpec;
import com.metamx.druid.shard.ShardSpec;
import com.metamx.druid.shard.SingleDimensionShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;
import org.joda.time.Interval;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 */
public class DeterminePartitionsJob implements Jobby
{
  private static final Logger log = new Logger(DeterminePartitionsJob.class);

  private static final Joiner keyJoiner = Joiner.on(",");
  private static final Splitter keySplitter = Splitter.on(",");
  private static final Joiner tabJoiner = HadoopDruidIndexerConfig.tabJoiner;
  private static final Splitter tabSplitter = HadoopDruidIndexerConfig.tabSplitter;

  private final HadoopDruidIndexerConfig config;

  public DeterminePartitionsJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
  }

  public boolean run()
  {
    try {
      Job job = new Job(
          new Configuration(),
          String.format("%s-determine_partitions-%s", config.getDataSource(), config.getIntervals())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.19");
      for (String propName : System.getProperties().stringPropertyNames()) {
        Configuration conf = job.getConfiguration();
        if (propName.startsWith("hadoop.")) {
          conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
        }
      }

      job.setInputFormatClass(TextInputFormat.class);

      job.setMapperClass(DeterminePartitionsMapper.class);
      job.setMapOutputValueClass(Text.class);

      SortableBytes.useSortableBytesAsKey(job);

      job.setCombinerClass(DeterminePartitionsCombiner.class);
      job.setReducerClass(DeterminePartitionsReducer.class);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(DeterminePartitionsJob.DeterminePartitionsOutputFormat.class);
      FileOutputFormat.setOutputPath(job, config.makeIntermediatePath());

      config.addInputPaths(job);
      config.intoConfiguration(job);

      job.setJarByClass(DeterminePartitionsJob.class);

      job.submit();
      log.info("Job submitted, status available at %s", job.getTrackingURL());

      final boolean retVal = job.waitForCompletion(true);

      if (retVal) {
        log.info("Job completed, loading up partitions for intervals[%s].", config.getSegmentGranularIntervals());
        FileSystem fileSystem = null;
        Map<DateTime, List<HadoopyShardSpec>> shardSpecs = Maps.newTreeMap(DateTimeComparator.getInstance());
        int shardCount = 0;
        for (Interval segmentGranularity : config.getSegmentGranularIntervals()) {
          DateTime bucket = segmentGranularity.getStart();

          final Path partitionInfoPath = config.makeSegmentPartitionInfoPath(new Bucket(0, bucket, 0));
          if (fileSystem == null) {
            fileSystem = partitionInfoPath.getFileSystem(job.getConfiguration());
          }
          if (fileSystem.exists(partitionInfoPath)) {
            List<ShardSpec> specs = config.jsonMapper.readValue(
                Utils.openInputStream(job, partitionInfoPath), new TypeReference<List<ShardSpec>>()
            {
            }
            );

            List<HadoopyShardSpec> actualSpecs = Lists.newArrayListWithExpectedSize(specs.size());
            for (int i = 0; i < specs.size(); ++i) {
              actualSpecs.add(new HadoopyShardSpec(specs.get(i), shardCount++));
              log.info("DateTime[%s], partition[%d], spec[%s]", bucket, i, actualSpecs.get(i));
            }

            shardSpecs.put(bucket, actualSpecs);
          }
          else {
            log.info("Path[%s] didn't exist!?", partitionInfoPath);
          }
        }
        config.setShardSpecs(shardSpecs);
      }
      else {
        log.info("Job completed unsuccessfully.");
      }

      return retVal;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static class DeterminePartitionsMapper extends Mapper<LongWritable, Text, BytesWritable, Text>
  {
    private HadoopDruidIndexerConfig config;
    private String partitionDimension;
    private Parser parser;
    private Function<String, DateTime> timestampConverter;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());
      partitionDimension = config.getPartitionDimension();
      parser = config.getDataSpec().getParser();
      timestampConverter = ParserUtils.createTimestampParser(config.getTimestampFormat());
    }

    @Override
    protected void map(
        LongWritable key, Text value, Context context
    ) throws IOException, InterruptedException
    {
      Map<String, Object> values = parser.parse(value.toString());
      final DateTime timestamp;
      final String tsStr = (String) values.get(config.getTimestampColumnName());
      try {
        timestamp = timestampConverter.apply(tsStr);
      }
      catch(IllegalArgumentException e) {
        if(config.isIgnoreInvalidRows()) {
          context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER).increment(1);
          return; // we're ignoring this invalid row
        }
        else {
          throw e;
        }
      }

      final Optional<Interval> maybeInterval = config.getGranularitySpec().bucketInterval(timestamp);
      if(maybeInterval.isPresent()) {
        final DateTime bucket = maybeInterval.get().getStart();
        final String outKey = keyJoiner.join(bucket.toString(), partitionDimension);

        final Object dimValue = values.get(partitionDimension);
        if (! (dimValue instanceof String)) {
          throw new IAE("Cannot partition on a tag-style dimension[%s], line was[%s]", partitionDimension, value);
        }

        final byte[] groupKey = outKey.getBytes(Charsets.UTF_8);
        write(context, groupKey, "", 1);
        write(context, groupKey, (String) dimValue, 1);
      }
    }
  }

  private static abstract class DeterminePartitionsBaseReducer extends Reducer<BytesWritable, Text, BytesWritable, Text>
  {

    protected static volatile HadoopDruidIndexerConfig config = null;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      if (config == null) {
        synchronized (DeterminePartitionsBaseReducer.class) {
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

      final Iterable<Pair<String, Long>> combinedIterable = combineRows(values);
      innerReduce(context, keyBytes, combinedIterable);
    }

    protected abstract void innerReduce(
        Context context, SortableBytes keyBytes, Iterable<Pair<String, Long>> combinedIterable
    ) throws IOException, InterruptedException;

    private Iterable<Pair<String, Long>> combineRows(Iterable<Text> input)
    {
      return new CombiningIterable<Pair<String, Long>>(
          Iterables.transform(
              input,
              new Function<Text, Pair<String, Long>>()
              {
                @Override
                public Pair<String, Long> apply(Text input)
                {
                  Iterator<String> splits = tabSplitter.split(input.toString()).iterator();
                  return new Pair<String, Long>(splits.next(), Long.parseLong(splits.next()));
                }
              }
          ),
          new Comparator<Pair<String, Long>>()
          {
            @Override
            public int compare(Pair<String, Long> o1, Pair<String, Long> o2)
            {
              return o1.lhs.compareTo(o2.lhs);
            }
          },
          new BinaryFn<Pair<String, Long>, Pair<String, Long>, Pair<String, Long>>()
          {
            @Override
            public Pair<String, Long> apply(Pair<String, Long> arg1, Pair<String, Long> arg2)
            {
              if (arg2 == null) {
                return arg1;
              }

              return new Pair<String, Long>(arg1.lhs, arg1.rhs + arg2.rhs);
            }
          }
      );
    }
  }

  public static class DeterminePartitionsCombiner extends DeterminePartitionsBaseReducer
  {
    @Override
    protected void innerReduce(
        Context context, SortableBytes keyBytes, Iterable<Pair<String, Long>> combinedIterable
    ) throws IOException, InterruptedException
    {
      for (Pair<String, Long> pair : combinedIterable) {
        write(context, keyBytes.getGroupKey(), pair.lhs, pair.rhs);
      }
    }
  }

  public static class DeterminePartitionsReducer extends DeterminePartitionsBaseReducer
  {
    String previousBoundary;
    long runningTotal;

    @Override
    protected void innerReduce(
        Context context, SortableBytes keyBytes, Iterable<Pair<String, Long>> combinedIterable
    ) throws IOException, InterruptedException
    {
      PeekingIterator<Pair<String, Long>> iterator = Iterators.peekingIterator(combinedIterable.iterator());
      Pair<String, Long> totalPair = iterator.next();

      Preconditions.checkState(totalPair.lhs.equals(""), "Total pair value was[%s]!?", totalPair.lhs);
      long totalRows = totalPair.rhs;

      long numPartitions = Math.max(totalRows / config.getTargetPartitionSize(), 1);
      long expectedRowsPerPartition = totalRows / numPartitions;

      class PartitionsList extends ArrayList<ShardSpec>
      {
      }
      List<ShardSpec> partitions = new PartitionsList();

      runningTotal = 0;
      Pair<String, Long> prev = null;
      previousBoundary = null;
      while (iterator.hasNext()) {
        Pair<String, Long> curr = iterator.next();

        if (runningTotal > expectedRowsPerPartition) {
          Preconditions.checkNotNull(
              prev, "Prev[null] while runningTotal[%s] was > expectedRows[%s]!?", runningTotal, expectedRowsPerPartition
          );

          addPartition(partitions, curr.lhs);
        }

        runningTotal += curr.rhs;
        prev = curr;
      }

      if (partitions.isEmpty()) {
        partitions.add(new NoneShardSpec());
      } else if (((double) runningTotal / (double) expectedRowsPerPartition) < 0.25) {
        final SingleDimensionShardSpec lastSpec = (SingleDimensionShardSpec) partitions.remove(partitions.size() - 1);
        partitions.add(
            new SingleDimensionShardSpec(
                config.getPartitionDimension(),
                lastSpec.getStart(),
                null,
                lastSpec.getPartitionNum()
            )
        );
      } else {
        partitions.add(
            new SingleDimensionShardSpec(
                config.getPartitionDimension(),
                previousBoundary,
                null,
                partitions.size()
            )
        );
      }

      DateTime bucket = new DateTime(
          Iterables.get(keySplitter.split(new String(keyBytes.getGroupKey(), Charsets.UTF_8)), 0)
      );
      OutputStream out = Utils.makePathAndOutputStream(
          context, config.makeSegmentPartitionInfoPath(new Bucket(0, bucket, 0)), config.isOverwriteFiles()
      );

      for (ShardSpec partition : partitions) {
        log.info("%s", partition);
      }

      try {
        config.jsonMapper.writeValue(out, partitions);
      }
      finally {
        Closeables.close(out, false);
      }
    }

    private void addPartition(List<ShardSpec> partitions, String boundary)
    {
      partitions.add(
          new SingleDimensionShardSpec(
              config.getPartitionDimension(),
              previousBoundary,
              boundary,
              partitions.size()
          )
      );
      previousBoundary = boundary;
      runningTotal = 0;
    }
  }

  public static class DeterminePartitionsOutputFormat extends FileOutputFormat
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

  private static void write(
      TaskInputOutputContext<? extends Writable, ? extends Writable, BytesWritable, Text> context,
      final byte[] groupKey,
      String value,
      long numRows
  )
      throws IOException, InterruptedException
  {
    context.write(
        new SortableBytes(groupKey, value.getBytes(HadoopDruidIndexerConfig.javaNativeCharset)).toBytesWritable(),
        new Text(tabJoiner.join(value, numRows))
    );
  }
}
