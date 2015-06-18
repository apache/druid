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

package io.druid.indexer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.common.parsers.ParseException;
import io.druid.collections.StupidPool;
import io.druid.data.input.InputRow;
import io.druid.data.input.Rows;
import io.druid.data.input.impl.InputRowParser;
import io.druid.offheap.OffheapBufferPool;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMaker;
import io.druid.segment.LoggingProgressIndicator;
import io.druid.segment.ProgressIndicator;
import io.druid.segment.QueryableIndex;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.segment.incremental.OffheapIncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.joda.time.Interval;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

/**
 */
public class IndexGeneratorJob implements Jobby
{
  private static final Logger log = new Logger(IndexGeneratorJob.class);

  public static List<DataSegment> getPublishedSegments(HadoopDruidIndexerConfig config)
  {
    final Configuration conf = JobHelper.injectSystemProperties(new Configuration());
    final ObjectMapper jsonMapper = HadoopDruidIndexerConfig.jsonMapper;

    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();

    final Path descriptorInfoDir = config.makeDescriptorInfoDir();

    try {
      FileSystem fs = descriptorInfoDir.getFileSystem(conf);

      for (FileStatus status : fs.listStatus(descriptorInfoDir)) {
        final DataSegment segment = jsonMapper.readValue(fs.open(status.getPath()), DataSegment.class);
        publishedSegmentsBuilder.add(segment);
        log.info("Adding segment %s to the list of published segments", segment.getIdentifier());
      }
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
    List<DataSegment> publishedSegments = publishedSegmentsBuilder.build();

    return publishedSegments;
  }

  private final HadoopDruidIndexerConfig config;
  private IndexGeneratorStats jobStats;

  public IndexGeneratorJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
    this.jobStats = new IndexGeneratorStats();
  }

  protected void setReducerClass(final Job job)
  {
    job.setReducerClass(IndexGeneratorReducer.class);
  }

  public IndexGeneratorStats getJobStats()
  {
    return jobStats;
  }

  public boolean run()
  {
    try {
      Job job = Job.getInstance(
          new Configuration(),
          String.format("%s-index-generator-%s", config.getDataSource(), config.getIntervals())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.23");

      JobHelper.injectSystemProperties(job);

      JobHelper.setInputFormat(job, config);

      job.setMapperClass(IndexGeneratorMapper.class);
      job.setMapOutputValueClass(Text.class);

      SortableBytes.useSortableBytesAsMapOutputKey(job);

      int numReducers = Iterables.size(config.getAllBuckets().get());
      if (numReducers == 0) {
        throw new RuntimeException("No buckets?? seems there is no data to index.");
      }
      job.setNumReduceTasks(numReducers);
      job.setPartitionerClass(IndexGeneratorPartitioner.class);

      setReducerClass(job);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(IndexGeneratorOutputFormat.class);
      FileOutputFormat.setOutputPath(job, config.makeIntermediatePath());

      config.addInputPaths(job);
      config.addJobProperties(job);

      // hack to get druid.processing.bitmap property passed down to hadoop job.
      // once IndexIO doesn't rely on globally injected properties, we can move this into the HadoopTuningConfig.
      final String bitmapProperty = "druid.processing.bitmap.type";
      final String bitmapType = HadoopDruidIndexerConfig.properties.getProperty(bitmapProperty);
      if (bitmapType != null) {
        for (String property : new String[]{"mapreduce.reduce.java.opts", "mapreduce.map.java.opts"}) {
          // prepend property to allow overriding using hadoop.xxx properties by JobHelper.injectSystemProperties above
          String value = Strings.nullToEmpty(job.getConfiguration().get(property));
          job.getConfiguration().set(property, String.format("-D%s=%s %s", bitmapProperty, bitmapType, value));
        }
      }

      config.intoConfiguration(job);

      JobHelper.setupClasspath(JobHelper.distributedClassPath(config.getWorkingPath()), job);

      job.submit();
      log.info("Job %s submitted, status available at %s", job.getJobName(), job.getTrackingURL());

      boolean success = job.waitForCompletion(true);

      Counter invalidRowCount = job.getCounters()
                                   .findCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER);
      jobStats.setInvalidRowCount(invalidRowCount.getValue());

      return success;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static class IndexGeneratorMapper extends HadoopDruidIndexerMapper<BytesWritable, Writable>
  {
    private static final HashFunction hashFunction = Hashing.murmur3_128();

    @Override
    protected void innerMap(
        InputRow inputRow,
        Writable value,
        Context context
    ) throws IOException, InterruptedException
    {
      // Group by bucket, sort by timestamp
      final Optional<Bucket> bucket = getConfig().getBucket(inputRow);

      if (!bucket.isPresent()) {
        throw new ISE("WTF?! No bucket found for row: %s", inputRow);
      }

      final long truncatedTimestamp = granularitySpec.getQueryGranularity().truncate(inputRow.getTimestampFromEpoch());
      final byte[] hashedDimensions = hashFunction.hashBytes(
          HadoopDruidIndexerConfig.jsonMapper.writeValueAsBytes(
              Rows.toGroupKey(
                  truncatedTimestamp,
                  inputRow
              )
          )
      ).asBytes();

      context.write(
          new SortableBytes(
              bucket.get().toGroupKey(),
              // sort rows by truncated timestamp and hashed dimensions to help reduce spilling on the reducer side
              ByteBuffer.allocate(Longs.BYTES + hashedDimensions.length)
                        .putLong(truncatedTimestamp)
                        .put(hashedDimensions)
                        .array()
          ).toBytesWritable(),
          value
      );
    }
  }

  public static class IndexGeneratorPartitioner extends Partitioner<BytesWritable, Writable> implements Configurable
  {
    private Configuration config;

    @Override
    public int getPartition(BytesWritable bytesWritable, Writable value, int numPartitions)
    {
      final ByteBuffer bytes = ByteBuffer.wrap(bytesWritable.getBytes());
      bytes.position(4); // Skip length added by SortableBytes
      int shardNum = bytes.getInt();
      if (config.get("mapred.job.tracker").equals("local")) {
        return shardNum % numPartitions;
      } else {
        if (shardNum >= numPartitions) {
          throw new ISE("Not enough partitions, shard[%,d] >= numPartitions[%,d]", shardNum, numPartitions);
        }
        return shardNum;

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
    }
  }

  public static class IndexGeneratorReducer extends Reducer<BytesWritable, Writable, BytesWritable, Text>
  {
    protected HadoopDruidIndexerConfig config;
    private List<String> metricNames = Lists.newArrayList();
    private InputRowParser parser;

    protected ProgressIndicator makeProgressIndicator(final Context context)
    {
      return new LoggingProgressIndicator("IndexGeneratorJob")
      {
        @Override
        public void progress()
        {
          context.progress();
        }
      };
    }

    protected File persist(
        final IncrementalIndex index,
        final Interval interval,
        final File file,
        final ProgressIndicator progressIndicator
    ) throws IOException
    {
      return IndexMaker.persist(
          index, interval, file, config.getIndexSpec(), progressIndicator
      );
    }

    protected File mergeQueryableIndex(
        final List<QueryableIndex> indexes,
        final AggregatorFactory[] aggs,
        final File file,
        ProgressIndicator progressIndicator
    ) throws IOException
    {
      return IndexMaker.mergeQueryableIndex(
          indexes, aggs, file, config.getIndexSpec(), progressIndicator
      );
    }

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());

      for (AggregatorFactory factory : config.getSchema().getDataSchema().getAggregators()) {
        metricNames.add(factory.getName());
      }

      parser = config.getParser();
    }

    @Override
    protected void reduce(
        BytesWritable key, Iterable<Writable> values, final Context context
    ) throws IOException, InterruptedException
    {
      SortableBytes keyBytes = SortableBytes.fromBytesWritable(key);
      Bucket bucket = Bucket.fromGroupKey(keyBytes.getGroupKey()).lhs;

      final Interval interval = config.getGranularitySpec().bucketInterval(bucket.time).get();
      final AggregatorFactory[] aggs = config.getSchema().getDataSchema().getAggregators();
      final int maxTotalBufferSize = config.getSchema().getTuningConfig().getBufferSize();
      final int aggregationBufferSize = (int) ((double) maxTotalBufferSize
                                               * config.getSchema().getTuningConfig().getAggregationBufferRatio());

      final StupidPool<ByteBuffer> bufferPool = new OffheapBufferPool(aggregationBufferSize);
      IncrementalIndex index = makeIncrementalIndex(bucket, aggs, bufferPool);
      try {
        File baseFlushFile = File.createTempFile("base", "flush");
        baseFlushFile.delete();
        baseFlushFile.mkdirs();

        Set<File> toMerge = Sets.newTreeSet();
        int indexCount = 0;
        int lineCount = 0;
        int runningTotalLineCount = 0;
        long startTime = System.currentTimeMillis();

        Set<String> allDimensionNames = Sets.newHashSet();
        final ProgressIndicator progressIndicator = makeProgressIndicator(context);

        for (final Writable value : values) {
          context.progress();
          int numRows;
          try {
            final InputRow inputRow = index.formatRow(HadoopDruidIndexerMapper.parseInputRow(value, parser));
            allDimensionNames.addAll(inputRow.getDimensions());

            numRows = index.add(inputRow);
          }
          catch (ParseException e) {
            if (config.isIgnoreInvalidRows()) {
              log.debug(e, "Ignoring invalid row [%s] due to parsing error", value.toString());
              context.getCounter(HadoopDruidIndexerConfig.IndexJobCounters.INVALID_ROW_COUNTER).increment(1);
              continue;
            } else {
              throw e;
            }
          }
          ++lineCount;

          if (!index.canAppendRow()) {
            log.info(index.getOutOfRowsReason());
            log.info(
                "%,d lines to %,d rows in %,d millis",
                lineCount - runningTotalLineCount,
                numRows,
                System.currentTimeMillis() - startTime
            );
            runningTotalLineCount = lineCount;

            final File file = new File(baseFlushFile, String.format("index%,05d", indexCount));
            toMerge.add(file);

            context.progress();
            persist(index, interval, file, progressIndicator);
            // close this index and make a new one, reusing same buffer
            index.close();
            index = makeIncrementalIndex(bucket, aggs, bufferPool);

            startTime = System.currentTimeMillis();
            ++indexCount;
          }
        }

        log.info("%,d lines completed.", lineCount);

        List<QueryableIndex> indexes = Lists.newArrayListWithCapacity(indexCount);
        final File mergedBase;

        if (toMerge.size() == 0) {
          if (index.isEmpty()) {
            throw new IAE("If you try to persist empty indexes you are going to have a bad time");
          }

          mergedBase = new File(baseFlushFile, "merged");
          persist(index, interval, mergedBase, progressIndicator);
        } else {
          if (!index.isEmpty()) {
            final File finalFile = new File(baseFlushFile, "final");
            persist(index, interval, finalFile, progressIndicator);
            toMerge.add(finalFile);
          }

          for (File file : toMerge) {
            indexes.add(IndexIO.loadIndex(file));
          }
          mergedBase = mergeQueryableIndex(
              indexes, aggs, new File(baseFlushFile, "merged"), progressIndicator
          );
        }
        final FileSystem outputFS = new Path(config.getSchema().getIOConfig().getSegmentOutputPath())
            .getFileSystem(context.getConfiguration());
        final DataSegment segment = JobHelper.serializeOutIndex(
            new DataSegment(
                config.getDataSource(),
                interval,
                config.getSchema().getTuningConfig().getVersion(),
                null,
                ImmutableList.copyOf(allDimensionNames),
                metricNames,
                config.getShardSpec(bucket).getActualSpec(),
                -1,
                -1
            ),
            context.getConfiguration(),
            context,
            context.getTaskAttemptID(),
            mergedBase,
            JobHelper.makeSegmentOutputPath(
                new Path(config.getSchema().getIOConfig().getSegmentOutputPath()),
                outputFS,
                config.getSchema().getDataSchema().getDataSource(),
                config.getSchema().getTuningConfig().getVersion(),
                config.getSchema().getDataSchema().getGranularitySpec().bucketInterval(bucket.time).get(),
                bucket.partitionNum
            )
        );

        Path descriptorPath = config.makeDescriptorInfoPath(segment);
        descriptorPath = JobHelper.prependFSIfNullScheme(
            FileSystem.get(
                descriptorPath.toUri(),
                context.getConfiguration()
            ), descriptorPath
        );

        log.info("Writing descriptor to path[%s]", descriptorPath);
        JobHelper.writeSegmentDescriptor(
            config.makeDescriptorInfoDir().getFileSystem(context.getConfiguration()),
            segment,
            descriptorPath,
            context
        );
        for (File file : toMerge) {
          FileUtils.deleteDirectory(file);
        }
      }
      finally {
        index.close();
      }
    }

    private IncrementalIndex makeIncrementalIndex(Bucket theBucket, AggregatorFactory[] aggs, StupidPool bufferPool)
    {
      final HadoopTuningConfig tuningConfig = config.getSchema().getTuningConfig();
      final IncrementalIndexSchema indexSchema = new IncrementalIndexSchema.Builder()
          .withMinTimestamp(theBucket.time.getMillis())
          .withDimensionsSpec(config.getSchema().getDataSchema().getParser())
          .withQueryGranularity(config.getSchema().getDataSchema().getGranularitySpec().getQueryGranularity())
          .withMetrics(aggs)
          .build();
      if (tuningConfig.isIngestOffheap()) {

        return new OffheapIncrementalIndex(
            indexSchema,
            bufferPool,
            true,
            tuningConfig.getBufferSize()
        );
      } else {
        return new OnheapIncrementalIndex(
            indexSchema,
            tuningConfig.getRowFlushBoundary()
        );
      }
    }
  }

  public static class IndexGeneratorOutputFormat extends TextOutputFormat
  {
    @Override
    public void checkOutputSpecs(JobContext job) throws IOException
    {
      Path outDir = getOutputPath(job);
      if (outDir == null) {
        throw new InvalidJobConfException("Output directory not set.");
      }
    }
  }

  public static class IndexGeneratorStats
  {
    private long invalidRowCount = 0;

    public long getInvalidRowCount()
    {
      return invalidRowCount;
    }

    public void setInvalidRowCount(long invalidRowCount)
    {
      this.invalidRowCount = invalidRowCount;
    }
  }
}
