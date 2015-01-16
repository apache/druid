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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import com.google.common.primitives.Longs;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.logger.Logger;
import io.druid.data.input.InputRow;
import io.druid.data.input.impl.StringInputRowParser;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.QueryableIndex;
import io.druid.segment.SegmentUtils;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexSchema;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.InvalidJobConfException;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 */
public class IndexGeneratorJob implements Jobby
{
  private static final Logger log = new Logger(IndexGeneratorJob.class);
  private final HadoopDruidIndexerConfig config;
  private IndexGeneratorStats jobStats;

  public IndexGeneratorJob(
      HadoopDruidIndexerConfig config
  )
  {
    this.config = config;
    this.jobStats = new IndexGeneratorStats();
  }

  public static List<DataSegment> getPublishedSegments(HadoopDruidIndexerConfig config)
  {

    final Configuration conf = new Configuration();
    final ObjectMapper jsonMapper = HadoopDruidIndexerConfig.jsonMapper;

    ImmutableList.Builder<DataSegment> publishedSegmentsBuilder = ImmutableList.builder();

    for (String propName : System.getProperties().stringPropertyNames()) {
      if (propName.startsWith("hadoop.")) {
        conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
      }
    }

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

  public IndexGeneratorStats getJobStats()
  {
    return jobStats;
  }

  public boolean run()
  {
    try {
      Job job = new Job(
          new Configuration(),
          String.format("%s-index-generator-%s", config.getDataSource(), config.getIntervals())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.23");

      JobHelper.injectSystemProperties(job);

      if (config.isCombineText()) {
        job.setInputFormatClass(CombineTextInputFormat.class);
      } else {
        job.setInputFormatClass(TextInputFormat.class);
      }

      job.setMapperClass(IndexGeneratorMapper.class);
      job.setMapOutputValueClass(Text.class);

      SortableBytes.useSortableBytesAsMapOutputKey(job);

      job.setNumReduceTasks(Iterables.size(config.getAllBuckets().get()));
      job.setPartitionerClass(IndexGeneratorPartitioner.class);

      job.setReducerClass(IndexGeneratorReducer.class);
      job.setOutputKeyClass(BytesWritable.class);
      job.setOutputValueClass(Text.class);
      job.setOutputFormatClass(IndexGeneratorOutputFormat.class);
      FileOutputFormat.setOutputPath(job, config.makeIntermediatePath());

      config.addInputPaths(job);
      config.addJobProperties(job);
      config.intoConfiguration(job);

      JobHelper.setupClasspath(config, job);

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

  public static class IndexGeneratorMapper extends HadoopDruidIndexerMapper<BytesWritable, Text>

  {
    @Override
    protected void innerMap(
        InputRow inputRow,
        Text text,
        Context context
    ) throws IOException, InterruptedException
    {
      // Group by bucket, sort by timestamp
      final Optional<Bucket> bucket = getConfig().getBucket(inputRow);

      if (!bucket.isPresent()) {
        throw new ISE("WTF?! No bucket found for row: %s", inputRow);
      }

      context.write(
          new SortableBytes(
              bucket.get().toGroupKey(),
              Longs.toByteArray(inputRow.getTimestampFromEpoch())
          ).toBytesWritable(),
          text
      );
    }
  }

  public static class IndexGeneratorPartitioner extends Partitioner<BytesWritable, Text> implements Configurable
  {
    private Configuration config;

    @Override
    public int getPartition(BytesWritable bytesWritable, Text text, int numPartitions)
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

  public static class IndexGeneratorReducer extends Reducer<BytesWritable, Text, BytesWritable, Text>
  {
    private HadoopDruidIndexerConfig config;
    private List<String> metricNames = Lists.newArrayList();
    private StringInputRowParser parser;

    @Override
    protected void setup(Context context)
        throws IOException, InterruptedException
    {
      config = HadoopDruidIndexerConfig.fromConfiguration(context.getConfiguration());

      for (AggregatorFactory factory : config.getSpec().getDataSchema().getAggregators()) {
        metricNames.add(factory.getName().toLowerCase());
      }

      parser = config.getParser();
    }

    @Override
    protected void reduce(
        BytesWritable key, Iterable<Text> values, final Context context
    ) throws IOException, InterruptedException
    {
      SortableBytes keyBytes = SortableBytes.fromBytesWritable(key);
      Bucket bucket = Bucket.fromGroupKey(keyBytes.getGroupKey()).lhs;

      final Interval interval = config.getGranularitySpec().bucketInterval(bucket.time).get();
      //final DataRollupSpec rollupSpec = config.getRollupSpec();
      final AggregatorFactory[] aggs = config.getSpec().getDataSchema().getAggregators();

      IncrementalIndex index = makeIncrementalIndex(bucket, aggs);

      File baseFlushFile = File.createTempFile("base", "flush");
      baseFlushFile.delete();
      baseFlushFile.mkdirs();

      Set<File> toMerge = Sets.newTreeSet();
      int indexCount = 0;
      int lineCount = 0;
      int runningTotalLineCount = 0;
      long startTime = System.currentTimeMillis();

      Set<String> allDimensionNames = Sets.newHashSet();

      for (final Text value : values) {
        context.progress();
        final InputRow inputRow = index.getSpatialDimensionRowFormatter().formatRow(parser.parse(value.toString()));
        allDimensionNames.addAll(inputRow.getDimensions());

        int numRows = index.add(inputRow);
        ++lineCount;

        if (numRows >= config.getSpec().getTuningConfig().getRowFlushBoundary()) {
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
          IndexMerger.persist(
              index, interval, file, new IndexMerger.ProgressIndicator()
          {
            @Override
            public void progress()
            {
              context.progress();
            }
          }
          );
          index = makeIncrementalIndex(bucket, aggs);

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
        IndexMerger.persist(
            index, interval, mergedBase, new IndexMerger.ProgressIndicator()
        {
          @Override
          public void progress()
          {
            context.progress();
          }
        }
        );
      } else {
        if (!index.isEmpty()) {
          final File finalFile = new File(baseFlushFile, "final");
          IndexMerger.persist(
              index, interval, finalFile, new IndexMerger.ProgressIndicator()
          {
            @Override
            public void progress()
            {
              context.progress();
            }
          }
          );
          toMerge.add(finalFile);
        }

        for (File file : toMerge) {
          indexes.add(IndexIO.loadIndex(file));
        }
        mergedBase = IndexMerger.mergeQueryableIndex(
            indexes, aggs, new File(baseFlushFile, "merged"), new IndexMerger.ProgressIndicator()
        {
          @Override
          public void progress()
          {
            context.progress();
          }
        }
        );
      }

      serializeOutIndex(context, bucket, mergedBase, Lists.newArrayList(allDimensionNames));

      for (File file : toMerge) {
        FileUtils.deleteDirectory(file);
      }
    }

    private void serializeOutIndex(Context context, Bucket bucket, File mergedBase, List<String> dimensionNames)
        throws IOException
    {
      Interval interval = config.getGranularitySpec().bucketInterval(bucket.time).get();

      int attemptNumber = context.getTaskAttemptID().getId();

      FileSystem fileSystem = FileSystem.get(context.getConfiguration());
      Path indexBasePath = config.makeSegmentOutputPath(fileSystem, bucket);
      Path indexZipFilePath = new Path(indexBasePath, String.format("index.zip.%s", attemptNumber));
      final FileSystem infoFS = config.makeDescriptorInfoDir().getFileSystem(context.getConfiguration());
      final FileSystem outputFS = indexBasePath.getFileSystem(context.getConfiguration());

      outputFS.mkdirs(indexBasePath);

      Exception caughtException = null;
      ZipOutputStream out = null;
      long size = 0;
      try {
        out = new ZipOutputStream(new BufferedOutputStream(outputFS.create(indexZipFilePath), 256 * 1024));

        List<String> filesToCopy = Arrays.asList(mergedBase.list());

        for (String file : filesToCopy) {
          size += copyFile(context, out, mergedBase, file);
        }
      }
      catch (Exception e) {
        caughtException = e;
      }
      finally {
        if (caughtException == null) {
          Closeables.close(out, false);
        } else {
          CloseQuietly.close(out);
          throw Throwables.propagate(caughtException);
        }
      }

      Path finalIndexZipFilePath = new Path(indexBasePath, "index.zip");
      final URI indexOutURI = finalIndexZipFilePath.toUri();
      ImmutableMap<String, Object> loadSpec;
      if (outputFS instanceof NativeS3FileSystem) {
        loadSpec = ImmutableMap.<String, Object>of(
            "type", "s3_zip",
            "bucket", indexOutURI.getHost(),
            "key", indexOutURI.getPath().substring(1) // remove the leading "/"
        );
      } else if (outputFS instanceof LocalFileSystem) {
        loadSpec = ImmutableMap.<String, Object>of(
            "type", "local",
            "path", indexOutURI.getPath()
        );
      } else if (outputFS instanceof DistributedFileSystem) {
        loadSpec = ImmutableMap.<String, Object>of(
            "type", "hdfs",
            "path", indexOutURI.toString()
        );
      } else {
        throw new ISE("Unknown file system[%s]", outputFS.getClass());
      }

      DataSegment segment = new DataSegment(
          config.getDataSource(),
          interval,
          config.getSpec().getTuningConfig().getVersion(),
          loadSpec,
          dimensionNames,
          metricNames,
          config.getShardSpec(bucket).getActualSpec(),
          SegmentUtils.getVersionFromDir(mergedBase),
          size
      );

      // retry 1 minute
      boolean success = false;
      for (int i = 0; i < 6; i++) {
        if (renameIndexFiles(infoFS, outputFS, indexBasePath, indexZipFilePath, finalIndexZipFilePath, segment)) {
          log.info("Successfully renamed [%s] to [%s]", indexZipFilePath, finalIndexZipFilePath);
          success = true;
          break;
        } else {
          log.info("Failed to rename [%s] to [%s]", indexZipFilePath, finalIndexZipFilePath);
          try {
            Thread.sleep(10000);
            context.progress();
          }
          catch (InterruptedException e) {
            throw new ISE(
                "Thread error in retry loop for renaming [%s] to [%s]",
                indexZipFilePath.toUri().getPath(),
                finalIndexZipFilePath.toUri().getPath()
            );
          }
        }
      }

      if (!success) {
        if (!outputFS.exists(indexZipFilePath)) {
          throw new ISE("File [%s] does not exist after retry loop.", indexZipFilePath.toUri().getPath());
        }

        if (outputFS.getFileStatus(indexZipFilePath).getLen() == outputFS.getFileStatus(finalIndexZipFilePath)
                                                                         .getLen()) {
          outputFS.delete(indexZipFilePath, true);
        } else {
          outputFS.delete(finalIndexZipFilePath, true);
          if (!renameIndexFiles(infoFS, outputFS, indexBasePath, indexZipFilePath, finalIndexZipFilePath, segment)) {
            throw new ISE(
                "Files [%s] and [%s] are different, but still cannot rename after retry loop",
                indexZipFilePath.toUri().getPath(),
                finalIndexZipFilePath.toUri().getPath()
            );
          }
        }
      }
    }

    private boolean renameIndexFiles(
        FileSystem intermediateFS,
        FileSystem outputFS,
        Path indexBasePath,
        Path indexZipFilePath,
        Path finalIndexZipFilePath,
        DataSegment segment
    )
        throws IOException
    {
      final boolean needRename;

      if (outputFS.exists(finalIndexZipFilePath)) {
        // NativeS3FileSystem.rename won't overwrite, so we might need to delete the old index first
        final FileStatus zipFile = outputFS.getFileStatus(indexZipFilePath);
        final FileStatus finalIndexZipFile = outputFS.getFileStatus(finalIndexZipFilePath);

        if (zipFile.getModificationTime() >= finalIndexZipFile.getModificationTime()
            || zipFile.getLen() != finalIndexZipFile.getLen()) {
          log.info(
              "File[%s / %s / %sB] existed, but wasn't the same as [%s / %s / %sB]",
              finalIndexZipFile.getPath(),
              new DateTime(finalIndexZipFile.getModificationTime()),
              finalIndexZipFile.getLen(),
              zipFile.getPath(),
              new DateTime(zipFile.getModificationTime()),
              zipFile.getLen()
          );
          outputFS.delete(finalIndexZipFilePath, false);
          needRename = true;
        } else {
          log.info(
              "File[%s / %s / %sB] existed and will be kept",
              finalIndexZipFile.getPath(),
              new DateTime(finalIndexZipFile.getModificationTime()),
              finalIndexZipFile.getLen()
          );
          needRename = false;
        }
      } else {
        needRename = true;
      }

      if (needRename && !outputFS.rename(indexZipFilePath, finalIndexZipFilePath)) {
        return false;
      }

      writeSegmentDescriptor(outputFS, segment, new Path(indexBasePath, "descriptor.json"));
      final Path descriptorPath = config.makeDescriptorInfoPath(segment);
      log.info("Writing descriptor to path[%s]", descriptorPath);
      intermediateFS.mkdirs(descriptorPath.getParent());
      writeSegmentDescriptor(intermediateFS, segment, descriptorPath);

      return true;
    }

    private void writeSegmentDescriptor(FileSystem outputFS, DataSegment segment, Path descriptorPath)
        throws IOException
    {
      if (outputFS.exists(descriptorPath)) {
        outputFS.delete(descriptorPath, false);
      }

      final FSDataOutputStream descriptorOut = outputFS.create(descriptorPath);
      try {
        HadoopDruidIndexerConfig.jsonMapper.writeValue(descriptorOut, segment);
      }
      finally {
        descriptorOut.close();
      }
    }

    private long copyFile(
        Context context, ZipOutputStream out, File mergedBase, final String filename
    ) throws IOException
    {
      createNewZipEntry(out, filename);
      long numRead = 0;

      InputStream in = null;
      try {
        in = new FileInputStream(new File(mergedBase, filename));
        byte[] buf = new byte[0x10000];
        int read;
        while (true) {
          read = in.read(buf);
          if (read == -1) {
            break;
          }

          out.write(buf, 0, read);
          numRead += read;
          context.progress();
        }
      }
      finally {
        CloseQuietly.close(in);
      }
      out.closeEntry();
      context.progress();

      return numRead;
    }

    private IncrementalIndex makeIncrementalIndex(Bucket theBucket, AggregatorFactory[] aggs)
    {
      return new IncrementalIndex(
          new IncrementalIndexSchema.Builder()
              .withMinTimestamp(theBucket.time.getMillis())
              .withSpatialDimensions(config.getSpec().getDataSchema().getParser())
              .withQueryGranularity(config.getSpec().getDataSchema().getGranularitySpec().getQueryGranularity())
              .withMetrics(aggs)
              .build()
      );
    }

    private void createNewZipEntry(ZipOutputStream out, String name) throws IOException
    {
      log.info("Creating new ZipEntry[%s]", name);
      out.putNextEntry(new ZipEntry(name));
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
