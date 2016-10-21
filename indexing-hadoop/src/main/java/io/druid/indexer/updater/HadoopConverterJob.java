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

package io.druid.indexer.updater;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.druid.indexer.JobHelper;
import io.druid.indexer.hadoop.DatasourceInputSplit;
import io.druid.indexer.hadoop.WindowedDataSegment;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobPriority;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskReport;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.util.Progressable;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class HadoopConverterJob
{
  private static final Logger log = new Logger(HadoopConverterJob.class);
  private static final String COUNTER_GROUP = "Hadoop Druid Converter";
  private static final String COUNTER_LOADED = "Loaded Bytes";
  private static final String COUNTER_WRITTEN = "Written Bytes";

  private static void setJobName(JobConf jobConf, List<DataSegment> segments)
  {
    if (segments.size() == 1) {
      final DataSegment segment = segments.get(0);
      jobConf.setJobName(
          String.format(
              "druid-convert-%s-%s-%s",
              segment.getDataSource(),
              segment.getInterval(),
              segment.getVersion()
          )
      );
    } else {
      final Set<String> dataSources = Sets.newHashSet(
          Iterables.transform(
              segments,
              new Function<DataSegment, String>()
              {
                @Override
                public String apply(DataSegment input)
                {
                  return input.getDataSource();
                }
              }
          )
      );
      final Set<String> versions = Sets.newHashSet(
          Iterables.transform(
              segments,
              new Function<DataSegment, String>()
              {
                @Override
                public String apply(DataSegment input)
                {
                  return input.getVersion();
                }
              }
          )
      );
      jobConf.setJobName(
          String.format(
              "druid-convert-%s-%s",
              Arrays.toString(dataSources.toArray()),
              Arrays.toString(versions.toArray())
          )
      );
    }
  }

  public static Path getJobPath(JobID jobID, Path workingDirectory)
  {
    return new Path(workingDirectory, jobID.toString());
  }

  public static Path getTaskPath(JobID jobID, TaskAttemptID taskAttemptID, Path workingDirectory)
  {
    return new Path(getJobPath(jobID, workingDirectory), taskAttemptID.toString());
  }

  public static Path getJobClassPathDir(String jobName, Path workingDirectory) throws IOException
  {
    return new Path(workingDirectory, jobName.replace(":", ""));
  }

  public static void cleanup(Job job) throws IOException
  {
    final Path jobDir = getJobPath(job.getJobID(), job.getWorkingDirectory());
    final FileSystem fs = jobDir.getFileSystem(job.getConfiguration());
    RuntimeException e = null;
    try {
      JobHelper.deleteWithRetry(fs, jobDir, true);
    }
    catch (RuntimeException ex) {
      e = ex;
    }
    try {
      JobHelper.deleteWithRetry(fs, getJobClassPathDir(job.getJobName(), job.getWorkingDirectory()), true);
    }
    catch (RuntimeException ex) {
      if (e == null) {
        e = ex;
      } else {
        e.addSuppressed(ex);
      }
    }
    if (e != null) {
      throw e;
    }
  }


  public static HadoopDruidConverterConfig converterConfigFromConfiguration(Configuration configuration)
      throws IOException
  {
    final String property = Preconditions.checkNotNull(
        configuration.get(HadoopDruidConverterConfig.CONFIG_PROPERTY),
        HadoopDruidConverterConfig.CONFIG_PROPERTY
    );
    return HadoopDruidConverterConfig.fromString(property);
  }

  public static void converterConfigIntoConfiguration(
      HadoopDruidConverterConfig priorConfig,
      List<DataSegment> segments,
      Configuration configuration
  )
  {
    final HadoopDruidConverterConfig config = new HadoopDruidConverterConfig(
        priorConfig.getDataSource(),
        priorConfig.getInterval(),
        priorConfig.getIndexSpec(),
        segments,
        priorConfig.isValidate(),
        priorConfig.getDistributedSuccessCache(),
        priorConfig.getHadoopProperties(),
        priorConfig.getJobPriority(),
        priorConfig.getSegmentOutputPath()
    );
    try {
      configuration.set(
          HadoopDruidConverterConfig.CONFIG_PROPERTY,
          HadoopDruidConverterConfig.jsonMapper.writeValueAsString(config)
      );
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }
  }

  private final HadoopDruidConverterConfig converterConfig;
  private long loadedBytes = 0L;
  private long writtenBytes = 0L;

  public HadoopConverterJob(
      HadoopDruidConverterConfig converterConfig
  )
  {
    this.converterConfig = converterConfig;
  }

  public List<DataSegment> run() throws IOException
  {
    final JobConf jobConf = new JobConf();
    jobConf.setKeepFailedTaskFiles(false);
    for (Map.Entry<String, String> entry : converterConfig.getHadoopProperties().entrySet()) {
      jobConf.set(entry.getKey(), entry.getValue(), "converterConfig.getHadoopProperties()");
    }
    final List<DataSegment> segments = converterConfig.getSegments();
    if (segments.isEmpty()) {
      throw new IAE(
          "No segments found for datasource [%s]",
          converterConfig.getDataSource()
      );
    }
    converterConfigIntoConfiguration(converterConfig, segments, jobConf);

    jobConf.setNumReduceTasks(0);// Map only. Number of map tasks determined by input format
    jobConf.setWorkingDirectory(new Path(converterConfig.getDistributedSuccessCache()));

    setJobName(jobConf, segments);

    if (converterConfig.getJobPriority() != null) {
      jobConf.setJobPriority(JobPriority.valueOf(converterConfig.getJobPriority()));
    }

    final Job job = Job.getInstance(jobConf);

    job.setInputFormatClass(ConfigInputFormat.class);
    job.setMapperClass(ConvertingMapper.class);
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(Text.class);
    job.setMapSpeculativeExecution(false);
    job.setOutputFormatClass(ConvertingOutputFormat.class);

    JobHelper.setupClasspath(
        JobHelper.distributedClassPath(jobConf.getWorkingDirectory()),
        JobHelper.distributedClassPath(getJobClassPathDir(job.getJobName(), jobConf.getWorkingDirectory())),
        job
    );

    Throwable throwable = null;
    try {
      job.submit();
      log.info("Job %s submitted, status available at %s", job.getJobName(), job.getTrackingURL());
      final boolean success = job.waitForCompletion(true);
      if (!success) {
        final TaskReport[] reports = job.getTaskReports(TaskType.MAP);
        if (reports != null) {
          for (final TaskReport report : reports) {
            log.error("Error in task [%s] : %s", report.getTaskId(), Arrays.toString(report.getDiagnostics()));
          }
        }
        return null;
      }
      try {
        loadedBytes = job.getCounters().findCounter(COUNTER_GROUP, COUNTER_LOADED).getValue();
        writtenBytes = job.getCounters().findCounter(COUNTER_GROUP, COUNTER_WRITTEN).getValue();
      }
      catch (IOException ex) {
        log.error(ex, "Could not fetch counters");
      }
      final JobID jobID = job.getJobID();

      final Path jobDir = getJobPath(jobID, job.getWorkingDirectory());
      final FileSystem fs = jobDir.getFileSystem(job.getConfiguration());
      final RemoteIterator<LocatedFileStatus> it = fs.listFiles(jobDir, true);
      final List<Path> goodPaths = new ArrayList<>();
      while (it.hasNext()) {
        final LocatedFileStatus locatedFileStatus = it.next();
        if (locatedFileStatus.isFile()) {
          final Path myPath = locatedFileStatus.getPath();
          if (ConvertingOutputFormat.DATA_SUCCESS_KEY.equals(myPath.getName())) {
            goodPaths.add(new Path(myPath.getParent(), ConvertingOutputFormat.DATA_FILE_KEY));
          }
        }
      }
      if (goodPaths.isEmpty()) {
        log.warn("No good data found at [%s]", jobDir);
        return null;
      }
      final List<DataSegment> returnList = ImmutableList.copyOf(
          Lists.transform(
              goodPaths, new Function<Path, DataSegment>()
              {
                @Nullable
                @Override
                public DataSegment apply(final Path input)
                {
                  try {
                    if (!fs.exists(input)) {
                      throw new ISE(
                          "Somehow [%s] was found but [%s] is missing at [%s]",
                          ConvertingOutputFormat.DATA_SUCCESS_KEY,
                          ConvertingOutputFormat.DATA_FILE_KEY,
                          jobDir
                      );
                    }
                  }
                  catch (final IOException e) {
                    throw Throwables.propagate(e);
                  }
                  try (final InputStream stream = fs.open(input)) {
                    return HadoopDruidConverterConfig.jsonMapper.readValue(stream, DataSegment.class);
                  }
                  catch (final IOException e) {
                    throw Throwables.propagate(e);
                  }
                }
              }
          )
      );
      if (returnList.size() == segments.size()) {
        return returnList;
      } else {
        throw new ISE(
            "Tasks reported success but result length did not match! Expected %d found %d at path [%s]",
            segments.size(),
            returnList.size(),
            jobDir
        );
      }
    }
    catch (InterruptedException | ClassNotFoundException e) {
      RuntimeException exception =  Throwables.propagate(e);
      throwable = exception;
      throw exception;
    }
    catch (Throwable t) {
      throwable = t;
      throw t;
    }
    finally {
      try {
        cleanup(job);
      }
      catch (IOException e) {
        if (throwable != null) {
          throwable.addSuppressed(e);
        } else {
          log.error(e, "Could not clean up job [%s]", job.getJobID());
        }
      }
    }
  }

  public long getLoadedBytes()
  {
    return loadedBytes;
  }

  public long getWrittenBytes()
  {
    return writtenBytes;
  }

  public static class ConvertingOutputFormat extends OutputFormat<Text, Text>
  {
    protected static final String DATA_FILE_KEY = "result";
    protected static final String DATA_SUCCESS_KEY = "_SUCCESS";
    protected static final String PUBLISHED_SEGMENT_KEY = "io.druid.indexer.updater.converter.publishedSegment";
    private static final Logger log = new Logger(ConvertingOutputFormat.class);

    @Override
    public RecordWriter<Text, Text> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException
    {
      return new RecordWriter<Text, Text>()
      {
        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException
        {
          // NOOP
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException
        {
          // NOOP
        }
      };
    }

    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException
    {
      // NOOP
    }

    @Override
    public OutputCommitter getOutputCommitter(final TaskAttemptContext context)
        throws IOException, InterruptedException
    {
      return new OutputCommitter()
      {
        @Override
        public void setupJob(JobContext jobContext) throws IOException
        {
          // NOOP
        }

        @Override
        public void setupTask(TaskAttemptContext taskContext) throws IOException
        {
          // NOOP
        }

        @Override
        public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException
        {
          return taskContext.getConfiguration().get(PUBLISHED_SEGMENT_KEY) != null;
        }

        @Override
        public void commitTask(final TaskAttemptContext taskContext) throws IOException
        {
          final Progressable commitProgressable = new Progressable()
          {
            @Override
            public void progress()
            {
              taskContext.progress();
            }
          };
          final String finalSegmentString = taskContext.getConfiguration().get(PUBLISHED_SEGMENT_KEY);
          if (finalSegmentString == null) {
            throw new IOException("Could not read final segment");
          }
          final DataSegment newSegment = HadoopDruidConverterConfig.jsonMapper.readValue(
              finalSegmentString,
              DataSegment.class
          );
          log.info("Committing new segment [%s]", newSegment);
          taskContext.progress();

          final FileSystem fs = taskContext.getWorkingDirectory().getFileSystem(taskContext.getConfiguration());
          final Path taskAttemptDir = getTaskPath(
              context.getJobID(),
              context.getTaskAttemptID(),
              taskContext.getWorkingDirectory()
          );
          final Path taskAttemptFile = new Path(taskAttemptDir, DATA_FILE_KEY);
          final Path taskAttemptSuccess = new Path(taskAttemptDir, DATA_SUCCESS_KEY);
          try (final OutputStream outputStream = fs.create(taskAttemptFile, false, 1 << 10, commitProgressable)) {
            outputStream.write(HadoopDruidConverterConfig.jsonMapper.writeValueAsBytes(newSegment));
          }

          fs.create(taskAttemptSuccess, false).close();

          taskContext.progress();
          taskContext.setStatus("Committed");
        }

        @Override
        public void abortTask(TaskAttemptContext taskContext) throws IOException
        {
          log.warn("Aborting task. Nothing to clean up.");
        }
      };
    }
  }


  public static class ConvertingMapper extends Mapper<String, String, Text, Text>
  {
    private static final Logger log = new Logger(ConvertingMapper.class);
    private static final String TMP_FILE_LOC_KEY = "io.druid.indexer.updater.converter.reducer.tmpDir";

    @Override
    protected void map(
        String key, String value,
        final Context context
    ) throws IOException, InterruptedException
    {
      final InputSplit split = context.getInputSplit();
      if (!(split instanceof DatasourceInputSplit)) {
        throw new IAE(
            "Unexpected split type. Expected [%s] was [%s]",
            DatasourceInputSplit.class.getCanonicalName(),
            split.getClass().getCanonicalName()
        );
      }

      final String tmpDirLoc = context.getConfiguration().get(TMP_FILE_LOC_KEY);
      final File tmpDir = Paths.get(tmpDirLoc).toFile();

      final DataSegment segment  = Iterables.getOnlyElement(((DatasourceInputSplit) split).getSegments()).getSegment();

      final HadoopDruidConverterConfig config = converterConfigFromConfiguration(context.getConfiguration());

      context.setStatus("DOWNLOADING");
      context.progress();
      final Path inPath = new Path(JobHelper.getURIFromSegment(segment));
      final File inDir = new File(tmpDir, "in");

      if (inDir.exists() && !inDir.delete()) {
        log.warn("Could not delete [%s]", inDir);
      }

      if (!inDir.mkdir() && (!inDir.exists() || inDir.isDirectory())) {
        log.warn("Unable to make directory");
      }

      final long inSize = JobHelper.unzipNoGuava(inPath, context.getConfiguration(), inDir, context);
      log.debug("Loaded %d bytes into [%s] for converting", inSize, inDir.getAbsolutePath());
      context.getCounter(COUNTER_GROUP, COUNTER_LOADED).increment(inSize);

      context.setStatus("CONVERTING");
      context.progress();
      final File outDir = new File(tmpDir, "out");
      if (!outDir.mkdir() && (!outDir.exists() || !outDir.isDirectory())) {
        throw new IOException(String.format("Could not create output directory [%s]", outDir));
      }
      HadoopDruidConverterConfig.INDEX_MERGER.convert(
          inDir,
          outDir,
          config.getIndexSpec(),
          JobHelper.progressIndicatorForContext(context)
      );
      if (config.isValidate()) {
        context.setStatus("Validating");
        HadoopDruidConverterConfig.INDEX_IO.validateTwoSegments(inDir, outDir);
      }
      context.progress();
      context.setStatus("Starting PUSH");
      final Path baseOutputPath = new Path(config.getSegmentOutputPath());
      final FileSystem outputFS = baseOutputPath.getFileSystem(context.getConfiguration());
      final DataSegment finalSegmentTemplate = segment.withVersion(
          segment.getVersion()
          + "_converted"
      );
      final DataSegment finalSegment = JobHelper.serializeOutIndex(
          finalSegmentTemplate,
          context.getConfiguration(),
          context,
          context.getTaskAttemptID(),
          outDir,
          JobHelper.makeSegmentOutputPath(
              baseOutputPath,
              outputFS,
              finalSegmentTemplate
          )
      );
      context.progress();
      context.setStatus("Finished PUSH");
      final String finalSegmentString = HadoopDruidConverterConfig.jsonMapper.writeValueAsString(finalSegment);
      context.getConfiguration().set(ConvertingOutputFormat.PUBLISHED_SEGMENT_KEY, finalSegmentString);
      context.write(new Text("dataSegment"), new Text(finalSegmentString));

      context.getCounter(COUNTER_GROUP, COUNTER_WRITTEN).increment(finalSegment.getSize());
      context.progress();
      context.setStatus("Ready To Commit");
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException
    {
      final File tmpFile = Files.createTempDir();
      context.getConfiguration().set(TMP_FILE_LOC_KEY, tmpFile.getAbsolutePath());
    }

    @Override
    protected void cleanup(
        Context context
    ) throws IOException, InterruptedException
    {
      final String tmpDirLoc = context.getConfiguration().get(TMP_FILE_LOC_KEY);
      final File tmpDir = Paths.get(tmpDirLoc).toFile();
      FileUtils.deleteDirectory(tmpDir);
      context.progress();
      context.setStatus("Clean");
    }
  }

  public static class ConfigInputFormat extends InputFormat<String, String>
  {
    @Override
    public List<InputSplit> getSplits(final JobContext jobContext) throws IOException, InterruptedException
    {
      final HadoopDruidConverterConfig config = converterConfigFromConfiguration(jobContext.getConfiguration());
      final List<DataSegment> segments = config.getSegments();
      if (segments == null) {
        throw new IOException("Bad config, missing segments");
      }
      return Lists.transform(
          segments, new Function<DataSegment, InputSplit>()
          {
            @Nullable
            @Override
            public InputSplit apply(DataSegment input)
            {
              return new DatasourceInputSplit(ImmutableList.of(WindowedDataSegment.of(input)), null);
            }
          }
      );
    }

    @Override
    public RecordReader<String, String> createRecordReader(
        final InputSplit inputSplit, final TaskAttemptContext taskAttemptContext
    ) throws IOException, InterruptedException
    {
      return new RecordReader<String, String>()
      {
        boolean readAnything = false;

        @Override
        public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
            throws IOException, InterruptedException
        {
          // NOOP
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException
        {
          return !readAnything;
        }

        @Override
        public String getCurrentKey() throws IOException, InterruptedException
        {
          return "key";
        }

        @Override
        public String getCurrentValue() throws IOException, InterruptedException
        {
          readAnything = true;
          return "fakeValue";
        }

        @Override
        public float getProgress() throws IOException, InterruptedException
        {
          return readAnything ? 0.0F : 1.0F;
        }

        @Override
        public void close() throws IOException
        {
          // NOOP
        }
      };
    }
  }
}
