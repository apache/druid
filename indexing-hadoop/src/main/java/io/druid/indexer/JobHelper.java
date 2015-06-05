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

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.metamx.common.IAE;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Progressable;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 */
public class JobHelper
{
  private static final Logger log = new Logger(JobHelper.class);

  private static final Set<Path> existing = Sets.newHashSet();

  private static final int NUM_RETRIES = 6;
  private static final int SECONDS_BETWEEN_RETRIES = 10;


  public static void setupClasspath(
      HadoopDruidIndexerConfig config,
      Job job
  )
      throws IOException
  {
    String classpathProperty = System.getProperty("druid.hadoop.internal.classpath");
    if (classpathProperty == null) {
      classpathProperty = System.getProperty("java.class.path");
    }

    String[] jarFiles = classpathProperty.split(File.pathSeparator);

    final Configuration conf = job.getConfiguration();
    final Path distributedClassPath = new Path(config.getWorkingPath(), "classpath");
    final FileSystem fs = distributedClassPath.getFileSystem(conf);

    if (fs instanceof LocalFileSystem) {
      return;
    }

    for (String jarFilePath : jarFiles) {
      File jarFile = new File(jarFilePath);
      if (jarFile.getName().endsWith(".jar")) {
        final Path hdfsPath = new Path(distributedClassPath, jarFile.getName());

        if (!existing.contains(hdfsPath)) {
          if (jarFile.getName().matches(".*SNAPSHOT(-selfcontained)?\\.jar$") || !fs.exists(hdfsPath)) {
            log.info("Uploading jar to path[%s]", hdfsPath);
            ByteStreams.copy(
                Files.newInputStreamSupplier(jarFile),
                new OutputSupplier<OutputStream>()
                {
                  @Override
                  public OutputStream getOutput() throws IOException
                  {
                    return fs.create(hdfsPath);
                  }
                }
            );
          }

          existing.add(hdfsPath);
        }

        DistributedCache.addFileToClassPath(hdfsPath, conf, fs);
      }
    }
  }

  public static void injectSystemProperties(Job job)
  {
    injectSystemProperties(job.getConfiguration());
  }

  public static Configuration injectSystemProperties(Configuration conf)
  {
    for (String propName : System.getProperties().stringPropertyNames()) {
      if (propName.startsWith("hadoop.")) {
        conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
      }
    }
    return conf;
  }

  public static void ensurePaths(HadoopDruidIndexerConfig config)
  {
    // config.addInputPaths() can have side-effects ( boo! :( ), so this stuff needs to be done before anything else
    try {
      Job job = Job.getInstance(
          new Configuration(),
          String.format("%s-determine_partitions-%s", config.getDataSource(), config.getIntervals())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.19");
      injectSystemProperties(job);

      config.addInputPaths(job);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static boolean runJobs(List<Jobby> jobs, HadoopDruidIndexerConfig config)
  {
    String failedMessage = null;
    for (Jobby job : jobs) {
      if (failedMessage == null) {
        if (!job.run()) {
          failedMessage = String.format("Job[%s] failed!", job.getClass());
        }
      }
    }

    if (!config.getSchema().getTuningConfig().isLeaveIntermediate()) {
      if (failedMessage == null || config.getSchema().getTuningConfig().isCleanupOnFailure()) {
        Path workingPath = config.makeIntermediatePath();
        log.info("Deleting path[%s]", workingPath);
        try {
          workingPath.getFileSystem(injectSystemProperties(new Configuration())).delete(workingPath, true);
        }
        catch (IOException e) {
          log.error(e, "Failed to cleanup path[%s]", workingPath);
        }
      }
    }

    if (failedMessage != null) {
      throw new ISE(failedMessage);
    }

    return true;
  }

  public static void setInputFormat(Job job, HadoopDruidIndexerConfig indexerConfig)
  {
    if (indexerConfig.getInputFormatClass() != null) {
      job.setInputFormatClass(indexerConfig.getInputFormatClass());
    } else if (indexerConfig.isCombineText()) {
      job.setInputFormatClass(CombineTextInputFormat.class);
    } else {
      job.setInputFormatClass(TextInputFormat.class);
    }
  }

  public static DataSegment serializeOutIndex(
      final DataSegment segmentTemplate,
      final Configuration configuration,
      final Progressable progressable,
      final TaskAttemptID taskAttemptID,
      final File mergedBase,
      final Path segmentBasePath
  )
      throws IOException
  {
    final FileSystem outputFS = FileSystem.get(segmentBasePath.toUri(), configuration);
    final FileContext fileContext = FileContext.getFileContext(segmentBasePath.toUri(), configuration);
    final Path tmpPath = new Path(segmentBasePath, String.format("index.zip.%d", taskAttemptID.getId()));
    final AtomicLong size = new AtomicLong(0L);
    final DataPusher zipPusher = (DataPusher) RetryProxy.create(
        DataPusher.class, new DataPusher()
        {
          @Override
          public void push() throws IOException
          {
            try (OutputStream outputStream = fileContext.create(
                tmpPath,
                EnumSet.of(CreateFlag.OVERWRITE, CreateFlag.CREATE),
                Options.CreateOpts.createParent(),
                Options.CreateOpts.bufferSize(256 * 1024)
            )) {
              size.set(zipAndCopyDir(mergedBase, outputStream, progressable));
              outputStream.flush();
            }
            catch (IOException | RuntimeException exception) {
              log.error(exception, "Exception in retry loop");
              throw exception;
            }
          }
        },
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS)
    );
    zipPusher.push();
    log.info("Zipped %,d bytes to [%s]", size.get(), tmpPath.toUri());

    final Path finalIndexZipFilePath = new Path(segmentBasePath, "index.zip");
    final URI indexOutURI = finalIndexZipFilePath.toUri();
    final ImmutableMap<String, Object> loadSpec;
    // TODO: Make this a part of Pushers or Pullers
    switch (outputFS.getScheme()) {
      case "hdfs":
        loadSpec = ImmutableMap.<String, Object>of(
            "type", "hdfs",
            "path", indexOutURI.toString()
        );
        break;
      case "s3":
      case "s3n":
        loadSpec = ImmutableMap.<String, Object>of(
            "type", "s3_zip",
            "bucket", indexOutURI.getHost(),
            "key", indexOutURI.getPath().substring(1) // remove the leading "/"
        );
        break;
      case "file":
        loadSpec = ImmutableMap.<String, Object>of(
            "type", "local",
            "path", indexOutURI.getPath()
        );
        break;
      default:
        throw new IAE("Unknown file system scheme [%s]", outputFS.getScheme());
    }
    final DataSegment finalSegment = segmentTemplate
        .withLoadSpec(loadSpec)
        .withSize(size.get())
        .withBinaryVersion(SegmentUtils.getVersionFromDir(mergedBase));
    fileContext.rename(tmpPath, finalIndexZipFilePath, Options.Rename.OVERWRITE);
    writeSegmentDescriptor(
        outputFS,
        finalSegment,
        new Path(segmentBasePath, "descriptor.json"),
        fileContext,
        progressable
    );
    return finalSegment;
  }

  public static void writeSegmentDescriptor(
      final FileSystem outputFS,
      final DataSegment segment,
      final Path descriptorPath,
      final FileContext fileContext,
      final Progressable progressable
  )
      throws IOException
  {
    final DataPusher descriptorPusher = (DataPusher) RetryProxy.create(
        DataPusher.class, new DataPusher()
        {
          @Override
          public void push() throws IOException
          {
            try {
              progressable.progress();
              if (outputFS.exists(descriptorPath)) {
                if (!fileContext.delete(descriptorPath, false)) {
                  throw new IOException(String.format("Failed to delete descriptor at [%s]", descriptorPath));
                }
              }
              try (final OutputStream descriptorOut = fileContext.create(
                  descriptorPath,
                  EnumSet.of(CreateFlag.OVERWRITE, CreateFlag.CREATE),
                  Options.CreateOpts.bufferSize(256 * 1024),
                  Options.CreateOpts.createParent()
              )) {
                HadoopDruidIndexerConfig.jsonMapper.writeValue(descriptorOut, segment);
                descriptorOut.flush();
              }
            }
            catch (RuntimeException | IOException ex) {
              log.info(ex, "Error in retry loop");
              throw ex;
            }
          }
        },
        RetryPolicies.retryUpToMaximumCountWithFixedSleep(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS)
    );
    descriptorPusher.push();
  }

  /**
   * Simple interface for retry operations
   */
  public interface DataPusher
  {
    void push() throws IOException;
  }

  public static long zipAndCopyDir(
      File baseDir,
      OutputStream baseOutputStream,
      Progressable progressable
  ) throws IOException
  {
    long size = 0L;
    try (ZipOutputStream outputStream = new ZipOutputStream(baseOutputStream)) {
      List<String> filesToCopy = Arrays.asList(baseDir.list());
      for (String fileName : filesToCopy) {
        final File fileToCopy = new File(baseDir, fileName);
        if (java.nio.file.Files.isRegularFile(fileToCopy.toPath())) {
          size += copyFileToZipStream(fileToCopy, outputStream, progressable);
        } else {
          log.warn("File at [%s] is not a regular file! skipping as part of zip", fileToCopy.getPath());
        }
      }
      outputStream.flush();
    }
    return size;
  }

  public static long copyFileToZipStream(
      File file,
      ZipOutputStream zipOutputStream,
      Progressable progressable
  ) throws IOException
  {
    createNewZipEntry(zipOutputStream, file);
    long numRead = 0;
    try (FileInputStream inputStream = new FileInputStream(file)) {
      byte[] buf = new byte[0x10000];
      for (int bytesRead = inputStream.read(buf); bytesRead >= 0; bytesRead = inputStream.read(buf)) {
        progressable.progress();
        if (bytesRead == 0) {
          continue;
        }
        zipOutputStream.write(buf, 0, bytesRead);
        progressable.progress();
        numRead += bytesRead;
      }
    }
    zipOutputStream.closeEntry();
    progressable.progress();
    return numRead;
  }

  private static void createNewZipEntry(ZipOutputStream out, File file) throws IOException
  {
    log.info("Creating new ZipEntry[%s]", file.getName());
    out.putNextEntry(new ZipEntry(file.getName()));
  }

  public static Path makeSegmentOutputPath(
      Path basePath,
      FileSystem fileSystem,
      String dataSource,
      String version,
      Interval interval,
      int partitionNum
  )
  {
    Path outputPath = new Path(prependFSIfNullScheme(fileSystem, basePath), "./" + dataSource);
    if ("hdfs".equals(fileSystem.getScheme())) {
      outputPath = new Path(
          outputPath, String.format(
          "./%s_%s",
          interval.getStart().toString(ISODateTimeFormat.basicDateTime()),
          interval.getEnd().toString(ISODateTimeFormat.basicDateTime())
      )
      );
      outputPath = new Path(outputPath, version.replace(":", "_"));
    } else {
      outputPath = new Path(
          outputPath, String.format(
          "./%s_%s",
          interval.getStart().toString(),
          interval.getEnd().toString()
      )
      );
      outputPath = new Path(outputPath, String.format("./%s", version));
    }
    outputPath = new Path(outputPath, Integer.toString(partitionNum));
    return outputPath;
  }

  public static Path prependFSIfNullScheme(FileSystem fs, Path path)
  {
    if (path.toUri().getScheme() == null) {
      path = new Path(fs.getUri().toString(), String.format("./%s", path));
    }
    return path;
  }
}
