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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.Files;
import io.druid.indexer.updater.HadoopDruidConverterConfig;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.ProgressIndicator;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.retry.RetryPolicies;
import org.apache.hadoop.io.retry.RetryProxy;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

/**
 */
public class JobHelper
{
  private static final Logger log = new Logger(JobHelper.class);
  private static final int NUM_RETRIES = 8;
  private static final int SECONDS_BETWEEN_RETRIES = 2;
  private static final int DEFAULT_FS_BUFFER_SIZE = 1 << 18; // 256KB
  private static final Pattern SNAPSHOT_JAR = Pattern.compile(".*\\-SNAPSHOT(-selfcontained)?\\.jar$");

  public static Path distributedClassPath(String path)
  {
    return distributedClassPath(new Path(path));
  }

  public static Path distributedClassPath(Path base)
  {
    return new Path(base, "classpath");
  }

  public static final String INDEX_ZIP = "index.zip";
  public static final String DESCRIPTOR_JSON = "descriptor.json";

  /**
   * Dose authenticate against a secured hadoop cluster
   * In case of any bug fix make sure to fix the code at HdfsStorageAuthentication#authenticate as well.
   *
   * @param config containing the principal name and keytab path.
   */
  public static void authenticate(HadoopDruidIndexerConfig config)
  {
    String principal = config.HADOOP_KERBEROS_CONFIG.getPrincipal();
    String keytab = config.HADOOP_KERBEROS_CONFIG.getKeytab();
    if (!Strings.isNullOrEmpty(principal) && !Strings.isNullOrEmpty(keytab)) {
      Configuration conf = new Configuration();
      UserGroupInformation.setConfiguration(conf);
      if (UserGroupInformation.isSecurityEnabled()) {
        try {
          if (UserGroupInformation.getCurrentUser().hasKerberosCredentials() == false
              || !UserGroupInformation.getCurrentUser().getUserName().equals(principal)) {
            log.info("trying to authenticate user [%s] with keytab [%s]", principal, keytab);
            UserGroupInformation.loginUserFromKeytab(principal, keytab);
          }
        }
        catch (IOException e) {
          throw new ISE(e, "Failed to authenticate user principal [%s] with keytab [%s]", principal, keytab);
        }
      }
    }
  }

  /**
   * Uploads jar files to hdfs and configures the classpath.
   * Snapshot jar files are uploaded to intermediateClasspath and not shared across multiple jobs.
   * Non-Snapshot jar files are uploaded to a distributedClasspath and shared across multiple jobs.
   *
   * @param distributedClassPath  classpath shared across multiple jobs
   * @param intermediateClassPath classpath exclusive for this job. used to upload SNAPSHOT jar files.
   * @param job                   job to run
   *
   * @throws IOException
   */
  public static void setupClasspath(
      final Path distributedClassPath,
      final Path intermediateClassPath,
      final Job job
  )
      throws IOException
  {
    String classpathProperty = System.getProperty("druid.hadoop.internal.classpath");
    if (classpathProperty == null) {
      classpathProperty = System.getProperty("java.class.path");
    }

    String[] jarFiles = classpathProperty.split(File.pathSeparator);

    final Configuration conf = job.getConfiguration();
    final FileSystem fs = distributedClassPath.getFileSystem(conf);

    if (fs instanceof LocalFileSystem) {
      return;
    }

    for (String jarFilePath : jarFiles) {

      final File jarFile = new File(jarFilePath);
      if (jarFile.getName().endsWith(".jar")) {
        try {
          RetryUtils.retry(
              () -> {
                if (isSnapshot(jarFile)) {
                  addSnapshotJarToClassPath(jarFile, intermediateClassPath, fs, job);
                } else {
                  addJarToClassPath(jarFile, distributedClassPath, intermediateClassPath, fs, job);
                }
                return true;
              },
              shouldRetryPredicate(),
              NUM_RETRIES
          );
        }
        catch (Exception e) {
          throw Throwables.propagate(e);
        }
      }
    }
  }

  public static final Predicate<Throwable> shouldRetryPredicate()
  {
    return new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable input)
      {
        if (input == null) {
          return false;
        }
        if (input instanceof IOException) {
          return true;
        }
        return apply(input.getCause());
      }
    };
  }

  static void addJarToClassPath(
      File jarFile,
      Path distributedClassPath,
      Path intermediateClassPath,
      FileSystem fs,
      Job job
  )
      throws IOException
  {
    // Create distributed directory if it does not exist.
    // rename will always fail if destination does not exist.
    fs.mkdirs(distributedClassPath);

    // Non-snapshot jar files are uploaded to the shared classpath.
    final Path hdfsPath = new Path(distributedClassPath, jarFile.getName());
    if (!fs.exists(hdfsPath)) {
      // Muliple jobs can try to upload the jar here,
      // to avoid them from overwriting files, first upload to intermediateClassPath and then rename to the distributedClasspath.
      final Path intermediateHdfsPath = new Path(intermediateClassPath, jarFile.getName());
      uploadJar(jarFile, intermediateHdfsPath, fs);
      IOException exception = null;
      try {
        log.info("Renaming jar to path[%s]", hdfsPath);
        fs.rename(intermediateHdfsPath, hdfsPath);
        if (!fs.exists(hdfsPath)) {
          throw new IOE("File does not exist even after moving from[%s] to [%s]", intermediateHdfsPath, hdfsPath);
        }
      }
      catch (IOException e) {
        // rename failed, possibly due to race condition. check if some other job has uploaded the jar file.
        try {
          if (!fs.exists(hdfsPath)) {
            log.error(e, "IOException while Renaming jar file");
            exception = e;
          }
        }
        catch (IOException e1) {
          e.addSuppressed(e1);
          exception = e;
        }
      }
      finally {
        try {
          if (fs.exists(intermediateHdfsPath)) {
            fs.delete(intermediateHdfsPath, false);
          }
        }
        catch (IOException e) {
          if (exception == null) {
            exception = e;
          } else {
            exception.addSuppressed(e);
          }
        }
        if (exception != null) {
          throw exception;
        }
      }
    }
    job.addFileToClassPath(hdfsPath);
  }

  static void addSnapshotJarToClassPath(
      File jarFile,
      Path intermediateClassPath,
      FileSystem fs,
      Job job
  ) throws IOException
  {
    // Snapshot jars are uploaded to non shared intermediate directory.
    final Path hdfsPath = new Path(intermediateClassPath, jarFile.getName());
    // Prevent uploading same file multiple times in same run.
    if (!fs.exists(hdfsPath)) {
      uploadJar(jarFile, hdfsPath, fs);
    }
    job.addFileToClassPath(hdfsPath);
  }

  static void uploadJar(File jarFile, final Path path, final FileSystem fs) throws IOException
  {
    log.info("Uploading jar to path[%s]", path);
    try (OutputStream os = fs.create(path)) {
      Files.asByteSource(jarFile).copyTo(os);
    }
  }

  static boolean isSnapshot(File jarFile)
  {
    return SNAPSHOT_JAR.matcher(jarFile.getName()).matches();
  }

  public static void injectSystemProperties(Job job)
  {
    injectSystemProperties(job.getConfiguration());
  }

  public static void injectDruidProperties(Configuration configuration, List<String> listOfAllowedPrefix)
  {
    String mapJavaOpts = Strings.nullToEmpty(configuration.get(MRJobConfig.MAP_JAVA_OPTS));
    String reduceJavaOpts = Strings.nullToEmpty(configuration.get(MRJobConfig.REDUCE_JAVA_OPTS));

    for (String propName : System.getProperties().stringPropertyNames()) {
      for (String prefix : listOfAllowedPrefix) {
        if (propName.equals(prefix) || propName.startsWith(prefix + ".")) {
          mapJavaOpts = StringUtils.format("%s -D%s=%s", mapJavaOpts, propName, System.getProperty(propName));
          reduceJavaOpts = StringUtils.format("%s -D%s=%s", reduceJavaOpts, propName, System.getProperty(propName));
          break;
        }
      }

    }
    if (!Strings.isNullOrEmpty(mapJavaOpts)) {
      configuration.set(MRJobConfig.MAP_JAVA_OPTS, mapJavaOpts);
    }
    if (!Strings.isNullOrEmpty(reduceJavaOpts)) {
      configuration.set(MRJobConfig.REDUCE_JAVA_OPTS, reduceJavaOpts);
    }
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
    authenticate(config);
    // config.addInputPaths() can have side-effects ( boo! :( ), so this stuff needs to be done before anything else
    try {
      Job job = Job.getInstance(
          new Configuration(),
          StringUtils.format("%s-determine_partitions-%s", config.getDataSource(), config.getIntervals())
      );

      job.getConfiguration().set("io.sort.record.percent", "0.19");
      injectSystemProperties(job);
      config.addJobProperties(job);

      config.addInputPaths(job);
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public static boolean runSingleJob(Jobby job, HadoopDruidIndexerConfig config)
  {
    boolean succeeded = job.run();

    if (!config.getSchema().getTuningConfig().isLeaveIntermediate()) {
      if (succeeded || config.getSchema().getTuningConfig().isCleanupOnFailure()) {
        Path workingPath = config.makeIntermediatePath();
        log.info("Deleting path[%s]", workingPath);
        try {
          Configuration conf = injectSystemProperties(new Configuration());
          config.addJobProperties(conf);
          workingPath.getFileSystem(conf).delete(workingPath, true);
        }
        catch (IOException e) {
          log.error(e, "Failed to cleanup path[%s]", workingPath);
        }
      }
    }

    return succeeded;
  }

  public static boolean runJobs(List<Jobby> jobs, HadoopDruidIndexerConfig config)
  {
    boolean succeeded = true;
    for (Jobby job : jobs) {
      if (!job.run()) {
        succeeded = false;
        break;
      }
    }

    if (!config.getSchema().getTuningConfig().isLeaveIntermediate()) {
      if (succeeded || config.getSchema().getTuningConfig().isCleanupOnFailure()) {
        Path workingPath = config.makeIntermediatePath();
        log.info("Deleting path[%s]", workingPath);
        try {
          Configuration conf = injectSystemProperties(new Configuration());
          config.addJobProperties(conf);
          workingPath.getFileSystem(conf).delete(workingPath, true);
        }
        catch (IOException e) {
          log.error(e, "Failed to cleanup path[%s]", workingPath);
        }
      }
    }

    return succeeded;
  }

  public static DataSegment serializeOutIndex(
      final DataSegment segmentTemplate,
      final Configuration configuration,
      final Progressable progressable,
      final File mergedBase,
      final Path finalIndexZipFilePath,
      final Path finalDescriptorPath,
      final Path tmpPath,
      DataSegmentPusher dataSegmentPusher
  )
      throws IOException
  {
    final FileSystem outputFS = FileSystem.get(finalIndexZipFilePath.toUri(), configuration);
    final AtomicLong size = new AtomicLong(0L);
    final DataPusher zipPusher = (DataPusher) RetryProxy.create(
        DataPusher.class, new DataPusher()
        {
          @Override
          public long push() throws IOException
          {
            try (OutputStream outputStream = outputFS.create(
                tmpPath,
                true,
                DEFAULT_FS_BUFFER_SIZE,
                progressable
            )) {
              size.set(zipAndCopyDir(mergedBase, outputStream, progressable));
            }
            catch (IOException | RuntimeException exception) {
              log.error(exception, "Exception in retry loop");
              throw exception;
            }
            return -1;
          }
        },
        RetryPolicies.exponentialBackoffRetry(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS)
    );
    zipPusher.push();
    log.info("Zipped %,d bytes to [%s]", size.get(), tmpPath.toUri());

    final URI indexOutURI = finalIndexZipFilePath.toUri();
    final DataSegment finalSegment = segmentTemplate
        .withLoadSpec(dataSegmentPusher.makeLoadSpec(indexOutURI))
        .withSize(size.get())
        .withBinaryVersion(SegmentUtils.getVersionFromDir(mergedBase));

    if (!renameIndexFiles(outputFS, tmpPath, finalIndexZipFilePath)) {
      throw new IOE(
          "Unable to rename [%s] to [%s]",
          tmpPath.toUri().toString(),
          finalIndexZipFilePath.toUri().toString()
      );
    }

    writeSegmentDescriptor(
        outputFS,
        finalSegment,
        finalDescriptorPath,
        progressable
    );
    return finalSegment;
  }

  public static void writeSegmentDescriptor(
      final FileSystem outputFS,
      final DataSegment segment,
      final Path descriptorPath,
      final Progressable progressable
  )
      throws IOException
  {
    final DataPusher descriptorPusher = (DataPusher) RetryProxy.create(
        DataPusher.class, new DataPusher()
        {
          @Override
          public long push() throws IOException
          {
            try {
              progressable.progress();
              if (outputFS.exists(descriptorPath)) {
                if (!outputFS.delete(descriptorPath, false)) {
                  throw new IOE("Failed to delete descriptor at [%s]", descriptorPath);
                }
              }
              try (final OutputStream descriptorOut = outputFS.create(
                  descriptorPath,
                  true,
                  DEFAULT_FS_BUFFER_SIZE,
                  progressable
              )) {
                HadoopDruidIndexerConfig.JSON_MAPPER.writeValue(descriptorOut, segment);
                descriptorOut.flush();
              }
            }
            catch (RuntimeException | IOException ex) {
              log.info(ex, "Exception in descriptor pusher retry loop");
              throw ex;
            }
            return -1;
          }
        },
        RetryPolicies.exponentialBackoffRetry(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS)
    );
    descriptorPusher.push();
  }

  /**
   * Simple interface for retry operations
   */
  public interface DataPusher
  {
    long push() throws IOException;
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

  public static Path makeFileNamePath(
      final Path basePath,
      final FileSystem fs,
      final DataSegment segmentTemplate,
      final String baseFileName,
      DataSegmentPusher dataSegmentPusher
  )
  {
    return new Path(
        prependFSIfNullScheme(fs, basePath),
        dataSegmentPusher.makeIndexPathName(segmentTemplate, baseFileName)
    );
  }

  public static Path makeTmpPath(
      final Path basePath,
      final FileSystem fs,
      final DataSegment segmentTemplate,
      final TaskAttemptID taskAttemptID,
      DataSegmentPusher dataSegmentPusher
  )
  {
    return new Path(
        prependFSIfNullScheme(fs, basePath),
        StringUtils.format(
            "./%s.%d",
            dataSegmentPusher.makeIndexPathName(segmentTemplate, JobHelper.INDEX_ZIP),
            taskAttemptID.getId()
        )
    );
  }

  /**
   * Rename the files. This works around some limitations of both FileContext (no s3n support) and NativeS3FileSystem.rename
   * which will not overwrite
   *
   * @param outputFS              The output fs
   * @param indexZipFilePath      The original file path
   * @param finalIndexZipFilePath The to rename the original file to
   *
   * @return False if a rename failed, true otherwise (rename success or no rename needed)
   */
  private static boolean renameIndexFiles(
      final FileSystem outputFS,
      final Path indexZipFilePath,
      final Path finalIndexZipFilePath
  )
  {
    try {
      return RetryUtils.retry(
          () -> {
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
                    DateTimes.utc(finalIndexZipFile.getModificationTime()),
                    finalIndexZipFile.getLen(),
                    zipFile.getPath(),
                    DateTimes.utc(zipFile.getModificationTime()),
                    zipFile.getLen()
                );
                outputFS.delete(finalIndexZipFilePath, false);
                needRename = true;
              } else {
                log.info(
                    "File[%s / %s / %sB] existed and will be kept",
                    finalIndexZipFile.getPath(),
                    DateTimes.utc(finalIndexZipFile.getModificationTime()),
                    finalIndexZipFile.getLen()
                );
                needRename = false;
              }
            } else {
              needRename = true;
            }

            if (needRename) {
              log.info("Attempting rename from [%s] to [%s]", indexZipFilePath, finalIndexZipFilePath);
              return outputFS.rename(indexZipFilePath, finalIndexZipFilePath);
            } else {
              return true;
            }
          },
          FileUtils.IS_EXCEPTION,
          NUM_RETRIES
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }


  public static Path prependFSIfNullScheme(FileSystem fs, Path path)
  {
    if (path.toUri().getScheme() == null) {
      path = fs.makeQualified(path);
    }
    return path;
  }

  // TODO: Replace this whenever hadoop gets their act together and stops breaking with more recent versions of Guava
  public static long unzipNoGuava(
      final Path zip,
      final Configuration configuration,
      final File outDir,
      final Progressable progressable
  ) throws IOException
  {
    final DataPusher zipPusher = (DataPusher) RetryProxy.create(
        DataPusher.class, new DataPusher()
        {
          @Override
          public long push() throws IOException
          {
            try {
              final FileSystem fileSystem = zip.getFileSystem(configuration);
              long size = 0L;
              final byte[] buffer = new byte[1 << 13];
              progressable.progress();
              try (ZipInputStream in = new ZipInputStream(fileSystem.open(zip, 1 << 13))) {
                for (ZipEntry entry = in.getNextEntry(); entry != null; entry = in.getNextEntry()) {
                  final String fileName = entry.getName();
                  try (final OutputStream out = new BufferedOutputStream(
                      new FileOutputStream(
                          outDir.getAbsolutePath()
                          + File.separator
                          + fileName
                      ), 1 << 13
                  )) {
                    for (int len = in.read(buffer); len >= 0; len = in.read(buffer)) {
                      progressable.progress();
                      if (len == 0) {
                        continue;
                      }
                      size += len;
                      out.write(buffer, 0, len);
                    }
                    out.flush();
                  }
                }
              }
              progressable.progress();
              return size;
            }
            catch (IOException | RuntimeException exception) {
              log.error(exception, "Exception in unzip retry loop");
              throw exception;
            }
          }
        },
        RetryPolicies.exponentialBackoffRetry(NUM_RETRIES, SECONDS_BETWEEN_RETRIES, TimeUnit.SECONDS)
    );
    return zipPusher.push();
  }

  public static URI getURIFromSegment(DataSegment dataSegment)
  {
    // There is no good way around this...
    // TODO: add getURI() to URIDataPuller
    final Map<String, Object> loadSpec = dataSegment.getLoadSpec();
    final String type = loadSpec.get("type").toString();
    final URI segmentLocURI;
    if ("s3_zip".equals(type)) {
      if ("s3a".equals(loadSpec.get("S3Schema"))) {
        segmentLocURI = URI.create(StringUtils.format("s3a://%s/%s", loadSpec.get("bucket"), loadSpec.get("key")));

      } else {
        segmentLocURI = URI.create(StringUtils.format("s3n://%s/%s", loadSpec.get("bucket"), loadSpec.get("key")));
      }
    } else if ("hdfs".equals(type)) {
      segmentLocURI = URI.create(loadSpec.get("path").toString());
    } else if ("google".equals(type)) {
      // Segment names contain : in their path.
      // Google Cloud Storage supports : but Hadoop does not.
      // This becomes an issue when re-indexing using the current segments.
      // The Hadoop getSplits code doesn't understand the : and returns "Relative path in absolute URI"
      // This could be fixed using the same code that generates path names for hdfs segments using
      // getHdfsStorageDir. But that wouldn't fix this issue for people who already have segments with ":".
      // Because of this we just URL encode the : making everything work as it should.
      segmentLocURI = URI.create(
          StringUtils.format("gs://%s/%s", loadSpec.get("bucket"), loadSpec.get("path").toString().replace(":", "%3A"))
      );
    } else if ("local".equals(type)) {
      try {
        segmentLocURI = new URI("file", null, loadSpec.get("path").toString(), null, null);
      }
      catch (URISyntaxException e) {
        throw new ISE(e, "Unable to form simple file uri");
      }
    } else {
      try {
        throw new IAE(
            "Cannot figure out loadSpec %s",
            HadoopDruidConverterConfig.jsonMapper.writeValueAsString(loadSpec)
        );
      }
      catch (JsonProcessingException e) {
        throw new ISE("Cannot write Map with json mapper");
      }
    }
    return segmentLocURI;
  }

  public static ProgressIndicator progressIndicatorForContext(
      final TaskAttemptContext context
  )
  {
    return new ProgressIndicator()
    {

      @Override
      public void progress()
      {
        context.progress();
      }

      @Override
      public void start()
      {
        context.progress();
        context.setStatus("STARTED");
      }

      @Override
      public void stop()
      {
        context.progress();
        context.setStatus("STOPPED");
      }

      @Override
      public void startSection(String section)
      {
        context.progress();
        context.setStatus(StringUtils.format("STARTED [%s]", section));
      }

      @Override
      public void stopSection(String section)
      {
        context.progress();
        context.setStatus(StringUtils.format("STOPPED [%s]", section));
      }
    };
  }

  public static boolean deleteWithRetry(final FileSystem fs, final Path path, final boolean recursive)
  {
    try {
      return RetryUtils.retry(
          () -> fs.delete(path, recursive),
          shouldRetryPredicate(),
          NUM_RETRIES
      );
    }
    catch (Exception e) {
      log.error(e, "Failed to cleanup path[%s]", path);
      throw Throwables.propagate(e);
    }
  }

  public static String getJobTrackerAddress(Configuration config)
  {
    String jobTrackerAddress = config.get("mapred.job.tracker");
    if (jobTrackerAddress == null) {
      // New Property name for Hadoop 3.0 and later versions
      jobTrackerAddress = config.get("mapreduce.jobtracker.address");
    }
    return jobTrackerAddress;
  }
}
