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
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Set;

/**
 */
public class JobHelper
{
  private static final Logger log = new Logger(JobHelper.class);

  private static final Set<Path> existing = Sets.newHashSet();


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
    final Configuration conf = job.getConfiguration();
    for (String propName : System.getProperties().stringPropertyNames()) {
      if (propName.startsWith("hadoop.")) {
        conf.set(propName.substring("hadoop.".length()), System.getProperty(propName));
      }
    }
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
          workingPath.getFileSystem(new Configuration()).delete(workingPath, true);
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
    if(indexerConfig.getInputFormatClass() != null) {
      job.setInputFormatClass(indexerConfig.getInputFormatClass());
    } else if (indexerConfig.isCombineText()) {
      job.setInputFormatClass(CombineTextInputFormat.class);
    } else {
      job.setInputFormatClass(TextInputFormat.class);
    }
  }
}
