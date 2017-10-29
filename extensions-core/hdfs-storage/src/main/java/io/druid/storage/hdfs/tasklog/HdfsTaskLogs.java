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
package io.druid.storage.hdfs.tasklog;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Indexer hdfs task logs, to support storing hdfs tasks to hdfs.
 */
public class HdfsTaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(HdfsTaskLogs.class);

  private final HdfsTaskLogsConfig config;
  private final Configuration hadoopConfig;

  @Inject
  public HdfsTaskLogs(HdfsTaskLogsConfig config, Configuration hadoopConfig)
  {
    this.config = config;
    this.hadoopConfig = hadoopConfig;
  }

  @Override
  public void pushTaskLog(String taskId, File logFile) throws IOException
  {
    final Path path = getTaskLogFileFromId(taskId);
    log.info("Writing task log to: %s", path);
    final FileSystem fs = path.getFileSystem(hadoopConfig);
    try (
        final InputStream in = new FileInputStream(logFile);
        final OutputStream out = fs.create(path, true)
    ) {
      ByteStreams.copy(in, out);
    }

    log.info("Wrote task log to: %s", path);
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskId, final long offset) throws IOException
  {
    final Path path = getTaskLogFileFromId(taskId);
    final FileSystem fs = path.getFileSystem(hadoopConfig);
    if (fs.exists(path)) {
      return Optional.<ByteSource>of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              log.info("Reading task log from: %s", path);
              final long seekPos;
              if (offset < 0) {
                final FileStatus stat = fs.getFileStatus(path);
                seekPos = Math.max(0, stat.getLen() + offset);
              } else {
                seekPos = offset;
              }
              final FSDataInputStream inputStream = fs.open(path);
              inputStream.seek(seekPos);
              log.info("Read task log from: %s (seek = %,d)", path, seekPos);
              return inputStream;
            }
          }
      );
    } else {
      return Optional.absent();
    }
  }

  /**
   * Due to https://issues.apache.org/jira/browse/HDFS-13 ":" are not allowed in
   * path names. So we format paths differently for HDFS.
   */
  private Path getTaskLogFileFromId(String taskId)
  {
    return new Path(mergePaths(config.getDirectory(), taskId.replaceAll(":", "_")));
  }

  // some hadoop version Path.mergePaths does not exist
  private static String mergePaths(String path1, String path2)
  {
    return path1 + (path1.endsWith(Path.SEPARATOR) ? "" : Path.SEPARATOR) + path2;
  }

  @Override
  public void killAll() throws IOException
  {
    log.info("Deleting all task logs from hdfs dir [%s].", config.getDirectory());
    Path taskLogDir = new Path(config.getDirectory());
    FileSystem fs = taskLogDir.getFileSystem(hadoopConfig);
    fs.delete(taskLogDir, true);
  }

  @Override
  public void killOlderThan(long timestamp) throws IOException
  {
    Path taskLogDir = new Path(config.getDirectory());
    FileSystem fs = taskLogDir.getFileSystem(hadoopConfig);
    if (fs.exists(taskLogDir)) {

      if (!fs.isDirectory(taskLogDir)) {
        throw new IOE("taskLogDir [%s] must be a directory.", taskLogDir);
      }

      RemoteIterator<LocatedFileStatus> iter = fs.listLocatedStatus(taskLogDir);
      while (iter.hasNext()) {
        LocatedFileStatus file = iter.next();
        if (file.getModificationTime() < timestamp) {
          Path p = file.getPath();
          log.info("Deleting hdfs task log [%s].", p.toUri().toString());
          fs.delete(p, true);
        }

        if (Thread.currentThread().isInterrupted()) {
          throw new IOException(
              new InterruptedException("Thread interrupted. Couldn't delete all tasklogs.")
          );
        }
      }
    }
  }
}


