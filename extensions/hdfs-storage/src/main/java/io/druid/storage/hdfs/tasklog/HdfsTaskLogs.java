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
package io.druid.storage.hdfs.tasklog;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Indexer hdfs task logs, to support storing hdfs tasks to hdfs.
 */
public class HdfsTaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(HdfsTaskLogs.class);

  private final HdfsTaskLogsConfig config;

  @Inject
  public HdfsTaskLogs(HdfsTaskLogsConfig config)
  {
    this.config = config;
  }

  @Override
  public void pushTaskLog(String taskId, File logFile) throws IOException
  {
    final Path path = getTaskLogFileFromId(taskId);
    log.info("Writing task log to: %s", path);
    Configuration conf = new Configuration();
    final FileSystem fs = path.getFileSystem(conf);
    FileUtil.copy(logFile, fs, path, false, conf);
    log.info("Wrote task log to: %s", path);
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskId, final long offset) throws IOException
  {
    final Path path = getTaskLogFileFromId(taskId);
    final FileSystem fs = path.getFileSystem(new Configuration());
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
}


