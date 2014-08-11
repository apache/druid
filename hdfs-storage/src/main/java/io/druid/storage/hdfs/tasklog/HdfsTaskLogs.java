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
import com.google.common.io.ByteStreams;
import com.google.common.io.InputSupplier;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Indexer hdfs task logs, to support storing hdfs tasks to hdfs
 *
 * Created by Frank Ren on 6/20/14.
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
        log.info("writing task log to: %s", path);
        Configuration conf = new Configuration();
        final FileSystem fs = FileSystem.get(conf);
        FileUtil.copy(logFile, fs, path, false, conf);
        log.info("wrote task log to: %s", path);
    }

    @Override
    public Optional<InputSupplier<InputStream>> streamTaskLog(final String taskId, final long offset) throws IOException
    {
        final Path path = getTaskLogFileFromId(taskId);
        final FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(path)) {
            return Optional.<InputSupplier<InputStream>>of(
                new InputSupplier<InputStream>() {
                    @Override
                    public InputStream getInput() throws IOException
                    {
                        log.info("reading task log from: %s", path);
                        final InputStream inputStream = fs.open(path);
                        ByteStreams.skipFully(inputStream, offset);
                        log.info("read task log from: %s", path);
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


