/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.apache.druid.indexing.common.config.FileTaskLogsConfig;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class FileTaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(FileTaskLogs.class);

  private final FileTaskLogsConfig config;

  @Inject
  public FileTaskLogs(
      FileTaskLogsConfig config
  )
  {
    this.config = config;
  }

  @Override
  public void pushTaskLog(final String taskid, File file) throws IOException
  {
    if (config.getDirectory().exists() || config.getDirectory().mkdirs()) {
      final File outputFile = fileForTask(taskid, file.getName());
      Files.copy(file, outputFile);
      log.info("Wrote task log to: %s", outputFile);
    } else {
      throw new IOE("Unable to create task log dir[%s]", config.getDirectory());
    }
  }

  @Override
  public void pushTaskReports(String taskid, File reportFile) throws IOException
  {
    if (config.getDirectory().exists() || config.getDirectory().mkdirs()) {
      final File outputFile = fileForTask(taskid, reportFile.getName());
      Files.copy(reportFile, outputFile);
      log.info("Wrote task report to: %s", outputFile);
    } else {
      throw new IOE("Unable to create task report dir[%s]", config.getDirectory());
    }
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset)
  {
    final File file = fileForTask(taskid, "log");
    if (file.exists()) {
      return Optional.of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              return LogUtils.streamFile(file, offset);
            }
          }
      );
    } else {
      return Optional.absent();
    }
  }

  @Override
  public Optional<ByteSource> streamTaskReports(final String taskid)
  {
    final File file = fileForTask(taskid, "report.json");
    if (file.exists()) {
      return Optional.of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              return LogUtils.streamFile(file, 0);
            }
          }
      );
    } else {
      return Optional.absent();
    }
  }

  private File fileForTask(final String taskid, String filename)
  {
    return new File(config.getDirectory(), StringUtils.format("%s.%s", taskid, filename));
  }

  @Override
  public void killAll() throws IOException
  {
    log.info("Deleting all task logs from local dir [%s].", config.getDirectory().getAbsolutePath());
    FileUtils.deleteDirectory(config.getDirectory());
  }

  @Override
  public void killOlderThan(final long timestamp) throws IOException
  {
    File taskLogDir = config.getDirectory();
    if (taskLogDir.exists()) {

      if (!taskLogDir.isDirectory()) {
        throw new IOE("taskLogDir [%s] must be a directory.", taskLogDir);
      }

      File[] files = taskLogDir.listFiles(f -> f.lastModified() < timestamp);

      for (File file : files) {
        log.info("Deleting local task log [%s].", file.getAbsolutePath());
        org.apache.commons.io.FileUtils.forceDelete(file);

        if (Thread.currentThread().isInterrupted()) {
          throw new IOException(
              new InterruptedException("Thread interrupted. Couldn't delete all tasklogs.")
          );
        }
      }
    }
  }
}
