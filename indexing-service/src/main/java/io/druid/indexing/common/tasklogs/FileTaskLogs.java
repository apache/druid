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

package io.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.druid.indexing.common.config.FileTaskLogsConfig;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileFilter;
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
      final File outputFile = fileForTask(taskid);
      Files.copy(file, outputFile);
      log.info("Wrote task log to: %s", outputFile);
    } else {
      throw new IOE("Unable to create task log dir[%s]", config.getDirectory());
    }
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final File file = fileForTask(taskid);
    if (file.exists()) {
      return Optional.<ByteSource>of(
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

  private File fileForTask(final String taskid)
  {
    return new File(config.getDirectory(), StringUtils.format("%s.log", taskid));
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

      File[] files = taskLogDir.listFiles(
          new FileFilter()
          {
            @Override
            public boolean accept(File f)
            {
              return f.lastModified() < timestamp;
            }
          }
      );

      for (File file : files) {
        log.info("Deleting local task log [%s].", file.getAbsolutePath());
        FileUtils.forceDelete(file);

        if (Thread.currentThread().isInterrupted()) {
          throw new IOException(
              new InterruptedException("Thread interrupted. Couldn't delete all tasklogs.")
          );
        }
      }
    }
  }
}
