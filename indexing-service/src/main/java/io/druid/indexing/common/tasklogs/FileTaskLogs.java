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

package io.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.io.ByteSource;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.indexing.common.config.FileTaskLogsConfig;
import io.druid.tasklogs.TaskLogs;

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
    if (!config.getDirectory().exists()) {
      config.getDirectory().mkdir();
    }
    final File outputFile = fileForTask(taskid);
    Files.copy(file, outputFile);
    log.info("Wrote task log to: %s", outputFile);
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
    return new File(config.getDirectory(), String.format("%s.log", taskid));
  }
}
