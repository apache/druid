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

package io.druid.storage.azure;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.microsoft.azure.storage.StorageException;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public class AzureTaskLogs implements TaskLogs
{

  private static final Logger log = new Logger(AzureTaskLogs.class);

  private final AzureTaskLogsConfig config;
  private final AzureStorage azureStorage;

  @Inject
  public AzureTaskLogs(AzureTaskLogsConfig config, AzureStorage azureStorage)
  {
    this.config = config;
    this.azureStorage = azureStorage;
  }

  @Override
  public void pushTaskLog(final String taskid, final File logFile)
  {
    final String taskKey = getTaskLogKey(taskid);
    log.info("Pushing task log %s to: %s", logFile, taskKey);
    pushTaskFile(taskid, logFile, taskKey);
  }

  @Override
  public void pushTaskReports(String taskid, File reportFile) throws IOException
  {
    final String taskKey = getTaskReportsKey(taskid);
    log.info("Pushing task reports %s to: %s", reportFile, taskKey);
    pushTaskFile(taskid, reportFile, taskKey);
  }

  private void pushTaskFile(final String taskId, final File logFile, String taskKey)
  {
    try {
      AzureUtils.retryAzureOperation(
          () -> {
            azureStorage.uploadBlob(logFile, config.getContainer(), taskKey);
            return null;
          },
          config.getMaxTries()
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    return streamTaskFile(taskid, offset, getTaskLogKey(taskid));
  }

  @Override
  public Optional<ByteSource> streamTaskReports(String taskid) throws IOException
  {
    return streamTaskFile(taskid, 0, getTaskReportsKey(taskid));
  }

  private Optional<ByteSource> streamTaskFile(final String taskid, final long offset, String taskKey) throws IOException
  {
    final String container = config.getContainer();

    try {
      if (!azureStorage.getBlobExists(container, taskKey)) {
        return Optional.absent();
      }

      return Optional.<ByteSource>of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              try {
                final long start;
                final long length = azureStorage.getBlobLength(container, taskKey);

                if (offset > 0 && offset < length) {
                  start = offset;
                } else if (offset < 0 && (-1 * offset) < length) {
                  start = length + offset;
                } else {
                  start = 0;
                }

                InputStream stream = azureStorage.getBlobInputStream(container, taskKey);
                stream.skip(start);

                return stream;

              }
              catch (Exception e) {
                throw new IOException(e);
              }
            }
          }
      );
    }
    catch (StorageException | URISyntaxException e) {
      throw new IOE(e, "Failed to stream logs from: %s", taskKey);
    }
  }

  private String getTaskLogKey(String taskid)
  {
    return StringUtils.format("%s/%s/log", config.getPrefix(), taskid);
  }

  private String getTaskReportsKey(String taskid)
  {
    return StringUtils.format("%s/%s/report.json", config.getPrefix(), taskid);
  }

  @Override
  public void killAll()
  {
    throw new UnsupportedOperationException("not implemented");
  }

  @Override
  public void killOlderThan(long timestamp)
  {
    throw new UnsupportedOperationException("not implemented");
  }
}
