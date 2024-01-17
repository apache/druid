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

package org.apache.druid.storage.google;

import com.google.api.client.http.InputStreamContent;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.common.utils.CurrentTimeMillisSupplier;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Date;

public class GoogleTaskLogs implements TaskLogs
{
  private static final Logger LOG = new Logger(GoogleTaskLogs.class);

  private final GoogleTaskLogsConfig config;
  private final GoogleStorage storage;
  private final GoogleInputDataConfig inputDataConfig;
  private final CurrentTimeMillisSupplier timeSupplier;

  @Inject
  public GoogleTaskLogs(
      GoogleTaskLogsConfig config,
      GoogleStorage storage,
      GoogleInputDataConfig inputDataConfig,
      CurrentTimeMillisSupplier timeSupplier
  )
  {
    this.config = config;
    this.storage = storage;
    this.inputDataConfig = inputDataConfig;
    this.timeSupplier = timeSupplier;
  }

  @Override
  public void pushTaskLog(final String taskid, final File logFile) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);
    LOG.info("Pushing task log %s to: %s", logFile, taskKey);
    pushTaskFile(logFile, taskKey);
  }

  @Override
  public void pushTaskReports(String taskid, File reportFile) throws IOException
  {
    final String taskKey = getTaskReportKey(taskid);
    LOG.info("Pushing task reports %s to: %s", reportFile, taskKey);
    pushTaskFile(reportFile, taskKey);
  }

  @Override
  public void pushTaskStatus(String taskid, File statusFile) throws IOException
  {
    final String taskKey = getTaskStatusKey(taskid);
    LOG.info("Pushing task status %s to: %s", statusFile, taskKey);
    pushTaskFile(statusFile, taskKey);
  }

  private void pushTaskFile(final File logFile, final String taskKey) throws IOException
  {
    try (final InputStream fileStream = Files.newInputStream(logFile.toPath())) {

      InputStreamContent mediaContent = new InputStreamContent("text/plain", fileStream);
      mediaContent.setLength(logFile.length());

      try {
        RetryUtils.retry(
            (RetryUtils.Task<Void>) () -> {
              storage.insert(config.getBucket(), taskKey, mediaContent);
              return null;
            },
            GoogleUtils::isRetryable,
            1,
            5
        );
      }
      catch (IOException e) {
        throw e;
      }
      catch (Exception e) {
        throw new RE(e, "Failed to upload [%s] to [%s]", logFile, taskKey);
      }
    }
  }

  @Override
  public Optional<InputStream> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);
    return streamTaskFile(taskid, offset, taskKey);
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskid) throws IOException
  {
    final String taskKey = getTaskReportKey(taskid);
    return streamTaskFile(taskid, 0, taskKey);
  }

  @Override
  public Optional<InputStream> streamTaskStatus(String taskid) throws IOException
  {
    final String taskKey = getTaskStatusKey(taskid);
    return streamTaskFile(taskid, 0, taskKey);
  }

  private Optional<InputStream> streamTaskFile(final String taskid, final long offset, String taskKey)
      throws IOException
  {
    try {
      if (!storage.exists(config.getBucket(), taskKey)) {
        return Optional.absent();
      }

      final long length = storage.size(config.getBucket(), taskKey);
      try {
        final long start;

        if (offset > 0 && offset < length) {
          start = offset;
        } else if (offset < 0 && (-1 * offset) < length) {
          start = length + offset;
        } else {
          start = 0;
        }

        return Optional.of(new GoogleByteSource(storage, config.getBucket(), taskKey).openStream(start));
      }
      catch (Exception e) {
        throw new IOException(e);
      }
    }
    catch (IOException e) {
      throw new IOE(e, "Failed to stream logs from: %s", taskKey);
    }
  }

  private String getTaskLogKey(String taskid)
  {
    return config.getPrefix() + "/" + taskid.replace(':', '_');
  }

  private String getTaskReportKey(String taskid)
  {
    return config.getPrefix() + "/" + taskid.replace(':', '_') + ".report.json";
  }

  private String getTaskStatusKey(String taskid)
  {
    return config.getPrefix() + "/" + taskid.replace(':', '_') + ".status.json";
  }

  @Override
  public void killAll() throws IOException
  {
    LOG.info(
        "Deleting all task logs from gs location [bucket: '%s' prefix: '%s'].",
        config.getBucket(),
        config.getPrefix()
    );

    long now = timeSupplier.getAsLong();
    killOlderThan(now);
  }

  @Override
  public void killOlderThan(long timestamp) throws IOException
  {
    LOG.info(
        "Deleting all task logs from gs location [bucket: '%s' prefix: '%s'] older than %s.",
        config.getBucket(),
        config.getPrefix(),
        new Date(timestamp)
    );
    try {
      GoogleUtils.deleteObjectsInPath(
          storage,
          inputDataConfig,
          config.getBucket(),
          config.getPrefix(),
          (object) -> object.getLastUpdateTime() < timestamp
      );
    }
    catch (Exception e) {
      LOG.error("Error occurred while deleting task log files from gs. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
