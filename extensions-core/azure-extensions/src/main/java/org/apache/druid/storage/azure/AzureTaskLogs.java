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

package org.apache.druid.storage.azure;

import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.apache.druid.common.utils.CurrentTimeMillisSupplier;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;

/**
 * Deals with reading and writing task logs stored in Azure.
 */
public class AzureTaskLogs implements TaskLogs
{

  private static final Logger log = new Logger(AzureTaskLogs.class);

  private final AzureTaskLogsConfig config;
  private final AzureInputDataConfig inputDataConfig;
  private final AzureAccountConfig accountConfig;
  private final AzureStorage azureStorage;
  private final AzureCloudBlobIterableFactory azureCloudBlobIterableFactory;
  private final CurrentTimeMillisSupplier timeSupplier;

  @Inject
  public AzureTaskLogs(
      AzureTaskLogsConfig config,
      AzureInputDataConfig inputDataConfig,
      AzureAccountConfig accountConfig,
      @Global AzureStorage azureStorage,
      AzureCloudBlobIterableFactory azureCloudBlobIterableFactory,
      CurrentTimeMillisSupplier timeSupplier)
  {
    this.config = config;
    this.inputDataConfig = inputDataConfig;
    this.azureStorage = azureStorage;
    this.accountConfig = accountConfig;
    this.azureCloudBlobIterableFactory = azureCloudBlobIterableFactory;
    this.timeSupplier = timeSupplier;
  }

  @Override
  public void pushTaskLog(final String taskid, final File logFile)
  {
    final String taskKey = getTaskLogKey(taskid);
    log.info("Pushing task log %s to: %s", logFile, taskKey);
    pushTaskFile(logFile, taskKey);
  }

  @Override
  public void pushTaskReports(String taskid, File reportFile)
  {
    final String taskKey = getTaskReportsKey(taskid);
    log.info("Pushing task reports %s to: %s", reportFile, taskKey);
    pushTaskFile(reportFile, taskKey);
  }

  @Override
  public void pushTaskStatus(String taskid, File statusFile)
  {
    final String taskKey = getTaskStatusKey(taskid);
    log.info("Pushing task status %s to: %s", statusFile, taskKey);
    pushTaskFile(statusFile, taskKey);
  }

  private void pushTaskFile(final File logFile, String taskKey)
  {
    try {
      azureStorage.uploadBlockBlob(logFile, config.getContainer(), taskKey, accountConfig.getMaxTries());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void pushTaskPayload(String taskid, File taskPayloadFile)
  {
    final String taskKey = getTaskPayloadKey(taskid);
    log.info("Pushing task payload [%s] to location [%s]", taskPayloadFile, taskKey);
    pushTaskFile(taskPayloadFile, taskKey);
  }

  @Override
  public Optional<InputStream> streamTaskPayload(String taskid) throws IOException
  {
    return streamTaskFile(0, getTaskPayloadKey(taskid));
  }

  @Override
  public Optional<InputStream> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    return streamTaskFile(offset, getTaskLogKey(taskid));
  }

  @Override
  public Optional<InputStream> streamTaskReports(String taskid) throws IOException
  {
    return streamTaskFile(0, getTaskReportsKey(taskid));
  }

  @Override
  public Optional<InputStream> streamTaskStatus(String taskid) throws IOException
  {
    return streamTaskFile(0, getTaskStatusKey(taskid));
  }

  private Optional<InputStream> streamTaskFile(final long offset, String taskKey)
      throws IOException
  {
    final String container = config.getContainer();
    try {
      if (!azureStorage.getBlockBlobExists(container, taskKey)) {
        return Optional.absent();
      }
      try {
        final long start;
        final long length = azureStorage.getBlockBlobLength(container, taskKey);

        if (offset > 0 && offset < length) {
          start = offset;
        } else if (offset < 0 && (-1 * offset) < length) {
          start = length + offset;
        } else {
          start = 0;
        }

        InputStream stream = azureStorage.getBlockBlobInputStream(container, taskKey);
        stream.skip(start);

        return Optional.of(stream);
      }
      catch (Exception e) {
        throw new IOException(e);
      }
    }
    catch (BlobStorageException e) {
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

  private String getTaskStatusKey(String taskid)
  {
    return StringUtils.format("%s/%s/status.json", config.getPrefix(), taskid);
  }

  private String getTaskPayloadKey(String taskid)
  {
    return StringUtils.format("%s/%s/task.json", config.getPrefix(), taskid);
  }

  @Override
  public void killAll() throws IOException
  {
    log.info(
        "Deleting all task logs from Azure storage location [bucket: %s    prefix: %s].",
        config.getContainer(),
        config.getPrefix()
    );

    long now = timeSupplier.getAsLong();
    killOlderThan(now);
  }

  @Override
  public void killOlderThan(long timestamp) throws IOException
  {
    log.info(
        "Deleting all task logs from Azure storage location [bucket: '%s' prefix: '%s'] older than %s.",
        config.getContainer(),
        config.getPrefix(),
        new Date(timestamp)
    );
    try {
      AzureUtils.deleteObjectsInPath(
          azureStorage,
          inputDataConfig,
          accountConfig,
          azureCloudBlobIterableFactory,
          config.getContainer(),
          config.getPrefix(),
          (object) -> object.getLastModifed().getTime() < timestamp
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting task log files from Azure. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
