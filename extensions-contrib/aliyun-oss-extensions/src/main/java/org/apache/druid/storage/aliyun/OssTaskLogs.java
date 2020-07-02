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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.OSSException;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.ObjectMetadata;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import org.apache.druid.common.utils.CurrentTimeMillisSupplier;
import org.apache.druid.java.util.common.IOE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.Date;

/**
 * Provides task logs archived in aliyun OSS
 */
public class OssTaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(OssTaskLogs.class);

  private final OSS client;
  private final OssTaskLogsConfig config;
  private final OssInputDataConfig inputDataConfig;
  private final CurrentTimeMillisSupplier timeSupplier;

  @Inject
  public OssTaskLogs(
      OSS service,
      OssTaskLogsConfig config,
      OssInputDataConfig inputDataConfig,
      CurrentTimeMillisSupplier timeSupplier
  )
  {
    this.client = service;
    this.config = config;
    this.inputDataConfig = inputDataConfig;
    this.timeSupplier = timeSupplier;
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid, "log");
    return streamTaskFile(offset, taskKey);
  }

  @Override
  public Optional<ByteSource> streamTaskReports(String taskid) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid, "report.json");
    return streamTaskFile(0, taskKey);
  }

  private Optional<ByteSource> streamTaskFile(final long offset, String taskKey) throws IOException
  {
    try {
      final ObjectMetadata objectMetadata = client.getObjectMetadata(config.getBucket(), taskKey);

      return Optional.of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              try {
                final long start;
                final long end = objectMetadata.getContentLength() - 1;

                if (offset > 0 && offset < objectMetadata.getContentLength()) {
                  start = offset;
                } else if (offset < 0 && (-1 * offset) < objectMetadata.getContentLength()) {
                  start = objectMetadata.getContentLength() + offset;
                } else {
                  start = 0;
                }

                final GetObjectRequest request = new GetObjectRequest(config.getBucket(), taskKey);
                request.setMatchingETagConstraints(Collections.singletonList(objectMetadata.getETag()));
                request.setRange(start, end);

                return client.getObject(request).getObjectContent();
              }
              catch (OSSException e) {
                throw new IOException(e);
              }
            }
          }
      );
    }
    catch (OSSException e) {
      if ("NoSuchKey".equals(e.getErrorCode())
          || "NoSuchBucket".equals(e.getErrorCode())) {
        return Optional.absent();
      } else {
        throw new IOE(e, "Failed to stream logs from: %s", taskKey);
      }
    }
  }

  @Override
  public void pushTaskLog(final String taskid, final File logFile) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid, "log");
    log.info("Pushing task log %s to: %s", logFile, taskKey);
    pushTaskFile(logFile, taskKey);
  }

  @Override
  public void pushTaskReports(String taskid, File reportFile) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid, "report.json");
    log.info("Pushing task reports %s to: %s", reportFile, taskKey);
    pushTaskFile(reportFile, taskKey);
  }

  private void pushTaskFile(final File logFile, String taskKey) throws IOException
  {
    try {
      OssUtils.retry(
          () -> {
            OssUtils.uploadFileIfPossible(client, config.getBucket(), taskKey, logFile);
            return null;
          }
      );
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw new RuntimeException(e);
    }
  }

  String getTaskLogKey(String taskid, String filename)
  {
    return StringUtils.format("%s/%s/%s", config.getPrefix(), taskid, filename);
  }

  @Override
  public void killAll() throws IOException
  {
    log.info(
        "Deleting all task logs from aliyun OSS location [bucket: '%s' prefix: '%s'].",
        config.getBucket(),
        config.getPrefix()
    );

    long now = timeSupplier.getAsLong();
    killOlderThan(now);
  }

  @Override
  public void killOlderThan(long timestamp) throws IOException
  {
    log.info(
        "Deleting all task logs from aliyun OSS location [bucket: '%s' prefix: '%s'] older than %s.",
        config.getBucket(),
        config.getPrefix(),
        new Date(timestamp)
    );
    try {
      OssUtils.deleteObjectsInPath(
          client,
          inputDataConfig,
          config.getBucket(),
          config.getPrefix(),
          (object) -> object.getLastModified().getTime() < timestamp
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting task log files from aliyun OSS. Error: %s", e.getMessage());
      throw new IOException(e);
    }
  }
}
