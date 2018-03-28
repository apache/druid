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

package io.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Provides task logs archived on S3.
 */
public class S3TaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(S3TaskLogs.class);

  private final AmazonS3 service;
  private final S3TaskLogsConfig config;

  @Inject
  public S3TaskLogs(S3TaskLogsConfig config, AmazonS3 service)
  {
    this.config = config;
    this.service = service;
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);

    try {
      final ObjectMetadata objectMetadata = service.getObjectMetadata(config.getS3Bucket(), taskKey);

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

                final GetObjectRequest request = new GetObjectRequest(config.getS3Bucket(), taskKey)
                    .withMatchingETagConstraint(objectMetadata.getETag())
                    .withRange(start, end);

                return service.getObject(request).getObjectContent();
              }
              catch (AmazonServiceException e) {
                throw new IOException(e);
              }
            }
          }
      );
    }
    catch (AmazonS3Exception e) {
      if (404 == e.getStatusCode()
          || "NoSuchKey".equals(e.getErrorCode())
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
    final String taskKey = getTaskLogKey(taskid);
    log.info("Pushing task log %s to: %s", logFile, taskKey);

    try {
      S3Utils.retryS3Operation(
          () -> {
            service.putObject(config.getS3Bucket(), taskKey, logFile);
            return null;
          }
      );
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  private String getTaskLogKey(String taskid)
  {
    return StringUtils.format("%s/%s/log", config.getS3Prefix(), taskid);
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
