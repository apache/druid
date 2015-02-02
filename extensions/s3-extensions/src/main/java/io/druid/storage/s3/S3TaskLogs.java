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

package io.druid.storage.s3;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.tasklogs.TaskLogs;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageService;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.StorageObject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.Callable;

/**
 * Provides task logs archived on S3.
 */
public class S3TaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(S3TaskLogs.class);

  private final StorageService service;
  private final S3TaskLogsConfig config;

  @Inject
  public S3TaskLogs(S3TaskLogsConfig config, RestS3Service service)
  {
    this.config = config;
    this.service = service;
  }

  @Override
  public Optional<ByteSource> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);

    try {
      final StorageObject objectDetails = service.getObjectDetails(config.getS3Bucket(), taskKey, null, null, null, null);

      return Optional.<ByteSource>of(
          new ByteSource()
          {
            @Override
            public InputStream openStream() throws IOException
            {
              try {
                final long start;
                final long end = objectDetails.getContentLength() - 1;

                if (offset > 0 && offset < objectDetails.getContentLength()) {
                  start = offset;
                } else if (offset < 0 && (-1 * offset) < objectDetails.getContentLength()) {
                  start = objectDetails.getContentLength() + offset;
                } else {
                  start = 0;
                }

                return service.getObject(
                    config.getS3Bucket(),
                    taskKey,
                    null,
                    null,
                    new String[]{objectDetails.getETag()},
                    null,
                    start,
                    end
                ).getDataInputStream();
              }
              catch (ServiceException e) {
                throw new IOException(e);
              }
            }
          }
      );
    }
    catch (ServiceException e) {
      if (404 == e.getResponseCode()
          || "NoSuchKey".equals(e.getErrorCode())
          || "NoSuchBucket".equals(e.getErrorCode())) {
        return Optional.absent();
      } else {
        throw new IOException(String.format("Failed to stream logs from: %s", taskKey), e);
      }
    }
  }

  public void pushTaskLog(final String taskid, final File logFile) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);
    log.info("Pushing task log %s to: %s", logFile, taskKey);

    try {
      S3Utils.retryS3Operation(
          new Callable<Void>()
          {
            @Override
            public Void call() throws Exception
            {
              final StorageObject object = new StorageObject(logFile);
              object.setKey(taskKey);
              service.putObject(config.getS3Bucket(), object);
              return null;
            }
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
    return String.format("%s/%s/log", config.getS3Prefix(), taskid);
  }
}
