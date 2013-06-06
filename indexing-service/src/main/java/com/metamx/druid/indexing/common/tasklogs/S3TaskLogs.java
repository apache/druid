package com.metamx.druid.indexing.common.tasklogs;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.io.InputSupplier;
import com.metamx.common.logger.Logger;
import org.jets3t.service.ServiceException;
import org.jets3t.service.StorageService;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.StorageObject;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;

/**
 * Provides task logs archived on S3.
 */
public class S3TaskLogs implements TaskLogs
{
  private static final Logger log = new Logger(S3TaskLogs.class);

  private final String bucket;
  private final String prefix;
  private final StorageService service;

  public S3TaskLogs(String bucket, String prefix, RestS3Service service)
  {
    this.bucket = Preconditions.checkNotNull(bucket, "bucket");
    this.prefix = Preconditions.checkNotNull(prefix, "prefix");
    this.service = Preconditions.checkNotNull(service, "service");
  }

  @Override
  public Optional<InputSupplier<InputStream>> streamTaskLog(final String taskid, final long offset) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);

    try {
      final StorageObject objectDetails = service.getObjectDetails(bucket, taskKey, null, null, null, null);

      return Optional.<InputSupplier<InputStream>>of(
          new InputSupplier<InputStream>()
          {
            @Override
            public InputStream getInput() throws IOException
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
                    bucket,
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
      if (e.getErrorCode() != null && (e.getErrorCode().equals("NoSuchKey") || e.getErrorCode()
                                                                                .equals("NoSuchBucket"))) {
        return Optional.absent();
      } else {
        throw new IOException(String.format("Failed to stream logs from: %s", taskKey), e);
      }
    }
  }

  public void pushTaskLog(String taskid, File logFile) throws IOException
  {
    final String taskKey = getTaskLogKey(taskid);

    try {
      log.info("Pushing task log %s to: %s", logFile, taskKey);

      final StorageObject object = new StorageObject(logFile);
      object.setKey(taskKey);
      service.putObject(bucket, object);
    }
    catch (Exception e) {
      Throwables.propagateIfInstanceOf(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  private String getTaskLogKey(String taskid)
  {
    return String.format("%s/%s/log", prefix, taskid);
  }
}
