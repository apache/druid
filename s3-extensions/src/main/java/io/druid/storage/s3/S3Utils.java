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

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.metamx.common.RetryUtils;
import org.jets3t.service.ServiceException;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import java.io.IOException;
import java.util.concurrent.Callable;

/**
 *
 */
public class S3Utils
{
  private static final Joiner JOINER = Joiner.on("/").skipNulls();

  public static void closeStreamsQuietly(S3Object s3Obj)
  {
    if (s3Obj == null) {
      return;
    }

    try {
      s3Obj.closeDataInputStream();
    }
    catch (IOException e) {

    }
  }

  /**
   * Retries S3 operations that fail due to io-related exceptions. Service-level exceptions (access denied, file not
   * found, etc) are not retried.
   */
  public static <T> T retryS3Operation(Callable<T> f) throws Exception
  {
    final Predicate<Throwable> shouldRetry = new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable e)
      {
        if (e instanceof IOException) {
          return true;
        } else if (e instanceof ServiceException) {
          final boolean isIOException = e.getCause() instanceof IOException;
          final boolean isTimeout = "RequestTimeout".equals(((ServiceException) e).getErrorCode());
          return isIOException || isTimeout;
        } else {
          return false;
        }
      }
    };
    final int maxTries = 10;
    return RetryUtils.retry(f, shouldRetry, maxTries);
  }

  public static boolean isObjectInBucket(RestS3Service s3Client, String bucketName, String objectKey)
      throws ServiceException
  {
    try {
      s3Client.getObjectDetails(new S3Bucket(bucketName), objectKey);
    }
    catch (ServiceException e) {
      if (404 == e.getResponseCode()
          || "NoSuchKey".equals(e.getErrorCode())
          || "NoSuchBucket".equals(e.getErrorCode())) {
        return false;
      }
      if ("AccessDenied".equals(e.getErrorCode())) {
        // Object is inaccessible to current user, but does exist.
        return true;
      }
      // Something else has gone wrong
      throw e;
    }
    return true;
  }


  public static String constructSegmentPath(String baseKey, DataSegment segment)
  {
    return JOINER.join(
        baseKey.isEmpty() ? null : baseKey,
        DataSegmentPusherUtil.getStorageDir(segment)
    ) + "/index.zip";
  }

  public static String descriptorPathForSegmentPath(String s3Path)
  {
    return s3Path.substring(0, s3Path.lastIndexOf("/")) + "/descriptor.json";
  }
}
