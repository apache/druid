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

import com.google.common.base.Predicate;
import com.metamx.common.RetryUtils;
import org.jets3t.service.S3ServiceException;
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
        } else if (e instanceof S3ServiceException) {
          final boolean isIOException = e.getCause() instanceof IOException;
          final boolean isTimeout = "RequestTimeout".equals(((S3ServiceException) e).getS3ErrorCode());
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
      throws S3ServiceException
  {
    try {
      s3Client.getObjectDetails(new S3Bucket(bucketName), objectKey);
    }
    catch (S3ServiceException e) {
      if (404 == e.getResponseCode()
          || "NoSuchKey".equals(e.getS3ErrorCode())
          || "NoSuchBucket".equals(e.getS3ErrorCode())) {
        return false;
      }
      if ("AccessDenied".equals(e.getS3ErrorCode())) {
        // Object is inaccessible to current user, but does exist.
        return true;
      }
      // Something else has gone wrong
      throw e;
    }
    return true;
  }

}
