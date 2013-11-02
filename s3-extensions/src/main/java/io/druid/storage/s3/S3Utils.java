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

import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Bucket;
import org.jets3t.service.model.S3Object;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;

/**
 *
 */
public class S3Utils
{
  private static final Logger log = new Logger(S3Utils.class);

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
  public static <T> T retryS3Operation(Callable<T> f) throws IOException, S3ServiceException, InterruptedException
  {
    int nTry = 0;
    final int maxTries = 10;
    while (true) {
      try {
        nTry++;
        return f.call();
      }
      catch (IOException e) {
        if (nTry <= maxTries) {
          awaitNextRetry(e, nTry);
        } else {
          throw e;
        }
      }
      catch (S3ServiceException e) {
        if (nTry <= maxTries &&
            (e.getCause() instanceof IOException ||
             (e.getS3ErrorCode() != null && e.getS3ErrorCode().equals("RequestTimeout")))) {
          awaitNextRetry(e, nTry);
        } else {
          throw e;
        }
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
      }
    }
  }

  private static void awaitNextRetry(Exception e, int nTry) throws InterruptedException
  {
    final long baseSleepMillis = 1000;
    final long maxSleepMillis = 60000;
    final double fuzzyMultiplier = Math.min(Math.max(1 + 0.2 * new Random().nextGaussian(), 0), 2);
    final long sleepMillis = (long) (Math.min(maxSleepMillis, baseSleepMillis * Math.pow(2, nTry)) * fuzzyMultiplier);
    log.warn("S3 fail on try %d, retrying in %,dms.", nTry, sleepMillis);
    Thread.sleep(sleepMillis);
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
              || "NoSuchBucket".equals(e.getS3ErrorCode()))
          {
              return false;
          }
          if ("AccessDenied".equals(e.getS3ErrorCode()))
          {
              // Object is inaccessible to current user, but does exist.
              return true;
          }
          // Something else has gone wrong
          throw e;
      }
    return true;
  }

}
