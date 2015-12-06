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

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.metamx.common.RetryUtils;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import org.jets3t.service.ServiceException;
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

  public static boolean isServiceExceptionRecoverable(ServiceException ex)
  {
    final boolean isIOException = ex.getCause() instanceof IOException;
    final boolean isTimeout = "RequestTimeout".equals(((ServiceException) ex).getErrorCode());
    return isIOException || isTimeout;
  }

  public static final Predicate<Throwable> S3RETRY = new Predicate<Throwable>()
  {
    @Override
    public boolean apply(Throwable e)
    {
      if (e == null) {
        return false;
      } else if (e instanceof IOException) {
        return true;
      } else if (e instanceof ServiceException) {
        return isServiceExceptionRecoverable((ServiceException) e);
      } else {
        return apply(e.getCause());
      }
    }
  };

  /**
   * Retries S3 operations that fail due to io-related exceptions. Service-level exceptions (access denied, file not
   * found, etc) are not retried.
   */
  public static <T> T retryS3Operation(Callable<T> f) throws Exception
  {
    final int maxTries = 10;
    return RetryUtils.retry(f, S3RETRY, maxTries);
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
