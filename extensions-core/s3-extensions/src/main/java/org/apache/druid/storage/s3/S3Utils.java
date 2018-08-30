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

package org.apache.druid.storage.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.RetryUtils.Task;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 *
 */
public class S3Utils
{
  private static final Joiner JOINER = Joiner.on("/").skipNulls();
  private static final String MIMETYPE_JETS3T_DIRECTORY = "application/x-directory";

  static boolean isServiceExceptionRecoverable(AmazonServiceException ex)
  {
    final boolean isIOException = ex.getCause() instanceof IOException;
    final boolean isTimeout = "RequestTimeout".equals(ex.getErrorCode());
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
      } else if (e instanceof AmazonServiceException) {
        return isServiceExceptionRecoverable((AmazonServiceException) e);
      } else {
        return apply(e.getCause());
      }
    }
  };

  /**
   * Retries S3 operations that fail due to io-related exceptions. Service-level exceptions (access denied, file not
   * found, etc) are not retried.
   */
  public static <T> T retryS3Operation(Task<T> f) throws Exception
  {
    final int maxTries = 10;
    return RetryUtils.retry(f, S3RETRY, maxTries);
  }

  static boolean isObjectInBucketIgnoringPermission(
      ServerSideEncryptingAmazonS3 s3Client,
      String bucketName,
      String objectKey
  )
  {
    try {
      return s3Client.doesObjectExist(bucketName, objectKey);
    }
    catch (AmazonS3Exception e) {
      if (e.getStatusCode() == 404) {
        // Object is inaccessible to current user, but does exist.
        return true;
      }
      // Something else has gone wrong
      throw e;
    }
  }

  public static Iterator<S3ObjectSummary> objectSummaryIterator(
      final ServerSideEncryptingAmazonS3 s3Client,
      final String bucket,
      final String prefix,
      final int numMaxKeys
  )
  {
    final ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(prefix)
        .withMaxKeys(numMaxKeys);

    return new Iterator<S3ObjectSummary>()
    {
      private ListObjectsV2Result result;
      private Iterator<S3ObjectSummary> objectSummaryIterator;

      {
        fetchNextBatch();
      }

      private void fetchNextBatch()
      {
        result = s3Client.listObjectsV2(request);
        objectSummaryIterator = result.getObjectSummaries().iterator();
        request.setContinuationToken(result.getContinuationToken());
      }

      @Override
      public boolean hasNext()
      {
        return objectSummaryIterator.hasNext() || result.isTruncated();
      }

      @Override
      public S3ObjectSummary next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        if (objectSummaryIterator.hasNext()) {
          return objectSummaryIterator.next();
        }

        if (result.isTruncated()) {
          fetchNextBatch();
        }

        if (!objectSummaryIterator.hasNext()) {
          throw new ISE(
              "Failed to further iterate on bucket[%s] and prefix[%s]. The last continuationToken was [%s]",
              bucket,
              prefix,
              result.getContinuationToken()
          );
        }

        return objectSummaryIterator.next();
      }
    };
  }

  public static String constructSegmentPath(String baseKey, String storageDir)
  {
    return JOINER.join(
        baseKey.isEmpty() ? null : baseKey,
        storageDir
    ) + "/index.zip";
  }

  static String descriptorPathForSegmentPath(String s3Path)
  {
    return s3Path.substring(0, s3Path.lastIndexOf("/")) + "/descriptor.json";
  }

  static String indexZipForSegmentPath(String s3Path)
  {
    return s3Path.substring(0, s3Path.lastIndexOf("/")) + "/index.zip";
  }

  static String toFilename(String key)
  {
    return toFilename(key, "");
  }

  static String toFilename(String key, final String suffix)
  {
    String filename = key.substring(key.lastIndexOf("/") + 1); // characters after last '/'
    filename = filename.substring(0, filename.length() - suffix.length()); // remove the suffix from the end
    return filename;
  }

  static AccessControlList grantFullControlToBucketOwner(ServerSideEncryptingAmazonS3 s3Client, String bucket)
  {
    final AccessControlList acl = s3Client.getBucketAcl(bucket);
    acl.grantAllPermissions(new Grant(new CanonicalGrantee(acl.getOwner().getId()), Permission.FullControl));
    return acl;
  }

  public static String extractS3Key(URI uri)
  {
    return uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
  }

  // Copied from org.jets3t.service.model.StorageObject.isDirectoryPlaceholder()
  public static boolean isDirectoryPlaceholder(String key, ObjectMetadata objectMetadata)
  {
    // Recognize "standard" directory place-holder indications used by
    // Amazon's AWS Console and Panic's Transmit.
    if (key.endsWith("/") && objectMetadata.getContentLength() == 0) {
      return true;
    }
    // Recognize s3sync.rb directory placeholders by MD5/ETag value.
    if ("d66759af42f282e1ba19144df2d405d0".equals(objectMetadata.getETag())) {
      return true;
    }
    // Recognize place-holder objects created by the Google Storage console
    // or S3 Organizer Firefox extension.
    if (key.endsWith("_$folder$") && objectMetadata.getContentLength() == 0) {
      return true;
    }

    // We don't use JetS3t APIs anymore, but the below check is still needed for backward compatibility.

    // Recognize legacy JetS3t directory place-holder objects, only gives
    // accurate results if an object's metadata is populated.
    if (objectMetadata.getContentLength() == 0 && MIMETYPE_JETS3T_DIRECTORY.equals(objectMetadata.getContentType())) {
      return true;
    }
    return false;
  }

  /**
   * Gets a single {@link S3ObjectSummary} from s3. Since this method might return a wrong object if there are multiple
   * objects that match the given key, this method should be used only when it's guaranteed that the given key is unique
   * in the given bucket.
   *
   * @param s3Client s3 client
   * @param bucket   s3 bucket
   * @param key      unique key for the object to be retrieved
   */
  public static S3ObjectSummary getSingleObjectSummary(ServerSideEncryptingAmazonS3 s3Client, String bucket, String key)
  {
    final ListObjectsV2Request request = new ListObjectsV2Request()
        .withBucketName(bucket)
        .withPrefix(key)
        .withMaxKeys(1);
    final ListObjectsV2Result result = s3Client.listObjectsV2(request);

    if (result.getKeyCount() == 0) {
      throw new ISE("Cannot find object for bucket[%s] and key[%s]", bucket, key);
    }
    final S3ObjectSummary objectSummary = result.getObjectSummaries().get(0);
    if (!objectSummary.getBucketName().equals(bucket) || !objectSummary.getKey().equals(key)) {
      throw new ISE("Wrong object[%s] for bucket[%s] and key[%s]", objectSummary, bucket, key);
    }

    return objectSummary;
  }
}
