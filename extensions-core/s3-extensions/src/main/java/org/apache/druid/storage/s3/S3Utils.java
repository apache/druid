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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.RetryUtils.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class S3Utils
{
  private static final String SCHEME = S3StorageDruidModule.SCHEME;
  private static final Joiner JOINER = Joiner.on("/").skipNulls();
  private static final Logger log = new Logger(S3Utils.class);


  static boolean isServiceExceptionRecoverable(AmazonServiceException ex)
  {
    final boolean isIOException = ex.getCause() instanceof IOException;
    final boolean isTimeout = "RequestTimeout".equals(ex.getErrorCode());
    final boolean badStatusCode = ex.getStatusCode() == 400 || ex.getStatusCode() == 403 || ex.getStatusCode() == 404;
    return !badStatusCode && (isIOException || isTimeout);
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
  static <T> T retryS3Operation(Task<T> f) throws Exception
  {
    return RetryUtils.retry(f, S3RETRY, RetryUtils.DEFAULT_MAX_TRIES);
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

  /**
   * Create an iterator over a set of S3 objects specified by a set of prefixes.
   *
   * For each provided prefix URI, the iterator will walk through all objects that are in the same bucket as the
   * provided URI and whose keys start with that URI's path, except for directory placeholders (which will be
   * ignored). The iterator is computed incrementally by calling {@link ServerSideEncryptingAmazonS3#listObjectsV2} for
   * each prefix in batches of {@param maxListLength}. The first call is made at the same time the iterator is
   * constructed.
   */
  public static Iterator<S3ObjectSummary> objectSummaryIterator(
      final ServerSideEncryptingAmazonS3 s3Client,
      final Iterable<URI> prefixes,
      final int maxListingLength
  )
  {
    return new ObjectSummaryIterator(s3Client, prefixes, maxListingLength);
  }

  /**
   * Create an {@link URI} from the given {@link S3ObjectSummary}. The result URI is composed as below.
   *
   * <pre>
   * {@code s3://{BUCKET_NAME}/{OBJECT_KEY}}
   * </pre>
   */
  public static URI summaryToUri(S3ObjectSummary object)
  {
    return summaryToCloudObjectLocation(object).toUri(SCHEME);
  }

  public static CloudObjectLocation summaryToCloudObjectLocation(S3ObjectSummary object)
  {
    return new CloudObjectLocation(object.getBucketName(), object.getKey());
  }

  static String constructSegmentPath(String baseKey, String storageDir)
  {
    return JOINER.join(
        baseKey.isEmpty() ? null : baseKey,
        storageDir
    ) + "/index.zip";
  }

  static AccessControlList grantFullControlToBucketOwner(ServerSideEncryptingAmazonS3 s3Client, String bucket)
  {
    final AccessControlList acl = s3Client.getBucketAcl(bucket);
    acl.grantAllPermissions(new Grant(new CanonicalGrantee(acl.getOwner().getId()), Permission.FullControl));
    return acl;
  }

  public static String extractS3Key(URI uri)
  {
    return StringUtils.maybeRemoveLeadingSlash(uri.getPath());
  }

  public static URI checkURI(URI uri)
  {
    if (uri.getScheme().equalsIgnoreCase(S3StorageDruidModule.SCHEME_S3_ZIP)) {
      uri = URI.create(SCHEME + uri.toString().substring(S3StorageDruidModule.SCHEME_S3_ZIP.length()));
    }
    return CloudObjectLocation.validateUriScheme(SCHEME, uri);
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

    // Using getObjectSummaries().size() instead of getKeyCount as, in some cases
    // it is observed that even though the getObjectSummaries returns some data
    // keyCount is still zero.
    if (result.getObjectSummaries().size() == 0) {
      throw new ISE("Cannot find object for bucket[%s] and key[%s]", bucket, key);
    }
    final S3ObjectSummary objectSummary = result.getObjectSummaries().get(0);
    if (!objectSummary.getBucketName().equals(bucket) || !objectSummary.getKey().equals(key)) {
      throw new ISE("Wrong object[%s] for bucket[%s] and key[%s]", objectSummary, bucket, key);
    }

    return objectSummary;
  }

  /**
   * Delete the files from S3 in a specified bucket, matching a specified prefix and filter
   * @param s3Client s3 client
   * @param config   specifies the configuration to use when finding matching files in S3 to delete
   * @param bucket   s3 bucket
   * @param prefix   the file prefix
   * @param filter   function which returns true if the prefix file found should be deleted and false otherwise.
   * @throws Exception
   */
  public static void deleteObjectsInPath(
      ServerSideEncryptingAmazonS3 s3Client,
      S3InputDataConfig config,
      String bucket,
      String prefix,
      Predicate<S3ObjectSummary> filter
  )
      throws Exception
  {
    final List<DeleteObjectsRequest.KeyVersion> keysToDelete = new ArrayList<>(config.getMaxListingLength());
    final ObjectSummaryIterator iterator = new ObjectSummaryIterator(
        s3Client,
        ImmutableList.of(new CloudObjectLocation(bucket, prefix).toUri("s3")),
        config.getMaxListingLength()
    );

    while (iterator.hasNext()) {
      final S3ObjectSummary nextObject = iterator.next();
      if (filter.apply(nextObject)) {
        keysToDelete.add(new DeleteObjectsRequest.KeyVersion(nextObject.getKey()));
        if (keysToDelete.size() == config.getMaxListingLength()) {
          deleteBucketKeys(s3Client, bucket, keysToDelete);
          log.info("Deleted %d files", keysToDelete.size());
          keysToDelete.clear();
        }
      }
    }

    if (keysToDelete.size() > 0) {
      deleteBucketKeys(s3Client, bucket, keysToDelete);
      log.info("Deleted %d files", keysToDelete.size());
    }
  }

  private static void deleteBucketKeys(
      ServerSideEncryptingAmazonS3 s3Client,
      String bucket,
      List<DeleteObjectsRequest.KeyVersion> keysToDelete
  )
      throws Exception
  {
    DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keysToDelete);
    S3Utils.retryS3Operation(() -> {
      s3Client.deleteObjects(deleteRequest);
      return null;
    });
  }

  /**
   * Uploads a file to S3 if possible. First trying to set ACL to give the bucket owner full control of the file before uploading.
   *
   * @param service    S3 client
   * @param disableAcl true if ACL shouldn't be set for the file
   * @param key        The key under which to store the new object.
   * @param file       The path of the file to upload to Amazon S3.
   */
  static void uploadFileIfPossible(
      ServerSideEncryptingAmazonS3 service,
      boolean disableAcl,
      String bucket,
      String key,
      File file
  )
  {
    final PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, file);

    if (!disableAcl) {
      putObjectRequest.setAccessControlList(S3Utils.grantFullControlToBucketOwner(service, bucket));
    }
    log.info("Pushing [%s] to bucket[%s] and key[%s].", file, bucket, key);
    service.putObject(putObjectRequest);
  }
}
