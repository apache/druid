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
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectRequest;
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

public class OssUtils
{
  private static final String SCHEME = OssStorageDruidModule.SCHEME;
  private static final Joiner JOINER = Joiner.on("/").skipNulls();
  private static final Logger log = new Logger(OssUtils.class);
  public static final int MAX_LISTING_LENGTH = 1000; //limited by Aliyun OSS SDK


  static boolean isServiceExceptionRecoverable(OSSException ex)
  {
    final boolean isIOException = ex.getCause() instanceof IOException;
    final boolean isTimeout = "RequestTimeout".equals(ex.getErrorCode());
    final boolean badStatusCode = false; //ex. == 400 || ex.getStatusCode() == 403 || ex.getStatusCode() == 404;
    return !badStatusCode && (isIOException || isTimeout);
  }

  public static final Predicate<Throwable> RETRYABLE = new Predicate<Throwable>()
  {
    @Override
    public boolean apply(Throwable e)
    {
      if (e == null) {
        return false;
      } else if (e instanceof IOException) {
        return true;
      } else if (e instanceof OSSException) {
        return isServiceExceptionRecoverable((OSSException) e);
      } else {
        return apply(e.getCause());
      }
    }
  };

  /**
   * Retries aliyun OSS operations that fail due to io-related exceptions. Service-level exceptions (access denied, file not
   * found, etc) are not retried.
   */
  static <T> T retry(Task<T> f) throws Exception
  {
    return RetryUtils.retry(f, RETRYABLE, RetryUtils.DEFAULT_MAX_TRIES);
  }

  static boolean isObjectInBucketIgnoringPermission(
      OSS client,
      String bucketName,
      String objectKey
  )
  {
    try {
      return client.doesObjectExist(bucketName, objectKey);
    }
    catch (OSSException e) {
      if (e.getErrorCode().equals("NoSuchKey")) {
        // Object is inaccessible to current user, but does exist.
        return true;
      }
      // Something else has gone wrong
      throw e;
    }
  }

  /**
   * Create an iterator over a set of aliyun OSS objects specified by a set of prefixes.
   * <p>
   * For each provided prefix URI, the iterator will walk through all objects that are in the same bucket as the
   * provided URI and whose keys start with that URI's path, except for directory placeholders (which will be
   * ignored). The iterator is computed incrementally by calling {@link OSS#listObjects} for
   * each prefix in batches of {@param maxListingLength}. The first call is made at the same time the iterator is
   * constructed.
   */
  public static Iterator<OSSObjectSummary> objectSummaryIterator(
      final OSS client,
      final Iterable<URI> prefixes,
      final int maxListingLength
  )
  {
    return new OssObjectSummaryIterator(client, prefixes, maxListingLength);
  }

  /**
   * Create an {@link URI} from the given {@link OSSObjectSummary}. The result URI is composed as below.
   *
   * <pre>
   * {@code oss://{BUCKET_NAME}/{OBJECT_KEY}}
   * </pre>
   */
  public static URI summaryToUri(OSSObjectSummary object)
  {
    return summaryToCloudObjectLocation(object).toUri(SCHEME);
  }

  public static CloudObjectLocation summaryToCloudObjectLocation(OSSObjectSummary object)
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

  public static String extractKey(URI uri)
  {
    return StringUtils.maybeRemoveLeadingSlash(uri.getPath());
  }

  public static URI checkURI(URI uri)
  {
    if (uri.getScheme().equalsIgnoreCase(OssStorageDruidModule.SCHEME_ZIP)) {
      uri = URI.create(SCHEME + uri.toString().substring(OssStorageDruidModule.SCHEME_ZIP.length()));
    }
    return CloudObjectLocation.validateUriScheme(SCHEME, uri);
  }

  /**
   * Gets a single {@link OSSObjectSummary} from aliyun OSS. Since this method might return a wrong object if there are multiple
   * objects that match the given key, this method should be used only when it's guaranteed that the given key is unique
   * in the given bucket.
   *
   * @param client aliyun OSS client
   * @param bucket aliyun OSS bucket
   * @param key    unique key for the object to be retrieved
   */
  public static OSSObjectSummary getSingleObjectSummary(OSS client, String bucket, String key)
  {
    final ListObjectsRequest request = new ListObjectsRequest();
    request.setBucketName(bucket);
    request.setPrefix(key);
    request.setMaxKeys(1);
    final ObjectListing result = client.listObjects(request);

    // Using getObjectSummaries().size() instead of getKeyCount as, in some cases
    // it is observed that even though the getObjectSummaries returns some data
    // keyCount is still zero.
    if (result.getObjectSummaries().size() == 0) {
      throw new ISE("Cannot find object for bucket[%s] and key[%s]", bucket, key);
    }
    final OSSObjectSummary objectSummary = result.getObjectSummaries().get(0);
    if (!objectSummary.getBucketName().equals(bucket) || !objectSummary.getKey().equals(key)) {
      throw new ISE("Wrong object[%s] for bucket[%s] and key[%s]", objectSummary, bucket, key);
    }

    return objectSummary;
  }

  /**
   * Delete the files from aliyun OSS in a specified bucket, matching a specified prefix and filter
   *
   * @param client aliyun OSS client
   * @param config specifies the configuration to use when finding matching files in aliyun OSS to delete
   * @param bucket aliyun OSS bucket
   * @param prefix the file prefix
   * @param filter function which returns true if the prefix file found should be deleted and false otherwise.
   * @throws Exception
   */
  public static void deleteObjectsInPath(
      OSS client,
      OssInputDataConfig config,
      String bucket,
      String prefix,
      Predicate<OSSObjectSummary> filter
  )
      throws Exception
  {
    final List<String> keysToDelete = new ArrayList<>(config.getMaxListingLength());
    final OssObjectSummaryIterator iterator = new OssObjectSummaryIterator(
        client,
        ImmutableList.of(new CloudObjectLocation(bucket, prefix).toUri("http")),
        config.getMaxListingLength()
    );

    while (iterator.hasNext()) {
      final OSSObjectSummary nextObject = iterator.next();
      if (filter.apply(nextObject)) {
        keysToDelete.add(nextObject.getKey());
        if (keysToDelete.size() == config.getMaxListingLength()) {
          deleteBucketKeys(client, bucket, keysToDelete);
          log.info("Deleted %d files", keysToDelete.size());
          keysToDelete.clear();
        }
      }
    }

    if (keysToDelete.size() > 0) {
      deleteBucketKeys(client, bucket, keysToDelete);
      log.info("Deleted %d files", keysToDelete.size());
    }
  }

  private static void deleteBucketKeys(
      OSS client,
      String bucket,
      List<String> keysToDelete
  )
      throws Exception
  {
    DeleteObjectsRequest deleteRequest = new DeleteObjectsRequest(bucket).withKeys(keysToDelete);
    OssUtils.retry(() -> {
      client.deleteObjects(deleteRequest);
      return null;
    });
  }

  /**
   * Uploads a file to aliyun OSS if possible. First trying to set ACL to give the bucket owner full control of the file before uploading.
   *
   * @param client     aliyun OSS client
   * @param key        The key under which to store the new object.
   * @param file       The path of the file to upload to aliyun OSS.
   */
  static void uploadFileIfPossible(
      OSS client,
      String bucket,
      String key,
      File file
  )
  {
    final PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, file);

    log.info("Pushing [%s] to bucket[%s] and key[%s].", file, bucket, key);
    client.putObject(putObjectRequest);
  }
}
