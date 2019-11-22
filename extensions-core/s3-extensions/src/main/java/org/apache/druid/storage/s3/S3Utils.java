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
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.RetryUtils.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
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
  private static final Logger log = new Logger(S3Utils.class);

  public static final int MAX_S3_RETRIES = 10;

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
  public static <T> T retryS3Operation(Task<T> f) throws Exception
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

  public static Iterator<S3ObjectSummary> objectSummaryIterator(
      final ServerSideEncryptingAmazonS3 s3Client,
      final URI prefix,
      final int numMaxKeys
  )
  {
    return lazyFetchingObjectSummariesIterator(s3Client, Iterators.singletonIterator(prefix), numMaxKeys);
  }

  /**
   * Create an iterator over a set of s3 objects specified by a set of 'prefixes' which may be paths or individual
   * objects, in order to get {@link S3ObjectSummary} for each discovered object. This iterator is computed lazily as it
   * is iterated, calling {@link ServerSideEncryptingAmazonS3#listObjectsV2} for each prefix in batches of
   * {@param maxListLength}, falling back to {@link ServerSideEncryptingAmazonS3#getObjectMetadata} if the list API
   * returns a 403 status code as a fallback to check if the URI is a single object instead of a directory. These
   * summaries are supplied to the outer iterator until drained, then if additional results for the current prefix are
   * still available, it will continue fetching and repeat the process, else it will move on to the next prefix,
   * continuing until all objects have been evaluated.
   */
  public static Iterator<S3ObjectSummary> lazyFetchingObjectSummariesIterator(
      final ServerSideEncryptingAmazonS3 s3Client,
      final Iterator<URI> uris,
      final int maxListingLength
  )
  {
    return new Iterator<S3ObjectSummary>()
    {
      private ListObjectsV2Request request;
      private ListObjectsV2Result result;
      private URI currentUri;
      private String currentBucket;
      private String currentPrefix;
      private Iterator<S3ObjectSummary> objectSummaryIterator;

      {
        prepareNextRequest();
        fetchNextBatch();
      }

      private void prepareNextRequest()
      {
        currentUri = uris.next();
        currentBucket = currentUri.getAuthority();
        currentPrefix = S3Utils.extractS3Key(currentUri);

        request = new ListObjectsV2Request()
            .withBucketName(currentBucket)
            .withPrefix(currentPrefix)
            .withMaxKeys(maxListingLength);
      }

      private void fetchNextBatch()
      {
        try {
          result = S3Utils.retryS3Operation(() -> s3Client.listObjectsV2(request));
          objectSummaryIterator = result.getObjectSummaries().iterator();
          request.setContinuationToken(result.getContinuationToken());
        }
        catch (AmazonS3Exception outerException) {
          log.error(outerException, "Exception while listing on %s", currentUri);

          if (outerException.getStatusCode() == 403) {
            // The "Access Denied" means users might not have a proper permission for listing on the given uri.
            // Usually this is not a problem, but the uris might be the full paths to input objects instead of prefixes.
            // In this case, users should be able to get objects if they have a proper permission for GetObject.

            log.warn("Access denied for %s. Try to get the object from the uri without listing", currentUri);
            try {
              final ObjectMetadata objectMetadata =
                  S3Utils.retryS3Operation(() -> s3Client.getObjectMetadata(currentBucket, currentPrefix));

              if (!S3Utils.isDirectoryPlaceholder(currentPrefix, objectMetadata)) {
                // it's not a directory, so just generate an object summary
                S3ObjectSummary fabricated = new S3ObjectSummary();
                fabricated.setBucketName(currentBucket);
                fabricated.setKey(currentPrefix);
                objectSummaryIterator = Iterators.singletonIterator(fabricated);
              } else {
                throw new RE(
                    "[%s] is a directory placeholder, "
                    + "but failed to get the object list under the directory due to permission",
                    currentUri
                );
              }
            }
            catch (Exception innerException) {
              throw new RuntimeException(innerException);
            }
          } else {
            throw new RuntimeException(outerException);
          }
        }
        catch (Exception ex) {
          throw new RuntimeException(ex);
        }
      }

      @Override
      public boolean hasNext()
      {
        return objectSummaryIterator.hasNext() || result.isTruncated() || uris.hasNext();
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
        } else if (uris.hasNext()) {
          prepareNextRequest();
          fetchNextBatch();
        }

        if (!objectSummaryIterator.hasNext()) {
          throw new ISE(
              "Failed to further iterate on bucket[%s] and prefix[%s]. The last continuationToken was [%s]",
              currentBucket,
              currentPrefix,
              result.getContinuationToken()
          );
        }

        return objectSummaryIterator.next();
      }
    };
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
    final String originalAuthority = object.getBucketName();
    final String originalPath = object.getKey();
    final String authority = originalAuthority.endsWith("/") ?
                             originalAuthority.substring(0, originalAuthority.length() - 1) :
                             originalAuthority;
    final String path = originalPath.startsWith("/") ? originalPath.substring(1) : originalPath;

    return URI.create(StringUtils.format("s3://%s/%s", authority, path));
  }

  public static S3Coords summaryToS3Coords(S3ObjectSummary object)
  {
    return new S3Coords(object.getBucketName(), object.getKey());
  }

  public static String constructSegmentPath(String baseKey, String storageDir)
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
   * Uploads a file to S3 if possible. First trying to set ACL to give the bucket owner full control of the file before uploading.
   *
   * @param service S3 client
   * @param disableAcl true if ACL shouldn't be set for the file
   * @param key The key under which to store the new object.
   * @param file The path of the file to upload to Amazon S3.
   */
  public static void uploadFileIfPossible(ServerSideEncryptingAmazonS3 service, boolean disableAcl, String bucket, String key, File file)
  {
    final PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, key, file);

    if (!disableAcl) {
      putObjectRequest.setAccessControlList(S3Utils.grantFullControlToBucketOwner(service, bucket));
    }
    log.info("Pushing [%s] to bucket[%s] and key[%s].", file, bucket, key);
    service.putObject(putObjectRequest);
  }
}
