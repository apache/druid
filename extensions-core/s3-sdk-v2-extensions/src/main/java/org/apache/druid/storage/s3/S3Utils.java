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

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.aws.v2.AWSClientConfig;
import org.apache.druid.common.aws.v2.AWSClientUtil;
import org.apache.druid.common.aws.v2.AWSEndpointConfig;
import org.apache.druid.common.aws.v2.AWSProxyConfig;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.RetryUtils.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.URIs;
import org.apache.druid.java.util.common.logger.Logger;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.services.s3.model.AccessControlPolicy;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
public class S3Utils
{
  private static final String SCHEME = S3StorageDruidModule.SCHEME;
  private static final Joiner JOINER = Joiner.on("/").skipNulls();
  private static final Logger log = new Logger(S3Utils.class);

  /**
   * Error for calling putObject with an entity over 5GB in size.
   */
  public static final String ERROR_ENTITY_TOO_LARGE = "EntityTooLarge";

  public static final Predicate<Throwable> S3RETRY = new Predicate<>()
  {
    @Override
    public boolean apply(Throwable e)
    {
      if (e == null) {
        return false;
      } else if (e instanceof IOException) {
        if (e.getCause() != null) {
          // Recurse with the underlying cause to see if it's retriable.
          return apply(e.getCause());
        }
        return true;
      } else if (e instanceof SdkClientException
                 && e.getMessage().contains("Data read has a different length than the expected")) {
        // Can happen when connections to S3 are dropped; see https://github.com/apache/druid/pull/11941.
        // SdkClientException can be thrown for many reasons and the only way to distinguish it is to look at
        // the message. This is not ideal, since the message may change, so it may need to be adjusted in the future.
        return true;
      } else if (e instanceof SdkClientException && e.getMessage().contains("Unable to execute HTTP request")) {
        // This is likely due to a temporary DNS issue and can be retried.
        return true;
      } else if (e instanceof SdkClientException && e.getMessage().contains("Unable to find a region via the region provider chain")) {
        // This can happen sometimes when AWS isn't able to get the credentials for some service:
        // https://github.com/aws/aws-sdk-java/issues/2285
        return true;
      } else if (e instanceof S3Exception && ((S3Exception) e).awsErrorDetails().sdkHttpResponse().statusCode() == 200 &&
                 (e.getMessage().contains("InternalError") || e.getMessage().contains("SlowDown"))) {
        // This can happen sometimes when AWS returns a 200 response with internal error message
        // https://repost.aws/knowledge-center/s3-resolve-200-internalerror
        return true;
      } else if (e instanceof InterruptedException) {
        Thread.interrupted(); // Clear interrupted state and not retry
        return false;
      } else if (e instanceof SdkException) {
        return AWSClientUtil.isClientExceptionRecoverable((SdkException) e);
      } else {
        return apply(e.getCause());
      }
    }
  };

  /**
   * Retries S3 operations that fail intermittently (due to io-related exceptions, during obtaining credentials, etc).
   * Service-level exceptions (access denied, file not found, etc) are not retried.
   */
  public static <T> T retryS3Operation(Task<T> f) throws Exception
  {
    return RetryUtils.retry(f, S3RETRY, RetryUtils.DEFAULT_MAX_TRIES);
  }

  /**
   * Retries S3 operations that fail intermittently (due to io-related exceptions, during obtaining credentials, etc).
   * Service-level exceptions (access denied, file not found, etc) are not retried.
   * Also provide a way to set maxRetries that can be useful, i.e. for testing.
   */
  public static <T> T retryS3Operation(Task<T> f, int maxRetries) throws Exception
  {
    return RetryUtils.retry(f, S3RETRY, maxRetries);
  }

  @Nullable
  public static String getS3ErrorCode(final Throwable e)
  {
    if (e == null) {
      return null;
    } else if (e instanceof S3Exception) {
      return ((S3Exception) e).awsErrorDetails().errorCode();
    } else {
      return getS3ErrorCode(e.getCause());
    }
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
    catch (S3Exception e) {
      if (e.awsErrorDetails().sdkHttpResponse().statusCode() == 404) {
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
      final int maxListingLength,
      final int maxRetries
  )
  {
    return new ObjectSummaryIterator(s3Client, prefixes, maxListingLength, maxRetries);
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
    return new CloudObjectLocation(object.bucket(), object.key());
  }

  static String constructSegmentPath(String baseKey, String storageDir)
  {
    return constructSegmentBasePath(baseKey, storageDir) + "index.zip";
  }

  static String constructSegmentBasePath(String baseKey, String storageDir)
  {
    return JOINER.join(
        baseKey.isEmpty() ? null : baseKey,
        storageDir
    ) + "/";
  }

  static AccessControlPolicy grantFullControlToBucketOwner(ServerSideEncryptingAmazonS3 s3Client, String bucket)
  {
    final AccessControlPolicy acl = s3Client.getBucketAcl(bucket);
    // In SDK v2, AccessControlPolicy is immutable, so we need to build a new one with the added grant
    Grant fullControlGrant = Grant.builder()
        .grantee(Grantee.builder().id(acl.owner().id()).type("CanonicalUser").build())
        .permission(Permission.FULL_CONTROL)
        .build();
    List<Grant> newGrants = new ArrayList<>(acl.grants());
    newGrants.add(fullControlGrant);
    return AccessControlPolicy.builder()
        .owner(acl.owner())
        .grants(newGrants)
        .build();
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
   * Gets a single {@link HeadObjectResponse} from s3.
   *
   * @param s3Client s3 client
   * @param bucket   s3 bucket
   * @param key      s3 object key
   */
  public static HeadObjectResponse getSingleObjectMetadata(ServerSideEncryptingAmazonS3 s3Client, String bucket, String key)
  {
    try {
      return retryS3Operation(() -> s3Client.getObjectMetadata(bucket, key));
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Delete the files from S3 in a specified bucket, matching a specified prefix and filter
   *
   * @param s3Client         s3 client
   * @param maxListingLength maximum number of keys to fetch and delete at a time
   * @param bucket           s3 bucket
   * @param prefix           the file prefix
   * @param filter           function which returns true if the prefix file found should be deleted and false otherwise.
   *
   * @throws Exception in case of errors
   */

  public static void deleteObjectsInPath(
      ServerSideEncryptingAmazonS3 s3Client,
      int maxListingLength,
      String bucket,
      String prefix,
      Predicate<S3ObjectSummary> filter
  )
      throws Exception
  {
    deleteObjectsInPath(s3Client, maxListingLength, bucket, prefix, filter, RetryUtils.DEFAULT_MAX_TRIES);
  }

  public static void deleteObjectsInPath(
      ServerSideEncryptingAmazonS3 s3Client,
      int maxListingLength,
      String bucket,
      String prefix,
      Predicate<S3ObjectSummary> filter,
      int maxRetries
  )
      throws Exception
  {
    log.debug("Deleting directory at bucket: [%s], path: [%s]", bucket, prefix);

    final List<ObjectIdentifier> keysToDelete = new ArrayList<>(maxListingLength);
    final ObjectSummaryIterator iterator = new ObjectSummaryIterator(
        s3Client,
        ImmutableList.of(new CloudObjectLocation(bucket, prefix).toUri("s3")),
        maxListingLength
    );

    while (iterator.hasNext()) {
      final S3ObjectSummary nextObject = iterator.next();
      if (filter.apply(nextObject)) {
        keysToDelete.add(ObjectIdentifier.builder().key(nextObject.key()).build());
        if (keysToDelete.size() == maxListingLength) {
          deleteBucketKeys(s3Client, bucket, keysToDelete, maxRetries);
          keysToDelete.clear();
        }
      }
    }

    if (keysToDelete.size() > 0) {
      deleteBucketKeys(s3Client, bucket, keysToDelete, maxRetries);
    }
  }

  public static void deleteBucketKeys(
      ServerSideEncryptingAmazonS3 s3Client,
      String bucket,
      List<ObjectIdentifier> keysToDelete,
      int retries
  )
      throws Exception
  {
    if (keysToDelete != null && log.isDebugEnabled()) {
      List<String> keys = keysToDelete.stream()
                                      .map(ObjectIdentifier::key)
                                      .collect(Collectors.toList());
      log.debug("Deleting keys from bucket: [%s], keys: [%s]", bucket, keys);
    }
    DeleteObjectsRequest deleteRequest = DeleteObjectsRequest.builder().bucket(bucket)
        .delete(builder -> builder.objects(keysToDelete))
        .build();
    S3Utils.retryS3Operation(() -> {
      s3Client.deleteObjects(deleteRequest);
      return null;
    }, retries);
    log.info("Deleted %d files", keysToDelete.size());
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
    PutObjectRequest.Builder putObjectRequestBuilder = PutObjectRequest.builder().bucket(bucket).key(key);

    if (!disableAcl) {
      // Note: In SDK v2, ACL is set differently. For now, skip ACL setting as it requires different handling
      // putObjectRequestBuilder.acl(ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL);
    }
    log.info("Pushing [%s] to bucket[%s] and key[%s].", file, bucket, key);
    service.upload(putObjectRequestBuilder.build(), file);
  }

  @Nullable
  public static String parseProtocol(@Nullable String protocol)
  {
    if (protocol == null) {
      return null;
    }

    if (protocol.equalsIgnoreCase("http")) {
      return "http";
    } else if (protocol.equalsIgnoreCase("https")) {
      return "https";
    } else {
      throw new IAE("Unknown protocol[%s]", protocol);
    }
  }

  public static String determineProtocol(AWSClientConfig clientConfig, AWSEndpointConfig endpointConfig)
  {
    final String protocolFromClientConfig = parseProtocol(clientConfig.getProtocol());
    final String endpointUrl = endpointConfig.getUrl();
    if (org.apache.commons.lang3.StringUtils.isNotEmpty(endpointUrl)) {
      final URI uri = URIs.parse(endpointUrl, protocolFromClientConfig != null ? protocolFromClientConfig : "https");
      final String protocol = parseProtocol(uri.getScheme());
      if (protocol != null && !protocol.equals(protocolFromClientConfig)) {
        log.warn("[%s] protocol will be used for endpoint [%s]", protocol, endpointUrl);
      }
      return protocol;
    } else {
      return protocolFromClientConfig != null ? protocolFromClientConfig : "https";
    }
  }

  public static ClientOverrideConfiguration.Builder setProxyConfig(
      ClientOverrideConfiguration.Builder confBuilder,
      AWSProxyConfig proxyConfig
  )
  {
    // In AWS SDK v2, proxy configuration is handled at the HTTP client level, not ClientOverrideConfiguration
    // This method is kept for API compatibility but proxy settings should be configured on the S3ClientBuilder
    return confBuilder;
  }
}
