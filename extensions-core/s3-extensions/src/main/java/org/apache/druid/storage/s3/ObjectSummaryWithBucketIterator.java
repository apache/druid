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

import org.apache.druid.java.util.common.RE;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Iterator that returns S3 objects along with their bucket names.
 * This is needed because AWS SDK v2's S3Object doesn't include the bucket name.
 */
public class ObjectSummaryWithBucketIterator implements Iterator<S3Utils.S3ObjectWithBucket>
{
  private final ServerSideEncryptingAmazonS3 s3Client;
  private final Iterator<URI> prefixesIterator;
  private final int maxListingLength;
  private final int maxRetries;

  private String currentBucket;
  private String currentPrefix;
  private String continuationToken;
  private ListObjectsV2Response result;
  private Iterator<S3Object> objectSummaryIterator;
  private S3Utils.S3ObjectWithBucket currentObjectSummary;
  private AtomicBoolean initialized;

  ObjectSummaryWithBucketIterator(
      final ServerSideEncryptingAmazonS3 s3Client,
      final Iterable<URI> prefixes,
      final int maxListingLength,
      final int maxRetries
  )
  {
    this.s3Client = s3Client;
    this.prefixesIterator = prefixes.iterator();
    this.maxListingLength = maxListingLength;
    this.maxRetries = maxRetries;
    this.initialized = new AtomicBoolean(false);
  }

  @Override
  public boolean hasNext()
  {
    initialize();
    return currentObjectSummary != null;
  }

  private void initialize()
  {
    if (initialized.compareAndSet(false, true)) {
      prepareNextRequest();
      fetchNextBatch();
      advanceObjectSummary();
    }
  }

  @Override
  public S3Utils.S3ObjectWithBucket next()
  {
    initialize();
    if (currentObjectSummary == null) {
      throw new NoSuchElementException();
    }

    final S3Utils.S3ObjectWithBucket retVal = currentObjectSummary;
    advanceObjectSummary();
    return retVal;
  }

  private void prepareNextRequest()
  {
    final URI currentUri = prefixesIterator.next();
    currentBucket = currentUri.getAuthority();
    currentPrefix = S3Utils.extractS3Key(currentUri);
    continuationToken = null;
  }

  private void fetchNextBatch()
  {
    try {
      ListObjectsV2Request request = ListObjectsV2Request.builder()
          .bucket(currentBucket)
          .prefix(currentPrefix)
          .maxKeys(maxListingLength)
          .continuationToken(continuationToken)
          .build();

      result = S3Utils.retryS3Operation(() -> s3Client.listObjectsV2(request), maxRetries);
      continuationToken = result.nextContinuationToken();
      objectSummaryIterator = result.contents().iterator();
    }
    catch (S3Exception e) {
      throw new RE(
          e,
          "Failed to get object summaries from S3 bucket[%s], prefix[%s]; S3 error: %s",
          currentBucket,
          currentPrefix,
          e.getMessage()
      );
    }
    catch (Exception e) {
      throw new RE(
          e,
          "Failed to get object summaries from S3 bucket[%s], prefix[%s]",
          currentBucket,
          currentPrefix
      );
    }
  }

  private void advanceObjectSummary()
  {
    while (objectSummaryIterator.hasNext() || result.isTruncated() || prefixesIterator.hasNext()) {
      while (objectSummaryIterator.hasNext()) {
        S3Object s3Object = objectSummaryIterator.next();
        // skips directories and empty objects
        if (!isDirectoryPlaceholder(s3Object) && s3Object.size() > 0) {
          currentObjectSummary = new S3Utils.S3ObjectWithBucket(currentBucket, s3Object);
          return;
        }
      }

      // Exhausted "objectSummaryIterator" without finding a non-placeholder.
      if (result.isTruncated()) {
        fetchNextBatch();
      } else if (prefixesIterator.hasNext()) {
        prepareNextRequest();
        fetchNextBatch();
      }
    }

    // Truly nothing left to read.
    currentObjectSummary = null;
  }

  /**
   * Checks if a given object is a directory placeholder and should be ignored.
   *
   * Adapted from org.jets3t.service.model.StorageObject.isDirectoryPlaceholder(). Does not include the check for
   * legacy JetS3t directory placeholder objects, since it is based on content-type, which isn't available in an
   * S3Object.
   */
  private static boolean isDirectoryPlaceholder(final S3Object objectSummary)
  {
    // Recognize "standard" directory place-holder indications used by Amazon's AWS Console and Panic's Transmit.
    if (objectSummary.key().endsWith("/") && objectSummary.size() == 0) {
      return true;
    }

    // Recognize s3sync.rb directory placeholders by MD5/ETag value.
    // See https://github.com/mondain/jets3t/blob/master/jets3t/src/main/java/org/jets3t/service/model/StorageObject.java.
    if ("d66759af42f282e1ba19144df2d405d0".equals(objectSummary.eTag())) {
      return true;
    }

    // Recognize place-holder objects created by the Google Storage console or S3 Organizer Firefox extension.
    if (objectSummary.key().endsWith("_$folder$") && objectSummary.size() == 0) {
      return true;
    }

    return false;
  }
}
