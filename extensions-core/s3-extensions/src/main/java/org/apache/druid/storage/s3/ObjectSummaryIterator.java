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

import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.apache.druid.java.util.common.RE;

import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator class used by {@link S3Utils#objectSummaryIterator}.
 *
 * As required by the specification of that method, this iterator is computed incrementally in batches of
 * {@code maxListLength}. The first call is made at the same time the iterator is constructed.
 */
public class ObjectSummaryIterator implements Iterator<S3ObjectSummary>
{
  private final ServerSideEncryptingAmazonS3 s3Client;
  private final Iterator<URI> prefixesIterator;
  private final int maxListingLength;

  private ListObjectsV2Request request;
  private ListObjectsV2Result result;
  private Iterator<S3ObjectSummary> objectSummaryIterator;
  private S3ObjectSummary currentObjectSummary;

  ObjectSummaryIterator(
      final ServerSideEncryptingAmazonS3 s3Client,
      final Iterable<URI> prefixes,
      final int maxListingLength
  )
  {
    this.s3Client = s3Client;
    this.prefixesIterator = prefixes.iterator();
    this.maxListingLength = maxListingLength;

    prepareNextRequest();
    fetchNextBatch();
    advanceObjectSummary();
  }

  @Override
  public boolean hasNext()
  {
    return currentObjectSummary != null;
  }

  @Override
  public S3ObjectSummary next()
  {
    if (currentObjectSummary == null) {
      throw new NoSuchElementException();
    }

    final S3ObjectSummary retVal = currentObjectSummary;
    advanceObjectSummary();
    return retVal;
  }

  private void prepareNextRequest()
  {
    final URI currentUri = prefixesIterator.next();
    final String currentBucket = currentUri.getAuthority();
    final String currentPrefix = S3Utils.extractS3Key(currentUri);

    request = new ListObjectsV2Request()
        .withBucketName(currentBucket)
        .withPrefix(currentPrefix)
        .withMaxKeys(maxListingLength);
  }

  private void fetchNextBatch()
  {
    try {
      result = S3Utils.retryS3Operation(() -> s3Client.listObjectsV2(request));
      request.setContinuationToken(result.getNextContinuationToken());
      objectSummaryIterator = result.getObjectSummaries().iterator();
    }
    catch (AmazonS3Exception e) {
      throw new RE(
          e,
          "Failed to get object summaries from S3 bucket[%s], prefix[%s]; S3 error: %s",
          request.getBucketName(),
          request.getPrefix(),
          e.getMessage()
      );
    }
    catch (Exception e) {
      throw new RE(
          e,
          "Failed to get object summaries from S3 bucket[%s], prefix[%s]",
          request.getBucketName(),
          request.getPrefix()
      );
    }
  }

  /**
   * Advance objectSummaryIterator to the next non-placeholder, updating "currentObjectSummary".
   */
  private void advanceObjectSummary()
  {
    while (objectSummaryIterator.hasNext() || result.isTruncated() || prefixesIterator.hasNext()) {
      while (objectSummaryIterator.hasNext()) {
        currentObjectSummary = objectSummaryIterator.next();
        // skips directories and empty objects
        if (!isDirectoryPlaceholder(currentObjectSummary) && currentObjectSummary.getSize() > 0) {
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
   * S3ObjectSummary.
   */
  private static boolean isDirectoryPlaceholder(final S3ObjectSummary objectSummary)
  {
    // Recognize "standard" directory place-holder indications used by Amazon's AWS Console and Panic's Transmit.
    if (objectSummary.getKey().endsWith("/") && objectSummary.getSize() == 0) {
      return true;
    }

    // Recognize s3sync.rb directory placeholders by MD5/ETag value.
    if ("d66759af42f282e1ba19144df2d405d0".equals(objectSummary.getETag())) {
      return true;
    }

    // Recognize place-holder objects created by the Google Storage console or S3 Organizer Firefox extension.
    if (objectSummary.getKey().endsWith("_$folder$") && objectSummary.getSize() == 0) {
      return true;
    }

    return false;
  }
}
