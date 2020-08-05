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
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import org.apache.druid.java.util.common.RE;

import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator class used by {@link OssUtils#objectSummaryIterator}.
 * <p>
 * As required by the specification of that method, this iterator is computed incrementally in batches of
 * {@code maxListLength}. The first call is made at the same time the iterator is constructed.
 *
 */
public class OssObjectSummaryIterator implements Iterator<OSSObjectSummary>
{
  private final OSS client;
  private final Iterator<URI> prefixesIterator;
  private final int maxListingLength;

  private ListObjectsRequest request;
  private ObjectListing result;
  private Iterator<OSSObjectSummary> objectSummaryIterator;
  private OSSObjectSummary currentObjectSummary;

  OssObjectSummaryIterator(
      final OSS client,
      final Iterable<URI> prefixes,
      final int maxListingLength
  )
  {
    this.client = client;
    this.prefixesIterator = prefixes.iterator();
    this.maxListingLength = Math.min(OssUtils.MAX_LISTING_LENGTH, maxListingLength);

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
  public OSSObjectSummary next()
  {
    if (currentObjectSummary == null) {
      throw new NoSuchElementException();
    }

    final OSSObjectSummary retVal = currentObjectSummary;
    advanceObjectSummary();
    return retVal;
  }

  private void prepareNextRequest()
  {
    final URI currentUri = prefixesIterator.next();
    final String currentBucket = currentUri.getAuthority();
    final String currentPrefix = OssUtils.extractKey(currentUri);

    request = new ListObjectsRequest(currentBucket, currentPrefix, null, null, maxListingLength);
  }

  private void fetchNextBatch()
  {
    try {
      result = OssUtils.retry(() -> client.listObjects(request));
      request.setMarker(result.getNextMarker());
      objectSummaryIterator = result.getObjectSummaries().iterator();
    }
    catch (OSSException e) {
      throw new RE(
          e,
          "Failed to get object summaries from aliyun OSS bucket[%s], prefix[%s]; error: %s",
          request.getBucketName(),
          request.getPrefix(),
          e.getMessage()
      );
    }
    catch (Exception e) {
      throw new RE(
          e,
          "Failed to get object summaries from aliyun OSS bucket[%s], prefix[%s]",
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
        if (currentObjectSummary.getSize() > 0 && !isDirectory(currentObjectSummary)) {
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
   * Based on {@link org.apache.druid.storage.s3.ObjectSummaryIterator} which is adapted from org.jets3t.service.model.StorageObject.isDirectoryPlaceholder().
   *
   */
  private static boolean isDirectory(final OSSObjectSummary objectSummary)
  {
    return objectSummary.getSize() == 0 && objectSummary.getKey().endsWith("/");
  }
}
