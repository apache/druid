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

package org.apache.druid.storage.azure;

import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator class used by {@link AzureUtils#blobItemIterator}.
 * <p>
 * As required by the specification of that method, this iterator is computed incrementally in batches of
 * {@code maxListLength}. The first call is made at the same time the iterator is constructed.
 */
public class BlobItemIterator implements Iterator<CloudBlobDruid>
{
  private final Logger log = new Logger(BlobItemIterator.class);
  private final AzureStorage storage;
  private final Iterator<URI> prefixesIterator;
  private final int maxListingLength;

  private ResultSegmentDruid<ListBlobItem> result;
  private String currentContainer;
  private String currentPrefix;
  private ResultContinuation continuationToken;
  private Iterator<ListBlobItem> blobItemIterator;
  private CloudBlobDruid currentBlobItem;

  BlobItemIterator(
      final AzureStorage storage,
      final Iterable<URI> prefixes,
      final int maxListingLength
  )
  {
    log.info("In BlobItemIterator constuctor:\nstorage: %s\nprefixes: %s\nmaxListingLength: %s",
             storage, prefixes, maxListingLength
    );
    this.storage = storage;
    this.prefixesIterator = prefixes.iterator();
    this.maxListingLength = maxListingLength;
    this.currentContainer = null;

    prepareNextRequest();
    fetchNextBatch();
    advanceBlobItem();
  }

  @Override
  public boolean hasNext()
  {
    return currentBlobItem != null;
  }

  @Override
  public CloudBlobDruid next()
  {
    if (currentBlobItem == null) {
      log.info("next: NoSuchElementException");
      throw new NoSuchElementException();
    }

    final CloudBlobDruid retVal = currentBlobItem;
    log.info("next: retVal: %s", retVal);
    advanceBlobItem();
    return retVal;
  }

  private void prepareNextRequest()
  {
    URI currentUri = prefixesIterator.next();
    currentContainer = currentUri.getAuthority();
    currentPrefix = AzureUtils.extractAzureKey(currentUri);
    log.info("prepareNextRequest:\ncurrentUri: %s\ncurrentContainer: %s\ncurrentPrefix: %s",
             currentUri, currentContainer, currentPrefix
    );
    result = null;
    continuationToken = null;
  }

  private void fetchNextBatch()
  {
    try {
      result = storage.listBlobsWithPrefixInContainerSegmented(
          currentContainer,
          currentPrefix,
          continuationToken,
          maxListingLength
      );
      continuationToken = result.getContinuationToken();
      blobItemIterator = result.getResults().iterator();
    }
    catch (Exception e) {
      log.warn("fetchNextBatch threw exception: %s", e.getMessage());
      throw new RE(
          e,
          "Failed to get object summaries from S3 bucket[%s], prefix[%s]",
          currentContainer,
          currentPrefix,
          e.getMessage()
      );
    }
  }

  /**
   * Advance objectSummaryIterator to the next non-placeholder, updating "currentObjectSummary".
   */
  private void advanceBlobItem()
  {
    while (blobItemIterator.hasNext() || continuationToken != null || prefixesIterator.hasNext()) {
      while (blobItemIterator.hasNext()) {
        ListBlobItem blobItem = blobItemIterator.next();
        /* skip directory objects */
        if (blobItem instanceof CloudBlob) {
          currentBlobItem = new CloudBlobDruid((CloudBlob) blobItem);
          return;
        }
      }

      if (continuationToken != null) {
        fetchNextBatch();
      } else if (prefixesIterator.hasNext()) {
        prepareNextRequest();
        fetchNextBatch();
      }
    }

    // Truly nothing left to read.
    log.info("advanceBlobItem: nothing left to read");
    currentBlobItem = null;
  }
}
