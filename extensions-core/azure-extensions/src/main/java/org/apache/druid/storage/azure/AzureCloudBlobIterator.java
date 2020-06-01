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

import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;
import org.apache.druid.storage.azure.blob.ListBlobItemHolder;
import org.apache.druid.storage.azure.blob.ListBlobItemHolderFactory;

import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * This iterator is computed incrementally in batches of {@link #maxListingLength}.
 * The first call is made at the same time the iterator is constructed.
 */
public class AzureCloudBlobIterator implements Iterator<CloudBlobHolder>
{
  private static final Logger log = new Logger(AzureCloudBlobIterator.class);
  private final AzureStorage storage;
  private final ListBlobItemHolderFactory blobItemDruidFactory;
  private final Iterator<URI> prefixesIterator;
  private final int maxListingLength;

  private ResultSegment<ListBlobItem> result;
  private String currentContainer;
  private String currentPrefix;
  private ResultContinuation continuationToken;
  private CloudBlobHolder currentBlobItem;
  private Iterator<ListBlobItem> blobItemIterator;
  private final AzureAccountConfig config;

  @AssistedInject
  AzureCloudBlobIterator(
      AzureStorage storage,
      ListBlobItemHolderFactory blobItemDruidFactory,
      AzureAccountConfig config,
      @Assisted final Iterable<URI> prefixes,
      @Assisted final int maxListingLength
  )
  {
    this.storage = storage;
    this.blobItemDruidFactory = blobItemDruidFactory;
    this.config = config;
    this.prefixesIterator = prefixes.iterator();
    this.maxListingLength = maxListingLength;
    this.result = null;
    this.currentContainer = null;
    this.currentPrefix = null;
    this.continuationToken = null;
    this.currentBlobItem = null;
    this.blobItemIterator = null;

    if (prefixesIterator.hasNext()) {
      prepareNextRequest();
      fetchNextBatch();
      advanceBlobItem();
    }
  }

  @Override
  public boolean hasNext()
  {
    return currentBlobItem != null;
  }

  @Override
  public CloudBlobHolder next()
  {
    if (currentBlobItem == null) {
      throw new NoSuchElementException();
    }

    final CloudBlobHolder retVal = currentBlobItem;
    advanceBlobItem();
    return retVal;
  }

  private void prepareNextRequest()
  {
    URI currentUri = prefixesIterator.next();
    currentContainer = currentUri.getAuthority();
    currentPrefix = AzureUtils.extractAzureKey(currentUri);
    log.debug("currentUri: %s\ncurrentContainer: %s\ncurrentPrefix: %s",
              currentUri, currentContainer, currentPrefix
    );
    result = null;
    continuationToken = null;
  }

  private void fetchNextBatch()
  {
    try {
      log.debug(
          "fetching up to %s resources in container '%s' with prefix '%s'",
          maxListingLength,
          currentContainer,
          currentPrefix
      );
      result = AzureUtils.retryAzureOperation(() -> storage.listBlobsWithPrefixInContainerSegmented(
          currentContainer,
          currentPrefix,
          continuationToken,
          maxListingLength
      ), config.getMaxTries());
      continuationToken = result.getContinuationToken();
      blobItemIterator = result.getResults().iterator();
    }
    catch (Exception e) {
      throw new RE(
          e,
          "Failed to get blob item  from Azure container[%s], prefix[%s]. Error: %s",
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
        ListBlobItemHolder blobItem = blobItemDruidFactory.create(blobItemIterator.next());
        /* skip directory objects */
        if (blobItem.isCloudBlob() && blobItem.getCloudBlob().getBlobLength() > 0) {
          currentBlobItem = blobItem.getCloudBlob();
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
    currentBlobItem = null;
  }
}
