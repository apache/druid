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

import com.azure.storage.blob.models.BlobItem;
import com.google.inject.assistedinject.Assisted;
import com.google.inject.assistedinject.AssistedInject;
import org.apache.druid.data.input.azure.AzureStorageAccountInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;

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
  private final Iterator<URI> prefixesIterator;
  private final int maxListingLength;
  private AzureStorage storage;
  private String currentStorageAccount;
  private String currentContainer;
  private String currentPrefix;
  private CloudBlobHolder currentBlobItem;
  private Iterator<BlobItem> blobItemIterator;
  private final AzureAccountConfig config;

  @AssistedInject
  AzureCloudBlobIterator(
      @Assisted AzureStorage azureStorage,
      AzureAccountConfig config,
      @Assisted final Iterable<URI> prefixes,
      @Assisted final int maxListingLength
  )
  {
    this.storage = azureStorage;
    this.config = config;
    this.prefixesIterator = prefixes.iterator();
    this.maxListingLength = maxListingLength;
    this.currentStorageAccount = null;
    this.currentContainer = null;
    this.currentPrefix = null;
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

    if (currentUri.getScheme().equals(AzureStorageAccountInputSource.SCHEME)) {
      CloudObjectLocation cloudObjectLocation = new CloudObjectLocation(currentUri);
      Pair<String, String> containerInfo = AzureStorageAccountInputSource.getContainerAndPathFromObjectLocation(cloudObjectLocation);
      currentStorageAccount = cloudObjectLocation.getBucket();
      currentContainer = containerInfo.lhs;
      currentPrefix = containerInfo.rhs;
    } else {
      currentStorageAccount = config.getAccount();
      currentContainer = currentUri.getAuthority();
      currentPrefix = AzureUtils.extractAzureKey(currentUri);
    }
    log.debug("currentUri: %s\ncurrentContainer: %s\ncurrentPrefix: %s",
              currentUri, currentContainer, currentPrefix
    );
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
      // We don't need to iterate by page because the client handles this, it will fetch the next page when necessary.
      blobItemIterator = storage.listBlobsWithPrefixInContainerSegmented(
          currentStorageAccount,
          currentContainer,
          currentPrefix,
          maxListingLength,
          config.getMaxTries()
      ).stream().iterator();
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
    while (prefixesIterator.hasNext() || blobItemIterator.hasNext()) {
      while (blobItemIterator.hasNext()) {
        BlobItem blobItem = blobItemIterator.next();
        if (!blobItem.isPrefix() && blobItem.getProperties().getContentLength() > 0) {
          currentBlobItem = new CloudBlobHolder(blobItem, currentContainer, currentStorageAccount);
          return;
        }
      }
      if (prefixesIterator.hasNext()) {
        prepareNextRequest();
        fetchNextBatch();
      }
    }

    // Truly nothing left to read.
    currentBlobItem = null;
  }
}
