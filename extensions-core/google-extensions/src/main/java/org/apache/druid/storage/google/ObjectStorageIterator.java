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

package org.apache.druid.storage.google;

import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ObjectStorageIterator implements Iterator<StorageObject>
{
  private final GoogleStorage storage;
  private final Iterator<URI> uris;
  private final long maxListingLength;

  private Storage.Objects.List listRequest;
  private Objects results;
  private URI currentUri;
  private String nextPageToken;
  private Iterator<StorageObject> storageObjectsIterator;
  private StorageObject currentObject;

  public ObjectStorageIterator(GoogleStorage storage, Iterator<URI> uris, long maxListingLength)
  {
    this.storage = storage;
    this.uris = uris;
    this.maxListingLength = maxListingLength;
    this.nextPageToken = null;

    prepareNextRequest();
    fetchNextBatch();
    advanceStorageObject();
  }

  private void prepareNextRequest()
  {
    try {
      currentUri = uris.next();
      String currentBucket = currentUri.getAuthority();
      String currentPrefix = StringUtils.maybeRemoveLeadingSlash(currentUri.getPath());
      nextPageToken = null;
      listRequest = storage.list(currentBucket)
                           .setPrefix(currentPrefix)
                           .setMaxResults(maxListingLength);

    }
    catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  private void fetchNextBatch()
  {
    try {
      listRequest.setPageToken(nextPageToken);
      results = GoogleUtils.retryGoogleCloudStorageOperation(() -> listRequest.execute());
      storageObjectsIterator = results.getItems().iterator();
      nextPageToken = results.getNextPageToken();
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public boolean hasNext()
  {
    return currentObject != null;
  }

  @Override
  public StorageObject next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final StorageObject retVal = currentObject;
    advanceStorageObject();
    return retVal;
  }

  private void advanceStorageObject()
  {
    while (storageObjectsIterator.hasNext() || nextPageToken != null || uris.hasNext()) {
      while (storageObjectsIterator.hasNext()) {
        final StorageObject next = storageObjectsIterator.next();
        // list with prefix can return directories, but they should always end with `/`, ignore them.
        // also skips empty objects.
        if (!next.getName().endsWith("/") && next.getSize().signum() > 0) {
          currentObject = next;
          return;
        }
      }

      if (nextPageToken != null) {
        fetchNextBatch();
      } else if (uris.hasNext()) {
        prepareNextRequest();
        fetchNextBatch();
      }
    }

    // Truly nothing left to read.
    currentObject = null;
  }
}
