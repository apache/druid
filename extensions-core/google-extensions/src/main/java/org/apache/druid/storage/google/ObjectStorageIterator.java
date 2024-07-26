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

import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class ObjectStorageIterator implements Iterator<GoogleStorageObjectMetadata>
{
  private final GoogleStorage storage;
  private final Iterator<URI> uris;
  private final long maxListingLength;
  private GoogleStorageObjectPage googleStorageObjectPage;
  private URI currentUri;
  private String nextPageToken;
  private Iterator<GoogleStorageObjectMetadata> blobIterator;
  private GoogleStorageObjectMetadata currentObject;

  public ObjectStorageIterator(GoogleStorage storage, Iterator<URI> uris, long maxListingLength)
  {
    this.storage = storage;
    this.uris = uris;
    this.maxListingLength = maxListingLength;

    advanceURI();
    fetchNextPage();
    advanceStorageObject();
  }


  private void advanceURI()
  {
    currentUri = uris.next();
  }

  private void fetchNextPage()
  {
    try {
      String currentBucket = currentUri.getAuthority();
      String currentPrefix = StringUtils.maybeRemoveLeadingSlash(currentUri.getPath());
      googleStorageObjectPage = storage.list(currentBucket, currentPrefix, maxListingLength, nextPageToken);
      blobIterator = googleStorageObjectPage.getObjectList().iterator();
      nextPageToken = googleStorageObjectPage.getNextPageToken();
    }
    catch (IOException io) {
      throw new RuntimeException(io);
    }
  }

  @Override
  public boolean hasNext()
  {
    return currentObject != null;
  }

  @Override
  public GoogleStorageObjectMetadata next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    final GoogleStorageObjectMetadata retVal = currentObject;
    advanceStorageObject();
    return retVal;
  }

  private void advanceStorageObject()
  {
    while (blobIterator.hasNext() || nextPageToken != null || uris.hasNext()) {
      while (blobIterator.hasNext()) {
        final GoogleStorageObjectMetadata next = blobIterator.next();
        // list with prefix can return directories, but they should always end with `/`, ignore them.
        // also skips empty objects.
        if (!next.getName().endsWith("/") && Long.signum(next.getSize()) > 0) {
          currentObject = next;
          return;
        }
      }

      if (nextPageToken != null) {
        fetchNextPage();
      } else if (uris.hasNext()) {
        advanceURI();
        fetchNextPage();
      }
    }

    // Truly nothing left to read.
    currentObject = null;
  }
}
