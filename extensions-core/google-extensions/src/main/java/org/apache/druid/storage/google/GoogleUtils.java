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

import com.google.api.client.http.HttpResponseException;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Objects;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Predicate;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.RetryUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.NoSuchElementException;

public class GoogleUtils
{
  public static final Predicate<Throwable> GOOGLE_RETRY = GoogleUtils::isRetryable;

  public static boolean isRetryable(Throwable t)
  {
    if (t instanceof HttpResponseException) {
      final HttpResponseException e = (HttpResponseException) t;
      return e.getStatusCode() == 429 || (e.getStatusCode() / 500 == 1);
    }
    return t instanceof IOException;
  }

  private static <T> T retryGoogleCloudStorageOperation(RetryUtils.Task<T> f) throws Exception
  {
    return RetryUtils.retry(f, GOOGLE_RETRY, RetryUtils.DEFAULT_MAX_TRIES);
  }

  public static String extractGoogleCloudStorageObjectKey(URI uri)
  {
    return uri.getPath().startsWith("/") ? uri.getPath().substring(1) : uri.getPath();
  }

  public static Iterator<StorageObject> lazyFetchingStorageObjectsIterator(
      final GoogleStorage storage,
      final Iterator<URI> uris,
      final long maxListingLength
  )
  {
    return new Iterator<StorageObject>()
    {
      private Storage.Objects.List listRequest;
      private Objects results;
      private URI currentUri;
      private String currentBucket;
      private String currentPrefix;
      private String nextPageToken;
      private Iterator<StorageObject> storageObjectsIterator;

      {
        nextPageToken = null;
        prepareNextRequest();
        fetchNextBatch();
      }

      private void prepareNextRequest()
      {
        try {
          currentUri = uris.next();
          currentBucket = currentUri.getAuthority();
          currentPrefix = GoogleUtils.extractGoogleCloudStorageObjectKey(currentUri);
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
        return storageObjectsIterator.hasNext() || nextPageToken != null || uris.hasNext();
      }

      @Override
      public StorageObject next()
      {
        if (!hasNext()) {
          throw new NoSuchElementException();
        }

        while (storageObjectsIterator.hasNext()) {
          final StorageObject next = storageObjectsIterator.next();
          // list with prefix can return directories, but they should always end with `/`, ignore them
          if (!next.getName().endsWith("/")) {
            return next;
          }
        }

        if (nextPageToken != null) {
          fetchNextBatch();
        } else if (uris.hasNext()) {
          prepareNextRequest();
          fetchNextBatch();
        }

        if (!storageObjectsIterator.hasNext()) {
          throw new ISE(
              "Failed to further iterate on bucket[%s] and prefix[%s]. The last page token was [%s]",
              currentBucket,
              currentPrefix,
              nextPageToken
          );
        }

        return next();
      }
    };
  }
}
