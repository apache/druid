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
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Predicate;
import org.apache.druid.data.input.google.GoogleCloudStorageInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.RetryUtils;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

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

  static <T> T retryGoogleCloudStorageOperation(RetryUtils.Task<T> f) throws Exception
  {
    return RetryUtils.retry(f, GOOGLE_RETRY, RetryUtils.DEFAULT_MAX_TRIES);
  }

  public static URI objectToUri(StorageObject object)
  {
    return objectToCloudObjectLocation(object).toUri(GoogleCloudStorageInputSource.SCHEME);
  }

  public static CloudObjectLocation objectToCloudObjectLocation(StorageObject object)
  {
    return new CloudObjectLocation(object.getBucket(), object.getName());
  }

  public static Iterator<StorageObject> lazyFetchingStorageObjectsIterator(
      final GoogleStorage storage,
      final Iterator<URI> uris,
      final long maxListingLength
  )
  {
    return new ObjectStorageIterator(storage, uris, maxListingLength);
  }
}
