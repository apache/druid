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
import com.google.common.collect.ImmutableList;
import org.apache.druid.data.input.google.GoogleCloudStorageInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;

public class GoogleUtils
{
  private static final Logger log = new Logger(GoogleUtils.class);
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

  /**
   * Delete the files from Google Storage in a specified bucket, matching a specified prefix and filter
   *
   * @param storage Google Storage client
   * @param config  specifies the configuration to use when finding matching files in Google Storage to delete
   * @param bucket  Google Storage bucket
   * @param prefix  the file prefix
   * @param filter  function which returns true if the prefix file found should be deleted and false otherwise.
   * @throws Exception
   */
  public static void deleteObjectsInPath(
      GoogleStorage storage,
      GoogleInputDataConfig config,
      String bucket,
      String prefix,
      Predicate<StorageObject> filter
  )
      throws Exception
  {
    final Iterator<StorageObject> iterator = lazyFetchingStorageObjectsIterator(
        storage,
        ImmutableList.of(new CloudObjectLocation(bucket, prefix).toUri("gs")).iterator(),
        config.getMaxListingLength()
    );

    while (iterator.hasNext()) {
      final StorageObject nextObject = iterator.next();
      if (filter.apply(nextObject)) {
        retryGoogleCloudStorageOperation(() -> {
          storage.delete(nextObject.getBucket(), nextObject.getName());
          return null;
        });
      }
    }
  }
}
