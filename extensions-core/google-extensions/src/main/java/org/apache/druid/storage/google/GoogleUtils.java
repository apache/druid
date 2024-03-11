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
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
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

  public static <T> T retryGoogleCloudStorageOperation(RetryUtils.Task<T> f) throws Exception
  {
    return RetryUtils.retry(f, GOOGLE_RETRY, RetryUtils.DEFAULT_MAX_TRIES);
  }

  public static URI objectToUri(GoogleStorageObjectMetadata object)
  {
    return objectToCloudObjectLocation(object).toUri(GoogleStorageDruidModule.SCHEME_GS);
  }

  public static CloudObjectLocation objectToCloudObjectLocation(GoogleStorageObjectMetadata object)
  {
    return new CloudObjectLocation(object.getBucket(), object.getName());
  }

  public static Iterator<GoogleStorageObjectMetadata> lazyFetchingStorageObjectsIterator(
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
   *
   * @throws Exception
   */
  public static void deleteObjectsInPath(
      GoogleStorage storage,
      GoogleInputDataConfig config,
      String bucket,
      String prefix,
      Predicate<GoogleStorageObjectMetadata> filter
  )
      throws Exception
  {
    final Iterator<GoogleStorageObjectMetadata> iterator = lazyFetchingStorageObjectsIterator(
        storage,
        ImmutableList.of(new CloudObjectLocation(bucket, prefix).toUri(GoogleStorageDruidModule.SCHEME_GS)).iterator(),
        config.getMaxListingLength()
    );

    while (iterator.hasNext()) {
      final GoogleStorageObjectMetadata nextObject = iterator.next();
      if (filter.apply(nextObject)) {
        retryGoogleCloudStorageOperation(() -> {
          storage.delete(nextObject.getBucket(), nextObject.getName());
          return null;
        });
      }
    }
  }

  /**
   * Similar to {@link org.apache.druid.storage.s3.ObjectSummaryIterator#isDirectoryPlaceholder}
   * Copied to avoid creating dependency on s3 extensions
   */
  public static boolean isDirectoryPlaceholder(final GoogleStorageObjectMetadata objectMetadata)
  {
    // Recognize "standard" directory place-holder indications
    if (objectMetadata.getName().endsWith("/") && objectMetadata.getSize().intValue() == 0) {
      return true;
    }
    // Recognize place-holder objects created by the Google Storage console or S3 Organizer Firefox extension.
    return objectMetadata.getName().endsWith("_$folder$") && objectMetadata.getSize().intValue() == 0;
  }
}
