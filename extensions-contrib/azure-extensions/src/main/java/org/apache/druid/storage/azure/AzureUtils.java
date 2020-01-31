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

import com.google.common.base.Predicate;
import com.microsoft.azure.storage.StorageException;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.RetryUtils.Task;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

public class AzureUtils
{

  public static final Predicate<Throwable> AZURE_RETRY = e -> {
    if (e instanceof URISyntaxException) {
      return false;
    }

    if (e instanceof StorageException) {
      return true;
    }

    if (e instanceof IOException) {
      return true;
    }

    return false;
  };

  public static CloudObjectLocation summaryToCloudObjectLocation(CloudBlobDruid cloudBlob)
  {
    try {
      return new CloudObjectLocation(cloudBlob.getContainer().getName(), cloudBlob.getName());
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create an iterator over a set of S3 objects specified by a set of prefixes.
   * <p>
   * For each provided prefix URI, the iterator will walk through all objects that are in the same bucket as the
   * provided URI and whose keys start with that URI's path, except for directory placeholders (which will be
   * ignored). The iterator is computed incrementally by calling {@link AzureStorage#listBlobsWithPrefixInContainerSegmented} for
   * each prefix in batches of {@param maxListLength}. The first call is made at the same time the iterator is
   * constructed.
   */
  public static Iterator<CloudBlobDruid> blobItemIterator(
      final AzureStorage storage,
      final Iterable<URI> prefixes,
      final int maxListingLength
  )
  {
    return new BlobItemIterator(storage, prefixes, maxListingLength);
  }

  public static String extractAzureKey(URI uri)
  {
    return StringUtils.maybeRemoveLeadingSlash(uri.getPath());
  }

  static <T> T retryAzureOperation(Task<T> f, int maxTries) throws Exception
  {
    return RetryUtils.retry(f, AZURE_RETRY, maxTries);
  }
}
