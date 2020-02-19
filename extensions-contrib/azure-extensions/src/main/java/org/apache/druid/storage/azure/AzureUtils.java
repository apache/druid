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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.microsoft.azure.storage.StorageException;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.RetryUtils.Task;
import org.apache.druid.java.util.common.StringUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Utility class for miscellaneous things involving Azure.
 */
public class AzureUtils
{

  @VisibleForTesting
  static final String AZURE_STORAGE_HOST_ADDRESS = "blob.core.windows.net";

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

  /**
   * extracts the path component of the supplied uri with any leading '/' characters removed.
   * @param uri the uri to extract the path for
   * @return a String representing the path component of the uri with any leading '/'
   * characters removed.
   */
  public static String extractAzureKey(URI uri)
  {
    return StringUtils.maybeRemoveLeadingSlash(uri.getPath());
  }

  /**
   * extracts the blob path component of the supplied uri with any leading 'blob.core.windows.net/' string removed.
   * @param blobPath the path of the blob
   * @return a String representing the blob path component of the uri with any leading 'blob.core.windows.net/' string
   * removed characters removed.
   */
  public static String maybeRemoveAzurePathPrefix(String blobPath)
  {
    boolean blobPathIsHadoop = blobPath.contains(AZURE_STORAGE_HOST_ADDRESS);

    if (blobPathIsHadoop) {
      // Remove azure's hadoop prefix to match realtime ingestion path
      return blobPath.substring(
          blobPath.indexOf(AZURE_STORAGE_HOST_ADDRESS) + AZURE_STORAGE_HOST_ADDRESS.length() + 1);
    } else {
      return blobPath;
    }
  }

  static <T> T retryAzureOperation(Task<T> f, int maxTries) throws Exception
  {
    return RetryUtils.retry(f, AZURE_RETRY, maxTries);
  }
}
