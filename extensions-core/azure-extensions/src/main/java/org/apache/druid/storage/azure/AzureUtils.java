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
import com.google.common.collect.ImmutableList;
import com.microsoft.azure.storage.StorageException;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.RetryUtils.Task;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.azure.blob.CloudBlobHolder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;

/**
 * Utility class for miscellaneous things involving Azure.
 */
public class AzureUtils
{

  @VisibleForTesting
  static final String AZURE_STORAGE_HOST_ADDRESS = "blob.core.windows.net";

  // The azure storage hadoop access pattern is:
  // wasb[s]://<containername>@<accountname>.blob.core.windows.net/<path>
  // (from https://docs.microsoft.com/en-us/azure/hdinsight/hdinsight-hadoop-use-blob-storage)
  static final String AZURE_STORAGE_HADOOP_PROTOCOL = "wasbs";

  public static final Predicate<Throwable> AZURE_RETRY = e -> {
    if (e == null) {
      return false;
    }
    for (Throwable t = e; t != null; t = t.getCause()) {
      if (t instanceof URISyntaxException) {
        return false;
      }

      if (t instanceof StorageException) {
        return true;
      }

      if (t instanceof IOException) {
        return true;
      }
    }
    return false;
  };

  /**
   * extracts the path component of the supplied uri with any leading '/' characters removed.
   *
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
   *
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

  /**
   * Delete the files from Azure Storage in a specified bucket, matching a specified prefix and filter
   *
   * @param storage Azure Storage client
   * @param config  specifies the configuration to use when finding matching files in Azure Storage to delete
   * @param bucket  Azure Storage bucket
   * @param prefix  the file prefix
   * @param filter  function which returns true if the prefix file found should be deleted and false otherwise.
   * @throws Exception
   */
  public static void deleteObjectsInPath(
      AzureStorage storage,
      AzureInputDataConfig config,
      AzureAccountConfig accountConfig,
      AzureCloudBlobIterableFactory azureCloudBlobIterableFactory,
      String bucket,
      String prefix,
      Predicate<CloudBlobHolder> filter
  )
      throws Exception
  {
    AzureCloudBlobIterable azureCloudBlobIterable =
        azureCloudBlobIterableFactory.create(ImmutableList.of(new CloudObjectLocation(
            bucket,
            prefix
        ).toUri("azure")), config.getMaxListingLength());
    Iterator<CloudBlobHolder> iterator = azureCloudBlobIterable.iterator();

    while (iterator.hasNext()) {
      final CloudBlobHolder nextObject = iterator.next();
      if (filter.apply(nextObject)) {
        deleteBucketKeys(storage, accountConfig.getMaxTries(), nextObject.getContainerName(), nextObject.getName());
      }
    }
  }

  private static void deleteBucketKeys(
      AzureStorage storage,
      int maxTries,
      String bucket,
      String prefix
  ) throws Exception
  {
    AzureUtils.retryAzureOperation(() -> {
      storage.emptyCloudBlobDirectory(bucket, prefix);
      return null;
    }, maxTries);
  }

  static <T> T retryAzureOperation(Task<T> f, int maxTries) throws Exception
  {
    return RetryUtils.retry(f, AZURE_RETRY, maxTries);
  }
}
