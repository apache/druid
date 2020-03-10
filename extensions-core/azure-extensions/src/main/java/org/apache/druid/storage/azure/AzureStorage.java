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
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;

/**
 * Abstracts the Azure storage layer. Makes direct calls to Azure file system.
 */
public class AzureStorage
{
  private static final boolean USE_FLAT_BLOB_LISTING = true;

  private static final Logger log = new Logger(AzureStorage.class);

  private final CloudBlobClient cloudBlobClient;

  public AzureStorage(
      CloudBlobClient cloudBlobClient
  )
  {
    this.cloudBlobClient = cloudBlobClient;
  }

  public List<String> emptyCloudBlobDirectory(final String containerName, final String virtualDirPath)
      throws StorageException, URISyntaxException
  {
    List<String> deletedFiles = new ArrayList<>();
    CloudBlobContainer container = getOrCreateCloudBlobContainer(containerName);

    for (ListBlobItem blobItem : container.listBlobs(virtualDirPath, true, null, null, null)) {
      CloudBlob cloudBlob = (CloudBlob) blobItem;
      log.info("Removing file[%s] from Azure.", cloudBlob.getName());
      if (cloudBlob.deleteIfExists()) {
        deletedFiles.add(cloudBlob.getName());
      }
    }

    if (deletedFiles.isEmpty()) {
      log.warn("No files were deleted on the following Azure path: [%s]", virtualDirPath);
    }

    return deletedFiles;
  }

  public void uploadBlob(final File file, final String containerName, final String blobPath)
      throws IOException, StorageException, URISyntaxException
  {
    CloudBlobContainer container = getOrCreateCloudBlobContainer(containerName);
    try (FileInputStream stream = new FileInputStream(file)) {
      container.getBlockBlobReference(blobPath).upload(stream, file.length());
    }
  }

  public long getBlobLength(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getOrCreateCloudBlobContainer(containerName).getBlockBlobReference(blobPath).getProperties().getLength();
  }

  public InputStream getBlobInputStream(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getBlobInputStream(0L, containerName, blobPath);
  }

  public InputStream getBlobInputStream(long offset, final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    CloudBlobContainer container = getOrCreateCloudBlobContainer(containerName);
    return container.getBlockBlobReference(blobPath).openInputStream(offset, null, null, null, null);
  }

  public boolean getBlobExists(String container, String blobPath) throws URISyntaxException, StorageException
  {
    return getOrCreateCloudBlobContainer(container).getBlockBlobReference(blobPath).exists();
  }

  @VisibleForTesting
  CloudBlobClient getCloudBlobClient()
  {
    return this.cloudBlobClient;
  }

  @VisibleForTesting
  ResultSegment<ListBlobItem> listBlobsWithPrefixInContainerSegmented(
      final String containerName,
      final String prefix,
      ResultContinuation continuationToken,
      int maxResults
  ) throws StorageException, URISyntaxException
  {
    CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(containerName);
    return cloudBlobContainer
        .listBlobsSegmented(
            prefix,
            /* Use flat blob listing here so that we get only blob types and not directories.*/
            USE_FLAT_BLOB_LISTING,
            EnumSet
                .noneOf(BlobListingDetails.class),
            maxResults,
            continuationToken,
            null,
            null
        );
  }

  private CloudBlobContainer getOrCreateCloudBlobContainer(final String containerName)
      throws StorageException, URISyntaxException
  {
    CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(containerName);
    cloudBlobContainer.createIfNotExists();

    return cloudBlobContainer;
  }
}
