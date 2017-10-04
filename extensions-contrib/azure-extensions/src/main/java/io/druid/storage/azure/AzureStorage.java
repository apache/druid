/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.azure;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import io.druid.java.util.common.logger.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class AzureStorage
{

  private final Logger log = new Logger(AzureStorage.class);

  private final CloudBlobClient cloudBlobClient;

  public AzureStorage(
      CloudBlobClient cloudBlobClient
  )
  {
    this.cloudBlobClient = cloudBlobClient;
  }

  public CloudBlobContainer getCloudBlobContainer(final String containerName)
      throws StorageException, URISyntaxException
  {
    CloudBlobContainer cloudBlobContainer = cloudBlobClient.getContainerReference(containerName);
    cloudBlobContainer.createIfNotExists();

    return cloudBlobContainer;
  }

  public List<String> emptyCloudBlobDirectory(final String containerName, final String virtualDirPath)
      throws StorageException, URISyntaxException
  {
    List<String> deletedFiles = new ArrayList<>();
    CloudBlobContainer container = getCloudBlobContainer(containerName);

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
    CloudBlobContainer container = getCloudBlobContainer(containerName);
    try (FileInputStream stream = new FileInputStream(file)) {
      CloudBlockBlob blob = container.getBlockBlobReference(blobPath);
      blob.upload(stream, file.length());
    }
  }

  public CloudBlockBlob getBlob(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getCloudBlobContainer(containerName).getBlockBlobReference(blobPath);
  }

  public long getBlobLength(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getCloudBlobContainer(containerName).getBlockBlobReference(blobPath).getProperties().getLength();
  }

  public InputStream getBlobInputStream(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    CloudBlobContainer container = getCloudBlobContainer(containerName);
    return container.getBlockBlobReference(blobPath).openInputStream();
  }

  public boolean getBlobExists(String container, String blobPath) throws URISyntaxException, StorageException
  {
    return getCloudBlobContainer(container).getBlockBlobReference(blobPath).exists();
  }
}
