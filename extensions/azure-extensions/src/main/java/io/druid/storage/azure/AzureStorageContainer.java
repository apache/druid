/*
 * Druid - a distributed column store.
 *  Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package io.druid.storage.azure;

import com.metamx.common.logger.Logger;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlobDirectory;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

public class AzureStorageContainer
{

  private final Logger log = new Logger(AzureStorageContainer.class);

  private final CloudBlobContainer cloudBlobContainer;

  public AzureStorageContainer(
      CloudBlobContainer cloudBlobContainer
  )
  {
    this.cloudBlobContainer = cloudBlobContainer;
  }

  public List<String> emptyCloudBlobDirectory(final String path)
      throws StorageException, URISyntaxException
  {
    List<String> deletedFiles = new ArrayList<>();

    for (ListBlobItem blobItem : cloudBlobContainer.listBlobs(path, true, null, null, null))
    {
      CloudBlob cloudBlob = (CloudBlob) blobItem;
      log.info("Removing file[%s] from Azure.", cloudBlob.getName());
      if (cloudBlob.deleteIfExists()) {
        deletedFiles.add(cloudBlob.getName());
      }
    }

    if (deletedFiles.isEmpty())
    {
      log.warn("No files were deleted on the following Azure path: [%s]", path);
    }

    return deletedFiles;

  }

  public void uploadBlob(final File file, final String destination)
      throws IOException, StorageException, URISyntaxException

  {
    try (FileInputStream stream = new FileInputStream(file)) {
      CloudBlockBlob blob = cloudBlobContainer.getBlockBlobReference(destination);
      blob.upload(stream, file.length());
    }
  }

  public InputStream getBlobInputStream(final String filePath) throws URISyntaxException, StorageException
  {
    return cloudBlobContainer.getBlockBlobReference(filePath).openInputStream();
  }
}
