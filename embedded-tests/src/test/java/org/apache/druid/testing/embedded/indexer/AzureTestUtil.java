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

package org.apache.druid.testing.embedded.indexer;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.indexing.Resources;
import org.apache.druid.testing.tools.ITRetryUtil;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class AzureTestUtil
{
  public static final Logger LOG = new Logger(AzureTestUtil.class);
  private final CloudBlobClient azureStorageClient;
  private final String container;
  private final String cloudPath;

  public AzureTestUtil(CloudBlobClient storageClient, String container, String cloudPath)
  {
    this.azureStorageClient = storageClient;
    this.container = container;
    this.cloudPath = cloudPath;
  }

  public void createStorageContainer() throws URISyntaxException, StorageException
  {
    LOG.info("Creating azure container " + container);
    CloudBlobContainer container = azureStorageClient.getContainerReference(this.container);
    // Create the container if it does not exist.

    // From the azure documentation -
    // When a container is deleted, a container with the same name can't be created for at least 30 seconds.
    // The container might not be available for more than 30 seconds if the service is still processing the request.
    // While the container is being deleted, attempts to create a container of the same name fail with status
    // code 409 (Conflict). The service indicates that the container is being deleted.
    // All other operations, including operations on any blobs under the container,
    // fail with status code 404 (Not Found) while the container is being deleted.
    ITRetryUtil.retryUntil(
        () -> container.createIfNotExists(),
        true,
        10000,
        13,
        "Create Azure container : " + this.container + " "
    );

    LOG.info("Azure container " + this.container + " created");
  }

  public void deleteStorageContainer() throws URISyntaxException, StorageException
  {
    // Retrieve reference to a previously created container.
    CloudBlobContainer container = azureStorageClient.getContainerReference(this.container);
    // Delete the blob container.
    container.deleteIfExists();
  }

  /**
   * Uploads a list of files to s3 at the location set in the IT config
   *
   * @param  filePath path of file to be uploaded
   */
  public void uploadFileToContainer(String filePath) throws IOException, URISyntaxException, StorageException
  {
    // Retrieve reference to a previously created container.
    CloudBlobContainer container = azureStorageClient.getContainerReference(this.container);

    // Create or overwrite the "myimage.jpg" blob with contents from a local file.
    File source = Resources.getFileForResource(filePath);
    CloudBlockBlob blob = container.getBlockBlobReference(cloudPath + '/' + source.getName());
    LOG.info("Uploading file " + cloudPath + '/' + source.getName() + " in azure container " + this.container);
    blob.upload(Files.newInputStream(source.toPath()), source.length());
  }

  /**
   * Get a list of files under a path to be used for verification of kill tasks.
   *
   * @param  filePath path to look for files under
   */
  public List<URI> listFiles(String filePath) throws URISyntaxException, StorageException
  {
    // Retrieve reference to a previously created container.
    CloudBlobContainer container = azureStorageClient.getContainerReference(this.container);
    List<URI> activeFiles = new ArrayList<>();
    container.listBlobs(cloudPath + '/' + filePath).iterator().forEachRemaining(
        blob -> activeFiles.add(blob.getUri())
    );
    return activeFiles;
  }
}
