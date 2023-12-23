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
import com.google.common.base.Supplier;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.ResultSegment;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobDeleteBatchOperation;
import com.microsoft.azure.storage.blob.BlobListingDetails;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlob;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.DeleteSnapshotsOption;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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

  // Default value from Azure library
  private static final int DELTA_BACKOFF_MS = 30_000;

  private static final Logger log = new Logger(AzureStorage.class);

  /**
   * Some segment processing tools such as DataSegmentKiller are initialized when an ingestion job starts
   * if the extension is loaded, even when the implementation of DataSegmentKiller is not used. As a result,
   * if we have a CloudBlobClient instead of a supplier of it, it can cause unnecessary config validation
   * against Azure storage even when it's not used at all. To perform the config validation
   * only when it is actually used, we use a supplier.
   *
   * See OmniDataSegmentKiller for how DataSegmentKillers are initialized.
   */
  private final Supplier<CloudBlobClient> cloudBlobClient;

  public AzureStorage(
      Supplier<CloudBlobClient> cloudBlobClient
  )
  {
    this.cloudBlobClient = cloudBlobClient;
  }

  public List<String> emptyCloudBlobDirectory(final String containerName, final String virtualDirPath)
      throws StorageException, URISyntaxException
  {
    return emptyCloudBlobDirectory(containerName, virtualDirPath, null);
  }

  public List<String> emptyCloudBlobDirectory(final String containerName, final String virtualDirPath, final Integer maxAttempts)
      throws StorageException, URISyntaxException
  {
    List<String> deletedFiles = new ArrayList<>();
    CloudBlobContainer container = getOrCreateCloudBlobContainer(containerName);

    Iterable<ListBlobItem> blobItems = container.listBlobs(
        virtualDirPath,
        USE_FLAT_BLOB_LISTING,
        null,
        getRequestOptionsWithRetry(maxAttempts),
        null
    );

    for (ListBlobItem blobItem : blobItems) {
      CloudBlob cloudBlob = (CloudBlob) blobItem;
      log.debug("Removing file[%s] from Azure.", cloudBlob.getName());
      if (cloudBlob.deleteIfExists(DeleteSnapshotsOption.NONE, null, getRequestOptionsWithRetry(maxAttempts), null)) {
        deletedFiles.add(cloudBlob.getName());
      }
    }

    if (deletedFiles.isEmpty()) {
      log.warn("No files were deleted on the following Azure path: [%s]", virtualDirPath);
    }

    return deletedFiles;
  }

  public void uploadBlockBlob(final File file, final String containerName, final String blobPath)
      throws IOException, StorageException, URISyntaxException
  {
    CloudBlobContainer container = getOrCreateCloudBlobContainer(containerName);
    try (FileInputStream stream = new FileInputStream(file)) {
      container.getBlockBlobReference(blobPath).upload(stream, file.length());
    }
  }

  public OutputStream getBlockBlobOutputStream(
      final String containerName,
      final String blobPath,
      @Nullable final Integer streamWriteSizeBytes,
      Integer maxAttempts
  ) throws URISyntaxException, StorageException
  {
    CloudBlobContainer container = getOrCreateCloudBlobContainer(containerName);
    CloudBlockBlob blockBlobReference = container.getBlockBlobReference(blobPath);

    if (blockBlobReference.exists()) {
      throw new RE("Reference already exists");
    }

    if (streamWriteSizeBytes != null) {
      blockBlobReference.setStreamWriteSizeInBytes(streamWriteSizeBytes);
    }

    return blockBlobReference.openOutputStream(null, getRequestOptionsWithRetry(maxAttempts), null);

  }

  public CloudBlob getBlockBlobReferenceWithAttributes(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    final CloudBlockBlob blobReference = getOrCreateCloudBlobContainer(containerName).getBlockBlobReference(blobPath);
    blobReference.downloadAttributes();
    return blobReference;
  }

  public long getBlockBlobLength(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getBlockBlobReferenceWithAttributes(containerName, blobPath).getProperties().getLength();
  }

  public InputStream getBlockBlobInputStream(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getBlockBlobInputStream(0L, containerName, blobPath);
  }

  public InputStream getBlockBlobInputStream(long offset, final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getBlockBlobInputStream(offset, null, containerName, blobPath);
  }

  public InputStream getBlockBlobInputStream(long offset, Long length, final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getBlockBlobInputStream(offset, length, containerName, blobPath, null);
  }

  public InputStream getBlockBlobInputStream(long offset, Long length, final String containerName, final String blobPath, Integer maxAttempts)
      throws URISyntaxException, StorageException
  {
    CloudBlobContainer container = getOrCreateCloudBlobContainer(containerName);
    return container.getBlockBlobReference(blobPath)
                    .openInputStream(offset, length, null, getRequestOptionsWithRetry(maxAttempts), null);
  }

  public void batchDeleteFiles(String containerName, Iterable<String> paths, Integer maxAttempts)
      throws URISyntaxException, StorageException
  {
    CloudBlobContainer cloudBlobContainer = getOrCreateCloudBlobContainer(containerName);
    BlobDeleteBatchOperation blobDeleteBatchOperation = new BlobDeleteBatchOperation();
    for (String path : paths) {
      CloudBlob blobReference = cloudBlobContainer.getBlockBlobReference(path);
      blobDeleteBatchOperation.addSubOperation(blobReference);
    }
    cloudBlobClient.get().executeBatch(blobDeleteBatchOperation, getRequestOptionsWithRetry(maxAttempts), null);
  }

  public List<String> listDir(final String containerName, final String virtualDirPath)
      throws URISyntaxException, StorageException
  {
    return listDir(containerName, virtualDirPath, null);
  }

  public List<String> listDir(final String containerName, final String virtualDirPath, final Integer maxAttempts)
      throws StorageException, URISyntaxException
  {
    List<String> files = new ArrayList<>();
    CloudBlobContainer container = getOrCreateCloudBlobContainer(containerName);

    for (ListBlobItem blobItem :
        container.listBlobs(virtualDirPath, USE_FLAT_BLOB_LISTING, null, getRequestOptionsWithRetry(maxAttempts), null)) {
      CloudBlob cloudBlob = (CloudBlob) blobItem;
      files.add(cloudBlob.getName());
    }

    return files;
  }

  public boolean getBlockBlobExists(String container, String blobPath) throws URISyntaxException, StorageException
  {
    return getBlockBlobExists(container, blobPath, null);
  }


  public boolean getBlockBlobExists(String container, String blobPath, Integer maxAttempts)
      throws URISyntaxException, StorageException
  {
    return getOrCreateCloudBlobContainer(container).getBlockBlobReference(blobPath)
                                                   .exists(null, getRequestOptionsWithRetry(maxAttempts), null);
  }

  /**
   * If maxAttempts is provided, this method returns request options with retry built in.
   * Retry backoff is exponential backoff, with maxAttempts set to the one provided
   */
  @Nullable
  private BlobRequestOptions getRequestOptionsWithRetry(Integer maxAttempts)
  {
    if (maxAttempts == null) {
      return null;
    }
    BlobRequestOptions requestOptions = new BlobRequestOptions();
    requestOptions.setRetryPolicyFactory(new RetryExponentialRetry(DELTA_BACKOFF_MS, maxAttempts));
    return requestOptions;
  }

  @VisibleForTesting
  CloudBlobClient getCloudBlobClient()
  {
    return this.cloudBlobClient.get();
  }

  @VisibleForTesting
  ResultSegment<ListBlobItem> listBlobsWithPrefixInContainerSegmented(
      final String containerName,
      final String prefix,
      ResultContinuation continuationToken,
      int maxResults
  ) throws StorageException, URISyntaxException
  {
    CloudBlobContainer cloudBlobContainer = cloudBlobClient.get().getContainerReference(containerName);
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
    CloudBlobContainer cloudBlobContainer = cloudBlobClient.get().getContainerReference(containerName);
    cloudBlobContainer.createIfNotExists();

    return cloudBlobContainer;
  }
}
