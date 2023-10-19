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

import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.microsoft.azure.storage.ResultContinuation;
import com.microsoft.azure.storage.RetryExponentialRetry;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.BlobRequestOptions;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
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
  private final Supplier<BlobServiceClient> blobServiceClient;

  private final BlobBatchClient blobBatchClient;

  public AzureStorage(
      Supplier<CloudBlobClient> cloudBlobClient,
      Supplier<BlobServiceClient> blobServiceClient
  )
  {
    this.cloudBlobClient = cloudBlobClient;
    this.blobServiceClient = blobServiceClient;
    this.blobBatchClient = new BlobBatchClientBuilder(blobServiceClient.get()).buildClient();

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
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName);

    // https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-list The new client uses flat listing by default.
    PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(new ListBlobsOptions().setPrefix(virtualDirPath), Duration.ofMillis(DELTA_BACKOFF_MS));

    blobItems.iterableByPage().forEach(page -> {
      page.getElements().forEach(blob -> {
        if (blobContainerClient.getBlobClient(blob.getName()).deleteIfExists()) {
          deletedFiles.add(blob.getName());
        }
      });
    });

    if (deletedFiles.isEmpty()) {
      log.warn("No files were deleted on the following Azure path: [%s]", virtualDirPath);
    }

    return deletedFiles;
  }

  public void uploadBlockBlob(final File file, final String containerName, final String blobPath)
      throws IOException, StorageException, URISyntaxException
  {
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName);

    try (FileInputStream stream = new FileInputStream(file)) {
      // By default this creates a Block blob, no need to use a specific Block blob client.
      blobContainerClient.getBlobClient(blobPath).upload(stream);
    }
  }

  public OutputStream getBlockBlobOutputStream(
      final String containerName,
      final String blobPath,
      @Nullable final Integer streamWriteSizeBytes,
      Integer maxAttempts
  ) throws URISyntaxException, StorageException
  {
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName);
    BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(blobPath).getBlockBlobClient();

    // Can't figure out how to choose chunk size BlobInputStreamOptions
    if (blockBlobClient.exists()) {
      throw new RE("Reference already exists");
    }

    return blockBlobClient.getBlobOutputStream();
  }

  // There's no need to download attributes with the new azure clients, they will get fetched as needed.
  public BlockBlobClient getBlockBlobReferenceWithAttributes(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getOrCreateBlobContainerClient(containerName).getBlobClient(blobPath).getBlockBlobClient();
  }

  public long getBlockBlobLength(final String containerName, final String blobPath)
      throws URISyntaxException, StorageException
  {
    return getBlockBlobReferenceWithAttributes(containerName, blobPath).getProperties().getBlobSize();
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
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName);
    return blobContainerClient.getBlobClient(blobPath).openInputStream(new BlobInputStreamOptions().setRange(new BlobRange(offset, length)));
  }

  public void batchDeleteFiles(String containerName, Iterable<String> paths, Integer maxAttempts)
      throws URISyntaxException, StorageException
  {
    blobBatchClient.deleteBlobs(Lists.newArrayList(paths), DeleteSnapshotsOptionType.ONLY);
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
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName);

    PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(
        new ListBlobsOptions().setPrefix(virtualDirPath),
        Duration.ofMillis(DELTA_BACKOFF_MS)
    );

    blobItems.iterableByPage().forEach(page -> {
      page.getElements().forEach(blob -> {
          files.add(blob.getName());
      });
    });

    return files;
  }

  public boolean getBlockBlobExists(String container, String blobPath) throws URISyntaxException, StorageException
  {
    return getBlockBlobExists(container, blobPath, null);
  }


  public boolean getBlockBlobExists(String container, String blobPath, Integer maxAttempts)
      throws URISyntaxException, StorageException
  {
    return getOrCreateBlobContainerClient(container).getBlobClient(blobPath).exists();
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
  PagedIterable<BlobItem> listBlobsWithPrefixInContainerSegmented(
      final String containerName,
      final String prefix,
      ResultContinuation continuationToken,
      int maxResults
  ) throws StorageException, URISyntaxException
  {
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName);
    return blobContainerClient.listBlobs(
        new ListBlobsOptions().setPrefix(prefix).setMaxResultsPerPage(maxResults),
        Duration.ofMillis(DELTA_BACKOFF_MS)
    );
  }

  private CloudBlobContainer getOrCreateCloudBlobContainer(final String containerName)
      throws StorageException, URISyntaxException
  {
    CloudBlobContainer cloudBlobContainer = cloudBlobClient.get().getContainerReference(containerName);
    cloudBlobContainer.createIfNotExists();

    return cloudBlobContainer;
  }

  private BlobContainerClient getOrCreateBlobContainerClient(final String containerName)
      throws StorageException, URISyntaxException
  {
    BlobContainerClient blobContainerClient = blobServiceClient.get().createBlobContainerIfNotExists(containerName);

    return blobContainerClient;
  }
}
