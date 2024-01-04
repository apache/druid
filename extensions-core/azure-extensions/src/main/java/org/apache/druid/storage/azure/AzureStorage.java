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
import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.blob.models.ParallelTransferOptions;
import com.azure.storage.blob.options.BlobInputStreamOptions;
import com.azure.storage.blob.options.BlockBlobOutputStreamOptions;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.azure.storage.common.Utility;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.logger.Logger;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * Abstracts the Azure storage layer. Makes direct calls to Azure file system.
 */
public class AzureStorage
{

  // Default value from Azure library
  private static final int DELTA_BACKOFF_MS = 30_000;

  private static final Logger log = new Logger(AzureStorage.class);

  private final AzureClientFactory azureClientFactory;

  public AzureStorage(
      AzureClientFactory azureClientFactory
  )
  {
    this.azureClientFactory = azureClientFactory;
  }

  public List<String> emptyCloudBlobDirectory(final String containerName, final String virtualDirPath)
      throws BlobStorageException
  {
    return emptyCloudBlobDirectory(containerName, virtualDirPath, null);
  }

  public List<String> emptyCloudBlobDirectory(final String containerName, final String virtualDirPath, final Integer maxAttempts)
      throws BlobStorageException
  {
    List<String> deletedFiles = new ArrayList<>();
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName, maxAttempts);

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

  public void uploadBlockBlob(final File file, final String containerName, final String blobPath, final Integer maxAttempts)
      throws IOException, BlobStorageException
  {
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName, maxAttempts);

    try (FileInputStream stream = new FileInputStream(file)) {
      // By default this creates a Block blob, no need to use a specific Block blob client.
      // We also need to urlEncode the path to handle special characters.
      blobContainerClient.getBlobClient(Utility.urlEncode(blobPath)).upload(stream, file.length());
    }
  }

  public OutputStream getBlockBlobOutputStream(
      final String containerName,
      final String blobPath,
      @Nullable final Integer streamWriteSizeBytes,
      Integer maxAttempts
  ) throws BlobStorageException
  {
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName, maxAttempts);
    BlockBlobClient blockBlobClient = blobContainerClient.getBlobClient(Utility.urlEncode(blobPath)).getBlockBlobClient();

    if (blockBlobClient.exists()) {
      throw new RE("Reference already exists");
    }
    BlockBlobOutputStreamOptions options = new BlockBlobOutputStreamOptions();
    if (streamWriteSizeBytes != null) {
      options.setParallelTransferOptions(new ParallelTransferOptions().setBlockSizeLong(streamWriteSizeBytes.longValue()));
    }
    return blockBlobClient.getBlobOutputStream(options);
  }

  // There's no need to download attributes with the new azure clients, they will get fetched as needed.
  public BlockBlobClient getBlockBlobReferenceWithAttributes(final String containerName, final String blobPath)
      throws BlobStorageException
  {
    return getOrCreateBlobContainerClient(containerName).getBlobClient(Utility.urlEncode(blobPath)).getBlockBlobClient();
  }

  public long getBlockBlobLength(final String containerName, final String blobPath)
      throws BlobStorageException
  {
    return getBlockBlobReferenceWithAttributes(containerName, blobPath).getProperties().getBlobSize();
  }

  public InputStream getBlockBlobInputStream(final String containerName, final String blobPath)
      throws BlobStorageException
  {
    return getBlockBlobInputStream(0L, containerName, blobPath);
  }

  public InputStream getBlockBlobInputStream(long offset, final String containerName, final String blobPath)
      throws BlobStorageException
  {
    return getBlockBlobInputStream(offset, null, containerName, blobPath);
  }

  public InputStream getBlockBlobInputStream(long offset, Long length, final String containerName, final String blobPath)
      throws BlobStorageException
  {
    return getBlockBlobInputStream(offset, length, containerName, blobPath, null);
  }

  public InputStream getBlockBlobInputStream(long offset, Long length, final String containerName, final String blobPath, Integer maxAttempts)
      throws BlobStorageException
  {
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName, maxAttempts);
    return blobContainerClient.getBlobClient(Utility.urlEncode(blobPath)).openInputStream(new BlobInputStreamOptions().setRange(new BlobRange(offset, length)));
  }

  public void batchDeleteFiles(String containerName, Iterable<String> paths, Integer maxAttempts)
      throws BlobBatchStorageException
  {

    BlobBatchClient blobBatchClient = new BlobBatchClientBuilder(getOrCreateBlobContainerClient(containerName, maxAttempts)).buildClient();
    blobBatchClient.deleteBlobs(Lists.newArrayList(paths), DeleteSnapshotsOptionType.ONLY);
  }

  public List<String> listDir(final String containerName, final String virtualDirPath, final Integer maxAttempts)
      throws BlobStorageException
  {
    List<String> files = new ArrayList<>();
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName, maxAttempts);

    PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(
        new ListBlobsOptions().setPrefix(virtualDirPath),
        Duration.ofMillis(DELTA_BACKOFF_MS)
    );

    blobItems.iterableByPage().forEach(page -> page.getElements().forEach(blob -> files.add(blob.getName())));

    return files;
  }

  public boolean getBlockBlobExists(String container, String blobPath) throws BlobStorageException
  {
    return getBlockBlobExists(container, blobPath, null);
  }


  public boolean getBlockBlobExists(String container, String blobPath, Integer maxAttempts)
      throws BlobStorageException
  {
    return getOrCreateBlobContainerClient(container, maxAttempts).getBlobClient(Utility.urlEncode(blobPath)).exists();
  }

  @VisibleForTesting
  BlobServiceClient getBlobServiceClient(Integer maxAttempts)
  {
    return azureClientFactory.getBlobServiceClient(maxAttempts);
  }

  @VisibleForTesting
  PagedIterable<BlobItem> listBlobsWithPrefixInContainerSegmented(
      final String containerName,
      final String prefix,
      int maxResults,
      Integer maxAttempts
  ) throws BlobStorageException
  {
    BlobContainerClient blobContainerClient = getOrCreateBlobContainerClient(containerName, maxAttempts);
    return blobContainerClient.listBlobs(
        new ListBlobsOptions().setPrefix(prefix).setMaxResultsPerPage(maxResults),
        Duration.ofMillis(DELTA_BACKOFF_MS)
    );
  }

  private BlobContainerClient getOrCreateBlobContainerClient(final String containerName)
  {
    return getBlobServiceClient(null).createBlobContainerIfNotExists(containerName);
  }

  private BlobContainerClient getOrCreateBlobContainerClient(final String containerName, final Integer maxRetries)
  {
    return getBlobServiceClient(maxRetries).createBlobContainerIfNotExists(containerName);
  }
}
