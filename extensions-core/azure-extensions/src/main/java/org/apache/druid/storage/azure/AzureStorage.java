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
import com.azure.storage.blob.batch.BlobBatchClient;
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
import com.google.common.collect.Streams;
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
import java.util.stream.Collectors;

/**
 * Abstracts the Azure storage layer. Makes direct calls to Azure file system.
 */
public class AzureStorage
{
  // Default value from Azure library
  private static final int DELTA_BACKOFF_MS = 30_000;

  // https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch#request-body
  private static final Integer MAX_MULTI_OBJECT_DELETE_SIZE = 256;

  private static final Logger LOG = new Logger(AzureStorage.class);

  private final AzureClientFactory azureClientFactory;
  @Nullable private final String defaultStorageAccount;

  public AzureStorage(
      final AzureClientFactory azureClientFactory,
      @Nullable final String defaultStorageAccount
  )
  {
    this.azureClientFactory = azureClientFactory;
    this.defaultStorageAccount = defaultStorageAccount;
  }

  /**
   * See {@link AzureStorage#emptyCloudBlobDirectory(String, String, Integer)} for details.
   */
  public List<String> emptyCloudBlobDirectory(final String containerName, final String virtualDirPath)
      throws BlobStorageException
  {
    return emptyCloudBlobDirectory(containerName, virtualDirPath, null);
  }

  /**
   * Delete all blobs under the given prefix.
   *
   * @param containerName The storage container name.
   * @param prefix        The Azure storage prefix to delete blobs for.
   *                      If null, deletes all blobs in the storage container.
   * @param maxAttempts   Number of attempts to retry in case an API call fails.
   *                      If null, defaults to the `druid.azure.maxTries` config value.
   *
   * @return The list of blobs deleted.
   */
  public List<String> emptyCloudBlobDirectory(
      final String containerName,
      @Nullable final String prefix,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    final BlobContainerClient blobContainerClient = azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .getBlobContainerClient(containerName);

    // https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-list The new client uses flat listing by default.
    PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(
        new ListBlobsOptions().setPrefix(prefix),
        Duration.ofMillis(DELTA_BACKOFF_MS)
    );

    List<String> deletedFiles = new ArrayList<>();
    blobItems.iterableByPage().forEach(
        page -> page.getElements().forEach(
            blob -> {
              if (blobContainerClient.getBlobClient(blob.getName()).deleteIfExists()) {
                deletedFiles.add(blob.getName());
              }
            }
        )
    );

    if (deletedFiles.isEmpty()) {
      LOG.warn("No files were deleted on the following Azure path: [%s]", prefix);
    }

    return deletedFiles;
  }

  /**
   * Creates a new blob, or updates the content of an existing blob.
   *
   * @param file          The file to write to the blob.
   * @param containerName The storage container name.
   * @param blobPath      The blob path to write the file to.
   * @param maxAttempts   Number of attempts to retry in case an API call fails.
   *                      If null, defaults to the `druid.azure.maxTries` config value.
   */
  public void uploadBlockBlob(
      final File file,
      final String containerName,
      final String blobPath,
      @Nullable final Integer maxAttempts
  ) throws IOException, BlobStorageException
  {
    final BlobContainerClient blobContainerClient = azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .createBlobContainerIfNotExists(containerName);

    try (FileInputStream stream = new FileInputStream(file)) {
      blobContainerClient
          // Creates a blob by default, no need to use a specific blob client.
          // We also need to urlEncode the path to handle special characters.
          .getBlobClient(Utility.urlEncode(blobPath))

          // Set overwrite to true to keep behavior more similar to s3Client.putObject.
          .upload(stream, file.length(), true);
    }
  }

  public OutputStream getBlockBlobOutputStream(
      final String containerName,
      final String blobPath,
      @Nullable final Integer streamWriteSizeBytes,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    final BlockBlobClient blockBlobClient = azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .createBlobContainerIfNotExists(containerName)
        .getBlobClient(Utility.urlEncode(blobPath))
        .getBlockBlobClient();

    if (blockBlobClient.exists()) {
      throw new RE("Reference already exists");
    }

    BlockBlobOutputStreamOptions options = new BlockBlobOutputStreamOptions();
    if (streamWriteSizeBytes != null) {
      options.setParallelTransferOptions(new ParallelTransferOptions().setBlockSizeLong(streamWriteSizeBytes.longValue()));
    }

    return blockBlobClient.getBlobOutputStream(options);
  }

  public long getBlockBlobLength(final String containerName, final String blobPath) throws BlobStorageException
  {
    return azureClientFactory
        .getBlobServiceClient(null, defaultStorageAccount)
        .getBlobContainerClient(containerName)
        .getBlobClient(Utility.urlEncode(blobPath))
        .getBlockBlobClient()
        .getProperties()
        .getBlobSize();
  }

  public InputStream getBlockBlobInputStream(final String containerName, final String blobPath)
      throws BlobStorageException
  {
    return getBlockBlobInputStream(0L, containerName, blobPath);
  }

  public InputStream getBlockBlobInputStream(final long offset, final String containerName, final String blobPath)
      throws BlobStorageException
  {
    return getBlockBlobInputStream(offset, null, containerName, blobPath);
  }

  public InputStream getBlockBlobInputStream(
      final long offset,
      @Nullable final Long length,
      final String containerName,
      final String blobPath
  ) throws BlobStorageException
  {
    return getBlockBlobInputStream(offset, length, containerName, blobPath, null);
  }

  public InputStream getBlockBlobInputStream(
      final long offset,
      @Nullable final Long length,
      final String containerName,
      final String blobPath,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    return azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .getBlobContainerClient(containerName)
        .getBlobClient(Utility.urlEncode(blobPath))
        .openInputStream(new BlobInputStreamOptions().setRange(new BlobRange(offset, length)));
  }

  /**
   * Deletes multiple files from the specified container.
   *
   * @param containerName The name of the container from which files will be deleted.
   * @param paths         An iterable of file paths to be deleted.
   * @param maxAttempts   (Optional) The maximum number of attempts to delete each file.
   *                      If null, the system default number of attempts will be used.
   *
   * @return true if all files were successfully deleted; false otherwise.
   */
  public boolean batchDeleteFiles(
      final String containerName,
      final Iterable<String> paths,
      @Nullable final Integer maxAttempts
  ) throws BlobBatchStorageException
  {
    BlobContainerClient blobContainerClient = azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .getBlobContainerClient(containerName);

    BlobBatchClient blobBatchClient = azureClientFactory.getBlobBatchClient(blobContainerClient);
    List<String> blobUris = Streams.stream(paths)
                                   .map(path -> blobContainerClient.getBlobContainerUrl() + "/" + path)
                                   .collect(Collectors.toList());

    boolean hadException = false;
    List<List<String>> keysChunks = Lists.partition(blobUris, MAX_MULTI_OBJECT_DELETE_SIZE);
    for (List<String> chunkOfKeys : keysChunks) {
      try {
        LOG.info("Removing from container [%s] the following files: [%s]", containerName, chunkOfKeys);

        // We have to call forEach on the response because this is the only way azure batch will throw an exception on a operation failure.
        blobBatchClient.deleteBlobs(chunkOfKeys, DeleteSnapshotsOptionType.INCLUDE).forEach(response -> LOG.debug(
            "Deleting blob with URL %s completed with status code %d%n",
            response.getRequest().getUrl(),
            response.getStatusCode()
        ));
      }
      catch (BlobStorageException | BlobBatchStorageException e) {
        hadException = true;
        LOG.noStackTrace().warn(
            e,
            "Unable to delete from container [%s], the following keys [%s]",
            containerName,
            chunkOfKeys
        );
      }
      catch (Exception e) {
        hadException = true;
        LOG.noStackTrace().warn(
            e,
            "Unexpected exception occurred when deleting from container [%s], the following keys [%s]",
            containerName,
            chunkOfKeys
        );
      }
    }

    return !hadException;
  }

  public List<String> listDir(
      final String containerName,
      final String virtualDirPath,
      @Nullable final Integer maxAttempts
  )
      throws BlobStorageException
  {
    final BlobContainerClient blobContainerClient = azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .getBlobContainerClient(containerName);

    final PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(
        new ListBlobsOptions().setPrefix(virtualDirPath),
        Duration.ofMillis(DELTA_BACKOFF_MS)
    );

    final List<String> files = new ArrayList<>();
    blobItems.iterableByPage().forEach(page -> page.getElements().forEach(blob -> files.add(blob.getName())));

    return files;
  }

  public boolean getBlockBlobExists(final String container, final String blobPath) throws BlobStorageException
  {
    return getBlockBlobExists(container, blobPath, null);
  }

  public boolean getBlockBlobExists(
      final String container,
      final String blobPath,
      @Nullable final Integer maxAttempts
  )
      throws BlobStorageException
  {
    return azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .getBlobContainerClient(container)
        .getBlobClient(Utility.urlEncode(blobPath))
        .exists();
  }

  // This method is used in AzureCloudBlobIterator in a method where one azureStorage instance might need to list from multiple
  // storage accounts, so storageAccount is a valid parameter.
  @VisibleForTesting
  public PagedIterable<BlobItem> listBlobsWithPrefixInContainerSegmented(
      final String storageAccount,
      final String containerName,
      final String prefix,
      final int maxResults,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    return azureClientFactory
        .getBlobServiceClient(maxAttempts, storageAccount)
        .getBlobContainerClient(containerName)
        .listBlobs(
            new ListBlobsOptions().setPrefix(prefix).setMaxResultsPerPage(maxResults),
            Duration.ofMillis(DELTA_BACKOFF_MS)
        );
  }
}
