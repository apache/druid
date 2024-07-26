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
 * Abstracts the Azure storage layer, wrapping the Azure Java SDK.
 * <p>
 * When using a service client ({@link com.azure.storage.blob.BlobServiceClient}, methods that rely on a container to
 * exist should use {@link com.azure.storage.blob.BlobServiceClient#getBlobContainerClient}.
 * <p>
 * If a method relies on a container to be created if it doesn't exist, call
 * {@link com.azure.storage.blob.BlobServiceClient#createBlobContainerIfNotExists(String)}.
 */
public class AzureStorage
{
  // Default value from Azure library
  private static final int DELTA_BACKOFF_MS = 30_000;

  // https://learn.microsoft.com/en-us/rest/api/storageservices/blob-batch#request-body
  private static final Integer MAX_MULTI_OBJECT_DELETE_SIZE = 256;

  private static final Logger LOG = new Logger(AzureStorage.class);

  private final AzureClientFactory azureClientFactory;
  private final String defaultStorageAccount;

  public AzureStorage(
      final AzureClientFactory azureClientFactory,
      final String defaultStorageAccount
  )
  {
    this.azureClientFactory = azureClientFactory;
    this.defaultStorageAccount = defaultStorageAccount;
  }

  /**
   * See {@link AzureStorage#emptyCloudBlobDirectory(String, String, Integer)} for details.
   */
  public List<String> emptyCloudBlobDirectory(final String containerName, @Nullable final String prefix)
      throws BlobStorageException
  {
    return emptyCloudBlobDirectory(containerName, prefix, null);
  }

  /**
   * Delete all blobs under the given prefix.
   *
   * @param containerName The name of the storage container.
   * @param prefix        (Optional) The Azure storage prefix to delete blobs for.
   *                      If null, deletes all blobs in the storage container.
   * @param maxAttempts   (Optional) Number of attempts to retry in case an API call fails.
   *                      If null, defaults to the system default (`druid.azure.maxTries`).
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
    final PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobs(
        new ListBlobsOptions().setPrefix(prefix),
        Duration.ofMillis(DELTA_BACKOFF_MS)
    );

    final List<String> deletedFiles = new ArrayList<>();
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
   * Creates and opens an output stream to write data to the block blob.
   * <p>
   * If the blob already exists, an exception will be thrown.
   *
   * @param containerName The name of the storage container.
   * @param blobName      The name of the blob within the container.
   * @param blockSize     (Optional) The block size to use when writing the blob.
   *                      If null, the default block size will be used.
   * @param maxAttempts   (Optional) The maximum number of attempts to retry the upload in case of failure.
   *                      If null, the default value from the system configuration (`druid.azure.maxTries`) will be used.
   *
   * @return An OutputStream for writing the blob.
   */
  public OutputStream getBlockBlobOutputStream(
      final String containerName,
      final String blobName,
      @Nullable final Long blockSize,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    final BlockBlobClient blockBlobClient = azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .createBlobContainerIfNotExists(containerName)
        .getBlobClient(Utility.urlEncode(blobName))
        .getBlockBlobClient();

    // TODO based on the usage here, it might be better to overwrite the existing blob instead; that's what StorageConnector#write documents it does
    if (blockBlobClient.exists()) {
      throw new RE("Reference already exists");
    }

    final BlockBlobOutputStreamOptions options = new BlockBlobOutputStreamOptions();
    if (blockSize != null) {
      options.setParallelTransferOptions(new ParallelTransferOptions().setBlockSizeLong(blockSize));
    }

    return blockBlobClient.getBlobOutputStream(options);
  }

  /**
   * Gets the length of the specified block blob.
   *
   * @param containerName The name of the storage container.
   * @param blobName      The name of the blob within the container.
   *
   * @return The length of the blob in bytes.
   */
  public long getBlockBlobLength(final String containerName, final String blobName) throws BlobStorageException
  {
    return azureClientFactory
        .getBlobServiceClient(null, defaultStorageAccount)
        .getBlobContainerClient(containerName)
        .getBlobClient(Utility.urlEncode(blobName))
        .getBlockBlobClient()
        .getProperties()
        .getBlobSize();
  }

  /**
   * See {@link AzureStorage#getBlockBlobInputStream(long, Long, String, String, Integer)} for details.
   */
  public InputStream getBlockBlobInputStream(final String containerName, final String blobName)
      throws BlobStorageException
  {
    return getBlockBlobInputStream(0L, containerName, blobName);
  }

  /**
   * See {@link AzureStorage#getBlockBlobInputStream(long, Long, String, String, Integer)} for details.
   */
  public InputStream getBlockBlobInputStream(final long offset, final String containerName, final String blobName)
      throws BlobStorageException
  {
    return getBlockBlobInputStream(offset, null, containerName, blobName);
  }

  /**
   * See {@link AzureStorage#getBlockBlobInputStream(long, Long, String, String, Integer)} for details.
   */
  public InputStream getBlockBlobInputStream(
      final long offset,
      @Nullable final Long length,
      final String containerName,
      final String blobName
  ) throws BlobStorageException
  {
    return getBlockBlobInputStream(offset, length, containerName, blobName, null);
  }

  /**
   * Gets an InputStream for reading the contents of the specified block blob.
   *
   * @param containerName The name of the storage container.
   * @param blobName      The name of the blob within the container.
   *
   * @return An InputStream for reading the blob.
   */
  public InputStream getBlockBlobInputStream(
      final long offset,
      @Nullable final Long length,
      final String containerName,
      final String blobName,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    return azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .getBlobContainerClient(containerName)
        .getBlobClient(Utility.urlEncode(blobName))
        .openInputStream(new BlobInputStreamOptions().setRange(new BlobRange(offset, length)));
  }

  /**
   * Deletes multiple files from the specified container.
   *
   * @param containerName The name of the storage container.
   * @param paths         An iterable of file paths to be deleted.
   * @param maxAttempts   (Optional) Number of attempts to retry in case an API call fails.
   *                      If null, defaults to the system default (`druid.azure.maxTries`).
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

        // We have to call forEach on the response because this is the only way azure batch will throw an exception on an operation failure.
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

  /**
   * See {@link AzureStorage#getBlockBlobExists(String, String, Integer)} for details.
   */
  public boolean getBlockBlobExists(final String container, final String blobName) throws BlobStorageException
  {
    return getBlockBlobExists(container, blobName, null);
  }

  /**
   * Checks if the specified block blob exists in the given Azure Blob Storage container.
   *
   * @param container   The name of the Azure Blob Storage container.
   * @param blobName    The name of the blob within the container.
   * @param maxAttempts (Optional) The maximum number of attempts to retry the existence check in case of failure.
   *                    If null, the default value from the system configuration (`druid.azure.maxTries`) will be used.
   * @return `true` if the block blob exists, `false` otherwise.
   * @throws BlobStorageException If there is an error checking the existence of the blob.
   */
  public boolean getBlockBlobExists(
      final String container,
      final String blobName,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    return azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .getBlobContainerClient(container)
        .getBlobClient(Utility.urlEncode(blobName))
        .exists();
  }

  /**
   * Lists the blobs in the specified storage container, optionally matching a given prefix.
   *
   * @param storageAccount The name of the storage account.
   * @param containerName  The name of the storage container.
   * @param prefix         (Optional) The Azure storage prefix to list blobs for.
   *                       If null, lists all blobs in the storage container.
   * @param maxResults     (Optional) The maximum number of results to return per page.
   *                       If null, defaults to the API default (5000).
   * @param maxAttempts    (Optional) The maximum number of attempts to retry the list operation in case of failure.
   *                       If null, the default value from the system configuration (`druid.azure.maxTries`) will be used.
   *
   * @return Returns a lazy loaded list of blobs in this container.
   */
  public PagedIterable<BlobItem> listBlobsWithPrefixInContainerSegmented(
      final String storageAccount,
      final String containerName,
      @Nullable final String prefix,
      @Nullable final Integer maxResults,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    final ListBlobsOptions listOptions = new ListBlobsOptions();

    if (maxResults != null) {
      listOptions.setMaxResultsPerPage(maxResults);
    }

    if (prefix != null) {
      listOptions.setPrefix(prefix);
    }

    return azureClientFactory
        .getBlobServiceClient(maxAttempts, storageAccount)
        .getBlobContainerClient(containerName)
        .listBlobs(listOptions, Duration.ofMillis(DELTA_BACKOFF_MS));
  }

  /**
   * Lists the blobs in the specified storage container, optionally matching a given prefix.
   *
   * @param containerName The name of the storage container.
   * @param prefix        (Optional) The Azure storage prefix to list blobs for.
   *                      If null, lists all blobs in the storage container.
   * @param maxResults    (Optional) The maximum number of results to return per page.
   *                      If null, defaults to the API default (5000).
   * @param maxAttempts   (Optional) The maximum number of attempts to retry the list operation in case of failure.
   *                      If null, the default value from the system configuration (`druid.azure.maxTries`) will be used.
   *
   * @return A list of blob names in this container.
   */
  public List<String> listBlobs(
      final String containerName,
      @Nullable final String prefix,
      @Nullable final Integer maxResults,
      @Nullable final Integer maxAttempts
  ) throws BlobStorageException
  {
    final ListBlobsOptions listOptions = new ListBlobsOptions().setPrefix(prefix);

    if (maxResults != null) {
      listOptions.setMaxResultsPerPage(maxResults);
    }

    if (prefix != null) {
      listOptions.setPrefix(prefix);
    }

    final PagedIterable<BlobItem> blobItems = azureClientFactory
        .getBlobServiceClient(maxAttempts, defaultStorageAccount)
        .getBlobContainerClient(containerName)
        .listBlobs(listOptions, Duration.ofMillis(DELTA_BACKOFF_MS));

    final List<String> files = new ArrayList<>();
    blobItems.iterableByPage().forEach(page -> page.getElements().forEach(blob -> files.add(blob.getName())));

    return files;
  }

  /**
   * Creates a new blob, or updates the content of an existing blob.
   *
   * @param file          The file to write to the blob.
   * @param containerName The name of the storage container.
   * @param blobName      The blob name to write the file to.
   * @param maxAttempts   (Optional) Number of attempts to retry in case an API call fails.
   *                      If null, defaults to the system default (`druid.azure.maxTries`).
   */
  public void uploadBlockBlob(
      final File file,
      final String containerName,
      final String blobName,
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
          .getBlobClient(Utility.urlEncode(blobName))

          // Set overwrite to true to keep behavior more similar to s3Client.putObject.
          .upload(stream, file.length(), true);
    }
  }
}
