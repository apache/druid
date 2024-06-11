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
import com.azure.core.http.rest.PagedResponse;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.azure.storage.blob.specialized.BlockBlobAsyncClient;
import com.azure.storage.blob.specialized.BlockBlobClient;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.java.util.common.RE;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Using Mockito for the whole test class since Azure classes (e.g. BlobContainerClient) are final
 * and can't be mocked with EasyMock.
 */
@ExtendWith(MockitoExtension.class)
public class AzureStorageTest
{
  private final String STORAGE_ACCOUNT = "storageAccount";
  private final String CONTAINER = "container";
  private final String BLOB_NAME = "blobName";

  private AzureStorage azureStorage;

  @Mock
  private AzureClientFactory azureClientFactory;
  @Mock
  private BlockBlobClient blockBlobClient;
  @Mock
  private BlobClient blobClient;
  @Mock
  private BlobContainerClient blobContainerClient;
  @Mock
  private BlobServiceClient blobServiceClient;

  @BeforeEach
  public void setup() throws BlobStorageException
  {
    azureStorage = new AzureStorage(azureClientFactory, STORAGE_ACCOUNT);
  }

  @ParameterizedTest
  @ValueSource(longs = {-1, BlockBlobAsyncClient.MAX_STAGE_BLOCK_BYTES_LONG + 1})
  public void testGetBlockBlockOutputStream_blockSizeOutOfBoundsException(final long blockSize)
  {
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);
    Mockito.doReturn(blobClient).when(blobContainerClient).getBlobClient(BLOB_NAME);
    Mockito.doReturn(blockBlobClient).when(blobClient).getBlockBlobClient();

    final IllegalArgumentException exception = assertThrows(
        IllegalArgumentException.class,
        () -> azureStorage.getBlockBlobOutputStream(
            CONTAINER,
            BLOB_NAME,
            blockSize,
            null
        )
    );

    assertEquals(
        "The value of the parameter 'blockSize' should be between 1 and 4194304000.",
        exception.getMessage()
    );
  }

  @Test
  public void testGetBlockBlockOutputStream_blobAlreadyExistsException()
  {
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);
    Mockito.doReturn(blobClient).when(blobContainerClient).getBlobClient(BLOB_NAME);
    Mockito.doReturn(blockBlobClient).when(blobClient).getBlockBlobClient();
    Mockito.doReturn(true).when(blockBlobClient).exists();

    final RE exception = assertThrows(
        RE.class,
        () -> azureStorage.getBlockBlobOutputStream(
            CONTAINER,
            BLOB_NAME,
            100L,
            null
        )
    );

    assertEquals(
        "Reference already exists",
        exception.getMessage()
    );
  }

  @ParameterizedTest
  @NullSource
  @ValueSource(longs = {1, 100})
  public void testGetBlockBlockOutputStream_success(@Nullable final Long blockSize)
  {
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);
    Mockito.doReturn(blobClient).when(blobContainerClient).getBlobClient(BLOB_NAME);
    Mockito.doReturn(blockBlobClient).when(blobClient).getBlockBlobClient();

    assertDoesNotThrow(() -> azureStorage.getBlockBlobOutputStream(
        CONTAINER,
        BLOB_NAME,
        blockSize,
        null
    ));
  }

  @Test
  public void testListBlobs_retriable() throws BlobStorageException
  {
    BlobItem blobItem = new BlobItem().setName(BLOB_NAME).setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    Mockito.doReturn(pagedIterable).when(blobContainerClient).listBlobs(
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).getBlobContainerClient(CONTAINER);

    final Integer maxAttempts = 3;
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(maxAttempts, STORAGE_ACCOUNT);

    assertEquals(
        ImmutableList.of(BLOB_NAME),
        azureStorage.listBlobs(CONTAINER, "", null, maxAttempts)
    );
  }

  @Test
  public void testListBlobs_nullMaxAttempts() throws BlobStorageException
  {
    BlobItem blobItem = new BlobItem().setName(BLOB_NAME).setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    Mockito.doReturn(pagedIterable).when(blobContainerClient).listBlobs(
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).getBlobContainerClient(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);

    assertEquals(
        ImmutableList.of(BLOB_NAME),
        azureStorage.listBlobs(CONTAINER, "", null, null)
    );
  }

  @Test
  public void testListBlobsWithPrefixInContainerSegmented() throws BlobStorageException
  {
    String storageAccountCustom = "customStorageAccount";
    BlobItem blobItem = new BlobItem().setName(BLOB_NAME).setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    Mockito.doReturn(pagedIterable).when(blobContainerClient).listBlobs(
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).getBlobContainerClient(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(3, storageAccountCustom);

    azureStorage.listBlobsWithPrefixInContainerSegmented(
        storageAccountCustom,
        CONTAINER,
        "",
        1,
        3
    );
  }

  @Test
  public void testBatchDeleteFiles_emptyResponse() throws BlobStorageException
  {
    String containerUrl = "https://storageaccount.blob.core.windows.net/container";
    BlobBatchClient blobBatchClient = Mockito.mock(BlobBatchClient.class);

    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of()));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);

    ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);

    Mockito.doReturn(containerUrl).when(blobContainerClient).getBlobContainerUrl();
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).getBlobContainerClient(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);
    Mockito.doReturn(blobBatchClient).when(azureClientFactory).getBlobBatchClient(blobContainerClient);
    Mockito.doReturn(pagedIterable).when(blobBatchClient).deleteBlobs(
        captor.capture(), ArgumentMatchers.eq(DeleteSnapshotsOptionType.INCLUDE)
    );

    boolean deleteSuccessful = azureStorage.batchDeleteFiles(CONTAINER, ImmutableList.of(BLOB_NAME), null);
    assertEquals(captor.getValue().get(0), containerUrl + "/" + BLOB_NAME);
    assertTrue(deleteSuccessful);
  }

  @Test
  public void testBatchDeleteFiles_error() throws BlobStorageException
  {
    String containerUrl = "https://storageaccount.blob.core.windows.net/container";
    BlobBatchClient blobBatchClient = Mockito.mock(BlobBatchClient.class);

    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of()));

    ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);

    Mockito.doReturn(containerUrl).when(blobContainerClient).getBlobContainerUrl();
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).getBlobContainerClient(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);
    Mockito.doReturn(blobBatchClient).when(azureClientFactory).getBlobBatchClient(blobContainerClient);
    Mockito.doThrow(new RuntimeException()).when(blobBatchClient).deleteBlobs(
        captor.capture(), ArgumentMatchers.eq(DeleteSnapshotsOptionType.INCLUDE)
    );

    boolean deleteSuccessful = azureStorage.batchDeleteFiles(CONTAINER, ImmutableList.of(BLOB_NAME), null);
    assertEquals(captor.getValue().get(0), containerUrl + "/" + BLOB_NAME);
    assertFalse(deleteSuccessful);
  }

  @Test
  public void testBatchDeleteFiles_emptyResponse_multipleResponses() throws BlobStorageException
  {
    String containerUrl = "https://storageaccount.blob.core.windows.net/container";
    BlobBatchClient blobBatchClient = Mockito.mock(BlobBatchClient.class);

    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of()));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);

    ArgumentCaptor<List<String>> captor = ArgumentCaptor.forClass(List.class);

    Mockito.doReturn(containerUrl).when(blobContainerClient).getBlobContainerUrl();
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).getBlobContainerClient(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);
    Mockito.doReturn(blobBatchClient).when(azureClientFactory).getBlobBatchClient(blobContainerClient);
    Mockito.doReturn(pagedIterable).when(blobBatchClient).deleteBlobs(
        captor.capture(), ArgumentMatchers.eq(DeleteSnapshotsOptionType.INCLUDE)
    );


    List<String> blobNameList = new ArrayList<>();
    for (int i = 0; i <= 257; i++) {
      blobNameList.add(BLOB_NAME + i);
    }

    boolean deleteSuccessful = azureStorage.batchDeleteFiles(CONTAINER, blobNameList, null);

    List<List<String>> deletedValues = captor.getAllValues();
    assertEquals(deletedValues.get(0).size(), 256);
    assertEquals(deletedValues.get(1).size(), 2);
    assertTrue(deleteSuccessful);
  }

  @Test
  public void testUploadBlob_usesOverwrite(@TempDir Path tempPath) throws BlobStorageException, IOException
  {
    final File tempFile = Files.createFile(tempPath.resolve("tempFile.txt")).toFile();
    String blobPath = "blob";

    ArgumentCaptor<InputStream> captor = ArgumentCaptor.forClass(InputStream.class);
    ArgumentCaptor<Long> captor2 = ArgumentCaptor.forClass(Long.class);
    ArgumentCaptor<Boolean> overrideArgument = ArgumentCaptor.forClass(Boolean.class);


    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);
    Mockito.doReturn(blobClient).when(blobContainerClient).getBlobClient(blobPath);
    azureStorage.uploadBlockBlob(tempFile, CONTAINER, blobPath, null);

    Mockito.verify(blobClient).upload(captor.capture(), captor2.capture(), overrideArgument.capture());
    assertTrue(overrideArgument.getValue());
  }
}

