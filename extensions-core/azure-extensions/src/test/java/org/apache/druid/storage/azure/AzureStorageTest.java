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
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.SettableSupplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;


// Using Mockito for the whole test class since azure classes (e.g. BlobContainerClient) are final and can't be mocked with EasyMock
public class AzureStorageTest
{
  AzureStorage azureStorage;
  BlobClient blobClient = Mockito.mock(BlobClient.class);
  BlobServiceClient blobServiceClient = Mockito.mock(BlobServiceClient.class);
  BlobContainerClient blobContainerClient = Mockito.mock(BlobContainerClient.class);
  AzureClientFactory azureClientFactory = Mockito.mock(AzureClientFactory.class);

  private final String STORAGE_ACCOUNT = "storageAccount";
  private final String CONTAINER = "container";
  private final String BLOB_NAME = "blobName";

  @BeforeEach
  public void setup() throws BlobStorageException
  {
    azureStorage = new AzureStorage(azureClientFactory, STORAGE_ACCOUNT);
  }

  @Test
  public void testListDir_retriable() throws BlobStorageException
  {
    BlobItem blobItem = new BlobItem().setName(BLOB_NAME).setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    Mockito.doReturn(pagedIterable).when(blobContainerClient).listBlobs(
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);

    final Integer maxAttempts = 3;
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(maxAttempts, STORAGE_ACCOUNT);

    assertEquals(ImmutableList.of(BLOB_NAME), azureStorage.listDir(CONTAINER, "", maxAttempts));
  }

  @Test
  public void testListDir_nullMaxAttempts() throws BlobStorageException
  {
    BlobItem blobItem = new BlobItem().setName(BLOB_NAME).setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    Mockito.doReturn(pagedIterable).when(blobContainerClient).listBlobs(
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(null, STORAGE_ACCOUNT);

    assertEquals(ImmutableList.of(BLOB_NAME), azureStorage.listDir(CONTAINER, "", null));
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
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
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
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
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
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
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
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(CONTAINER);
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

