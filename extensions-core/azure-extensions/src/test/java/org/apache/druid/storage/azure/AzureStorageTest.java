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
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.azure.storage.blob.models.DeleteSnapshotsOptionType;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.SettableSupplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;


// Using Mockito for the whole test class since azure classes (e.g. BlobContainerClient) are final and can't be mocked with EasyMock
public class AzureStorageTest
{

  AzureStorage azureStorage;
  BlobServiceClient blobServiceClient = Mockito.mock(BlobServiceClient.class);
  BlobContainerClient blobContainerClient = Mockito.mock(BlobContainerClient.class);
  AzureClientFactory azureClientFactory = Mockito.mock(AzureClientFactory.class);

  private final String STORAGE_ACCOUNT = "storageAccount";
  private final String CONTAINER = "container";
  private final String BLOB_NAME = "blobName";
  private final Integer MAX_ATTEMPTS = 3;

  @Before
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
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getBlobServiceClient(MAX_ATTEMPTS, STORAGE_ACCOUNT);

    Assert.assertEquals(ImmutableList.of(BLOB_NAME), azureStorage.listDir(CONTAINER, "", MAX_ATTEMPTS));
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

    Assert.assertEquals(ImmutableList.of(BLOB_NAME), azureStorage.listDir(CONTAINER, "", null));
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
    Assert.assertEquals(captor.getValue().get(0), containerUrl + "/" + BLOB_NAME);
    Assert.assertTrue(deleteSuccessful);
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
    Assert.assertEquals(captor.getValue().get(0), containerUrl + "/" + BLOB_NAME);
    Assert.assertFalse(deleteSuccessful);
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
    Assert.assertEquals(deletedValues.get(0).size(), 256);
    Assert.assertEquals(deletedValues.get(1).size(), 2);
    Assert.assertTrue(deleteSuccessful);
  }
}

