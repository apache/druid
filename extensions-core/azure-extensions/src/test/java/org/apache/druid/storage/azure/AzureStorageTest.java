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
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.collect.ImmutableList;
import org.apache.druid.common.guava.SettableSupplier;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;


// Using Mockito for the whole test class since azure classes (e.g. BlobContainerClient) are final and can't be mocked with EasyMock
public class AzureStorageTest
{

  AzureStorage azureStorage;
  BlobServiceClient blobServiceClient = Mockito.mock(BlobServiceClient.class);
  BlobContainerClient blobContainerClient = Mockito.mock(BlobContainerClient.class);
  AzureClientFactory azureClientFactory = Mockito.mock(AzureClientFactory.class);

  private final String CONTAINER = "container";
  private final String BLOB_NAME = "blobName";
  private final Integer MAX_ATTEMPTS = 3;

  @Before
  public void setup() throws BlobStorageException
  {
    azureStorage = new AzureStorage(azureClientFactory);
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
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getRetriableBlobServiceClient(MAX_ATTEMPTS);

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
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getRetriableBlobServiceClient(0);

    Assert.assertEquals(ImmutableList.of(BLOB_NAME), azureStorage.listDir(CONTAINER, "", null));
  }

  @Test
  public void testListDir_increasedMaxAttempts() throws BlobStorageException
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
    Mockito.doReturn(blobServiceClient).when(azureClientFactory).getRetriableBlobServiceClient(3);

    BlobItem blobItem2 = new BlobItem().setName("blobName2").setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier2 = new SettableSupplier<>();
    supplier2.set(new TestPagedResponse<>(ImmutableList.of(blobItem2)));
    PagedIterable<BlobItem> pagedIterable2 = new PagedIterable<>(supplier2);
    BlobServiceClient blobServiceClient2 = Mockito.mock(BlobServiceClient.class);
    BlobContainerClient blobContainerClient2 = Mockito.mock(BlobContainerClient.class);

    Mockito.doReturn(pagedIterable2).when(blobContainerClient2).listBlobs(
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );

    Mockito.doReturn(blobContainerClient2).when(blobServiceClient2).createBlobContainerIfNotExists(CONTAINER);
    Mockito.doReturn(blobServiceClient2).when(azureClientFactory).getRetriableBlobServiceClient(5);

    Assert.assertEquals(ImmutableList.of(BLOB_NAME), azureStorage.listDir(CONTAINER, "", 3));

    // Should use the same client if maxAttempts has gone down
    Assert.assertEquals(ImmutableList.of(BLOB_NAME), azureStorage.listDir(CONTAINER, "", 2));

    // Should use a new client if maxAttempts has gone up
    Assert.assertEquals(ImmutableList.of("blobName2"), azureStorage.listDir(CONTAINER, "", 5));
  }
}

