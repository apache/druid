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
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;


@RunWith(EasyMockRunner.class)
public class AzureStorageTest extends EasyMockSupport
{

  AzureStorage azureStorage;
  BlobServiceClient blobServiceClient = Mockito.mock(BlobServiceClient.class);
  BlobContainerClient blobContainerClient = Mockito.mock(BlobContainerClient.class);

  @Mock
  AzureClientFactory azureClientFactory;

  @Before
  public void setup() throws BlobStorageException
  {
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(ArgumentMatchers.anyString());
    azureStorage = new AzureStorage(() -> blobServiceClient, azureClientFactory);
  }

  @Test
  public void testListDir() throws BlobStorageException
  {
    BlobItem blobItem = new BlobItem().setName("blobName").setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    Mockito.doReturn(pagedIterable).when(blobContainerClient).listBlobs(
        ArgumentMatchers.any(),
        ArgumentMatchers.any()

    );
    EasyMock.expect(azureClientFactory.getBlobContainerClient("test", null)).andReturn(blobContainerClient).times(1);

    replayAll();
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir("test", ""));
    verifyAll();
  }

  @Test
  public void testListDir_withMaxAttempts_factoryCreatesNewContainerClient() throws BlobStorageException
  {
    Integer maxAttempts = 5;
    String containerName = "test";
    String containerName2 = "test2";
    BlobItem blobItem = new BlobItem().setName("blobName").setProperties(new BlobItemProperties().setContentLength(10L));
    SettableSupplier<PagedResponse<BlobItem>> supplier = new SettableSupplier<>();
    supplier.set(new TestPagedResponse<>(ImmutableList.of(blobItem)));
    PagedIterable<BlobItem> pagedIterable = new PagedIterable<>(supplier);
    Mockito.doReturn(pagedIterable).when(blobContainerClient).listBlobs(
        ArgumentMatchers.any(),
        ArgumentMatchers.any()
    );
    EasyMock.expect(azureClientFactory.getBlobContainerClient(containerName, maxAttempts)).andReturn(blobContainerClient).times(1);
    EasyMock.expect(azureClientFactory.getBlobContainerClient(containerName2, maxAttempts)).andReturn(blobContainerClient).times(1);
    EasyMock.expect(azureClientFactory.getBlobContainerClient(containerName, maxAttempts + 1)).andReturn(blobContainerClient).times(1);

    replayAll();
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName, "", maxAttempts));
    // The second call should not trigger another call to getBlobContainerClient
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName, "", maxAttempts));
    // Requesting a different container should create another client.
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName2, "", maxAttempts));
    // Requesting the first container with different maxAttempts should create another client.
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName, "", maxAttempts + 1));
    verifyAll();
  }
}

