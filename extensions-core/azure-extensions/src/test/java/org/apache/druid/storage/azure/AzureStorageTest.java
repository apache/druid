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

import java.net.URI;


@RunWith(EasyMockRunner.class)
public class AzureStorageTest extends EasyMockSupport
{

  AzureStorage azureStorage;
  BlobServiceClient blobServiceClient = Mockito.mock(BlobServiceClient.class);
  BlobContainerClient blobContainerClient = Mockito.mock(BlobContainerClient.class);

  @Mock
  AzureClientFactory azureClientFactory;

  private final Integer MAX_TRIES = 3;
  private final String ACCOUNT = "storageAccount";

  @Before
  public void setup() throws BlobStorageException
  {
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
    Mockito.doReturn(blobContainerClient).when(blobServiceClient).createBlobContainerIfNotExists(ArgumentMatchers.anyString());

    replayAll();
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir("test", "", null));
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
    Mockito.doReturn(ACCOUNT).when(blobServiceClient).getAccountName();
    EasyMock.expect(azureClientFactory.getBlobContainerClient(ACCOUNT, containerName, maxAttempts)).andReturn(blobContainerClient).times(1);
    EasyMock.expect(azureClientFactory.getBlobContainerClient(ACCOUNT, containerName2, maxAttempts)).andReturn(blobContainerClient).times(1);
    EasyMock.expect(azureClientFactory.getBlobContainerClient(ACCOUNT, containerName, maxAttempts + 1)).andReturn(blobContainerClient).times(1);

    replayAll();
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName, "", maxAttempts));
    // The second call should not trigger another call to getBlobContainerClient
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName, "", maxAttempts));
    // Requesting a different container should create another client.
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName2, "", maxAttempts));
    // Requesting the first container with higher maxAttempts should create another client.
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName, "", maxAttempts + 1));
    // Requesting the first container with lower maxAttempts should not create another client.
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir(containerName, "", maxAttempts - 1));
    verifyAll();
  }

  @Test
  public void testURI()
  {
    URI uri = URI.create("azure://container/prefix1/file.json");
    Assert.assertEquals("container", uri.getHost());
    Assert.assertEquals("/prefix1/file.json", uri.getPath());

    URI uri2 = URI.create("azure://storageAccount/container/prefix1/file.json");
    Assert.assertEquals("storageAccount", uri2.getHost());
    Assert.assertEquals("/container/prefix1/file.json", uri2.getPath());
  }
}

