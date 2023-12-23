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

import com.google.common.collect.ImmutableList;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.ListBlobItem;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;

public class AzureStorageTest
{

  AzureStorage azureStorage;
  CloudBlobClient cloudBlobClient = Mockito.mock(CloudBlobClient.class);
  CloudBlobContainer cloudBlobContainer = Mockito.mock(CloudBlobContainer.class);

  @Before
  public void setup() throws URISyntaxException, StorageException
  {
    Mockito.doReturn(cloudBlobContainer).when(cloudBlobClient).getContainerReference(ArgumentMatchers.anyString());
    azureStorage = new AzureStorage(() -> cloudBlobClient);
  }

  @Test
  public void testListDir() throws URISyntaxException, StorageException
  {
    List<ListBlobItem> listBlobItems = ImmutableList.of(
        new CloudBlockBlob(new URI("azure://dummy.com/container/blobName"))
    );

    Mockito.doReturn(listBlobItems).when(cloudBlobContainer).listBlobs(
        ArgumentMatchers.anyString(),
        ArgumentMatchers.anyBoolean(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any(),
        ArgumentMatchers.any()

    );
    Assert.assertEquals(ImmutableList.of("blobName"), azureStorage.listDir("test", ""));

  }
}

