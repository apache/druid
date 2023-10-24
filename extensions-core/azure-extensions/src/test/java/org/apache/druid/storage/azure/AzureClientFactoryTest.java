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

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import org.junit.Assert;
import org.junit.Test;

public class AzureClientFactoryTest
{
  private AzureClientFactory azureClientFactory;
  private static final String ACCOUNT = "account";
  
  @Test
  public void test_blobServiceClient()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    azureClientFactory = new AzureClientFactory(config);
    config.setAccount(ACCOUNT);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient();
    Assert.assertEquals(ACCOUNT, blobServiceClient.getAccountName());
  }

  @Test
  public void test_blobContainerClient()
  {
    String container = "container";
    AzureAccountConfig config = new AzureAccountConfig();
    azureClientFactory = new AzureClientFactory(config);
    config.setAccount(ACCOUNT);
    BlobContainerClient blobContainerClient = azureClientFactory.getBlobContainerClient(container, null);
    Assert.assertEquals(ACCOUNT, blobContainerClient.getAccountName());
    Assert.assertEquals(container, blobContainerClient.getBlobContainerName());
  }
}
