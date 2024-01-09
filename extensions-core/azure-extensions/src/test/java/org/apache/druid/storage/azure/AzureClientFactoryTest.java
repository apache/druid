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

import com.azure.core.http.policy.AzureSasCredentialPolicy;
import com.azure.core.http.policy.BearerTokenAuthenticationPolicy;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.google.common.collect.ImmutableMap;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.net.MalformedURLException;
import java.net.URL;

public class AzureClientFactoryTest
{
  private AzureClientFactory azureClientFactory;
  private static final String ACCOUNT = "account";

  @Test
  public void test_blobServiceClient_accountName()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    Assert.assertEquals(ACCOUNT, blobServiceClient.getAccountName());
  }

  @Test
  public void test_blobServiceClientBuilder_key() throws MalformedURLException
  {
    AzureAccountConfig config = new AzureAccountConfig();
    config.setKey("key");
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    StorageSharedKeyCredential storageSharedKeyCredential = StorageSharedKeyCredential.getSharedKeyCredentialFromPipeline(
        blobServiceClient.getHttpPipeline()
    );
    Assert.assertNotNull(storageSharedKeyCredential);

    // Azure doesn't let us look at the key in the StorageSharedKeyCredential so make sure the authorization header generated is what we expect.
    Assert.assertEquals(
        new StorageSharedKeyCredential(ACCOUNT, "key").generateAuthorizationHeader(new URL("http://druid.com"), "POST", ImmutableMap.of()),
        storageSharedKeyCredential.generateAuthorizationHeader(new URL("http://druid.com"), "POST", ImmutableMap.of())
    );
  }

  @Test
  public void test_blobServiceClientBuilder_sasToken()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    config.setSharedAccessStorageToken("sasToken");
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    AzureSasCredentialPolicy azureSasCredentialPolicy = null;
    for (int i = 0; i < blobServiceClient.getHttpPipeline().getPolicyCount(); i++) {
      if (blobServiceClient.getHttpPipeline().getPolicy(i) instanceof AzureSasCredentialPolicy) {
        azureSasCredentialPolicy = (AzureSasCredentialPolicy) blobServiceClient.getHttpPipeline().getPolicy(i);
      }
    }

    Assert.assertNotNull(azureSasCredentialPolicy);
  }

  @Test
  public void test_blobServiceClientBuilder_useDefaultCredentialChain()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    config.setUseAzureCredentialsChain(true);
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    BearerTokenAuthenticationPolicy bearerTokenAuthenticationPolicy = null;
    for (int i = 0; i < blobServiceClient.getHttpPipeline().getPolicyCount(); i++) {
      if (blobServiceClient.getHttpPipeline().getPolicy(i) instanceof BearerTokenAuthenticationPolicy) {
        bearerTokenAuthenticationPolicy = (BearerTokenAuthenticationPolicy) blobServiceClient.getHttpPipeline().getPolicy(i);
      }
    }

    Assert.assertNotNull(bearerTokenAuthenticationPolicy);
  }

  @Test
  public void test_blobServiceClientBuilder_useCachedClient()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    config.setUseAzureCredentialsChain(true);
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    BlobServiceClient blobServiceClient2 = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    Assert.assertEquals(blobServiceClient, blobServiceClient2);
  }

  @Test
  public void test_blobServiceClientBuilder_useNewClientForDifferentRetryCount()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    config.setUseAzureCredentialsChain(true);
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    BlobServiceClient blobServiceClient2 = azureClientFactory.getBlobServiceClient(1, ACCOUNT);
    Assert.assertNotEquals(blobServiceClient, blobServiceClient2);
  }

  @Test
  public void test_blobServiceClientBuilder_useAzureAccountConfig_asDefaultMaxTries()
  {
    AzureAccountConfig config = EasyMock.createMock(AzureAccountConfig.class);
    EasyMock.expect(config.getKey()).andReturn("key").times(2);
    EasyMock.expect(config.getMaxTries()).andReturn(3);
    EasyMock.expect(config.getBlobStorageEndpoint()).andReturn(AzureUtils.AZURE_STORAGE_HOST_ADDRESS);
    azureClientFactory = new AzureClientFactory(config);
    EasyMock.replay(config);
    azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    EasyMock.verify(config);
  }
}
