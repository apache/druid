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
import org.apache.druid.data.input.azure.AzureStorageAccountInputSourceConfig;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.MalformedURLException;
import java.net.URL;

@RunWith(EasyMockRunner.class)
public class AzureIngestClientFactoryTest extends EasyMockSupport
{
  private AzureIngestClientFactory azureIngestClientFactory;
  private static final String ACCOUNT = "account";
  private static final String KEY = "key";
  private static final String TOKEN = "token";

  @Mock
  private static AzureAccountConfig accountConfig;

  @Mock
  private static AzureStorageAccountInputSourceConfig azureStorageAccountInputSourceConfig;

  @Before
  public void setup()
  {
    EasyMock.expect(accountConfig.getBlobStorageEndpoint()).andReturn("blob.core.windows.net").anyTimes();
  }

  @Test
  public void test_blobServiceClient_accountName()
  {
    AzureStorageAccountInputSourceConfig azureStorageAccountInputSourceConfig = new AzureStorageAccountInputSourceConfig(
        null,
        KEY,
        null,
        null,
        null
    );
    azureIngestClientFactory = new AzureIngestClientFactory(accountConfig, azureStorageAccountInputSourceConfig);
    replayAll();
    BlobServiceClient blobServiceClient = azureIngestClientFactory.getBlobServiceClient(3, ACCOUNT);
    verifyAll();

    Assert.assertEquals(ACCOUNT, blobServiceClient.getAccountName());
  }

  @Test
  public void test_blobServiceClientBuilder_key() throws MalformedURLException
  {
    AzureStorageAccountInputSourceConfig azureStorageAccountInputSourceConfig = new AzureStorageAccountInputSourceConfig(
        null,
        KEY,
        null,
        null,
        null
    );
    azureIngestClientFactory = new AzureIngestClientFactory(accountConfig, azureStorageAccountInputSourceConfig);

    replayAll();
    BlobServiceClient blobServiceClient = azureIngestClientFactory.getBlobServiceClient(3, ACCOUNT);
    verifyAll();
    StorageSharedKeyCredential storageSharedKeyCredential = StorageSharedKeyCredential.getSharedKeyCredentialFromPipeline(
        blobServiceClient.getHttpPipeline()
    );
    Assert.assertNotNull(storageSharedKeyCredential);

    // Azure doesn't let us look at the key in the StorageSharedKeyCredential so make sure the authorization header generated is what we expect.
    Assert.assertEquals(
        new StorageSharedKeyCredential(ACCOUNT, KEY).generateAuthorizationHeader(new URL("http://druid.com"), "POST", ImmutableMap.of()),
        storageSharedKeyCredential.generateAuthorizationHeader(new URL("http://druid.com"), "POST", ImmutableMap.of())
    );
  }

  @Test
  public void test_blobServiceClientBuilder_sasToken()
  {
    AzureStorageAccountInputSourceConfig azureStorageAccountInputSourceConfig = new AzureStorageAccountInputSourceConfig(
        TOKEN,
        null,
        null,
        null,
        null
    );
    azureIngestClientFactory = new AzureIngestClientFactory(accountConfig, azureStorageAccountInputSourceConfig);
    replayAll();
    BlobServiceClient blobServiceClient = azureIngestClientFactory.getBlobServiceClient(3, ACCOUNT);
    verifyAll();

    AzureSasCredentialPolicy azureSasCredentialPolicy = null;
    for (int i = 0; i < blobServiceClient.getHttpPipeline().getPolicyCount(); i++) {
      if (blobServiceClient.getHttpPipeline().getPolicy(i) instanceof AzureSasCredentialPolicy) {
        azureSasCredentialPolicy = (AzureSasCredentialPolicy) blobServiceClient.getHttpPipeline().getPolicy(i);
      }
    }

    Assert.assertNotNull(azureSasCredentialPolicy);
  }

  @Test
  public void test_blobServiceClientBuilder_useAppRegistration()
  {
    AzureStorageAccountInputSourceConfig azureStorageAccountInputSourceConfig = new AzureStorageAccountInputSourceConfig(
        null,
        null,
        "clientId",
        "clientSecret",
        "tenantId"
    );
    azureIngestClientFactory = new AzureIngestClientFactory(accountConfig, azureStorageAccountInputSourceConfig);
    replayAll();
    BlobServiceClient blobServiceClient = azureIngestClientFactory.getBlobServiceClient(3, ACCOUNT);
    verifyAll();
    BearerTokenAuthenticationPolicy bearerTokenAuthenticationPolicy = null;
    for (int i = 0; i < blobServiceClient.getHttpPipeline().getPolicyCount(); i++) {
      if (blobServiceClient.getHttpPipeline().getPolicy(i) instanceof BearerTokenAuthenticationPolicy) {
        bearerTokenAuthenticationPolicy = (BearerTokenAuthenticationPolicy) blobServiceClient.getHttpPipeline().getPolicy(i);
      }
    }

    Assert.assertNotNull(bearerTokenAuthenticationPolicy);
  }


  @Test
  public void test_blobServiceClientBuilder_useAzureAccountConfig_asDefaultMaxTries()
  {
    // We should only call getKey twice (both times in the first call to getBlobServiceClient)
    EasyMock.expect(azureStorageAccountInputSourceConfig.getKey()).andReturn(KEY).times(2);
    azureIngestClientFactory = new AzureIngestClientFactory(accountConfig, azureStorageAccountInputSourceConfig);
    EasyMock.expect(accountConfig.getMaxTries()).andReturn(5);
    replayAll();
    azureIngestClientFactory.getBlobServiceClient(null, ACCOUNT);

    // should use the cached client and not call getKey
    azureIngestClientFactory.getBlobServiceClient(5, ACCOUNT);

    // should use the cached client and not call getKey
    azureIngestClientFactory.getBlobServiceClient(5, ACCOUNT);

    verifyAll();
  }

  @Test
  public void test_blobServiceClientBuilder_fallbackToAzureAccountConfig()
  {
    AzureStorageAccountInputSourceConfig azureStorageAccountInputSourceConfig = new AzureStorageAccountInputSourceConfig(
        null,
        null,
        null,
        null,
        null
    );
    azureIngestClientFactory = new AzureIngestClientFactory(accountConfig, azureStorageAccountInputSourceConfig);
    EasyMock.expect(accountConfig.getKey()).andReturn(KEY).times(2);
    replayAll();
    azureIngestClientFactory.getBlobServiceClient(5, ACCOUNT);
    verifyAll();
  }
}
