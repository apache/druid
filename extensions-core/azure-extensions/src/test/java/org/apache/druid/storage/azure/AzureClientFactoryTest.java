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
import org.apache.druid.java.util.common.concurrent.Execs;
import org.junit.jupiter.api.Test;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

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
    assertEquals(ACCOUNT, blobServiceClient.getAccountName());
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
    assertNotNull(storageSharedKeyCredential);

    // Azure doesn't let us look at the key in the StorageSharedKeyCredential so make sure the authorization header generated is what we expect.
    assertEquals(
        new StorageSharedKeyCredential(ACCOUNT, "key")
            .generateAuthorizationHeader(new URL("http://druid.com"), "POST", ImmutableMap.of()),
        storageSharedKeyCredential
            .generateAuthorizationHeader(new URL("http://druid.com"), "POST", ImmutableMap.of())
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

    assertNotNull(azureSasCredentialPolicy);
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

    assertNotNull(bearerTokenAuthenticationPolicy);
  }

  @Test
  public void test_blobServiceClientBuilder_useCachedClient()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    config.setUseAzureCredentialsChain(true);
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    BlobServiceClient blobServiceClient2 = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    assertEquals(blobServiceClient, blobServiceClient2);
  }

  @Test
  public void test_blobServiceClientBuilder_useNewClientForDifferentRetryCount()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    config.setUseAzureCredentialsChain(true);
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    BlobServiceClient blobServiceClient2 = azureClientFactory.getBlobServiceClient(1, ACCOUNT);
    assertNotEquals(blobServiceClient, blobServiceClient2);
  }

  @Test
  public void test_blobServiceClientBuilder_useAzureAccountConfig_asDefaultMaxTries()
  {
    AzureAccountConfig config = new AzureAccountConfig();
    config.setKey("key");
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient expectedBlobServiceClient = azureClientFactory.getBlobServiceClient(3, ACCOUNT);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    assertEquals(expectedBlobServiceClient, blobServiceClient);
  }

  @Test
  public void test_blobServiceClientBuilder_useAzureAccountConfigWithNonDefaultEndpoint_clientUsesEndpointSpecified()
      throws MalformedURLException
  {
    String endpointSuffix = "core.nonDefault.windows.net";
    AzureAccountConfig config = new AzureAccountConfig();
    config.setKey("key");
    config.setEndpointSuffix(endpointSuffix);
    URL expectedAccountUrl = new URL("https", ACCOUNT + "." + AzureUtils.BLOB + "." + endpointSuffix, "");
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    assertEquals(expectedAccountUrl.toString(), blobServiceClient.getAccountUrl());
  }

  @Test
  public void test_blobServiceClientBuilder_useAzureAccountConfigWithStorageAccountEndpointAndNonDefaultEndpoint_clientUsesEndpointSpecified()
      throws MalformedURLException
  {
    String endpointSuffix = "core.nonDefault.windows.net";
    String storageAccountEndpointSuffix = "ABC123.blob.storage.azure.net";
    AzureAccountConfig config = new AzureAccountConfig();
    config.setKey("key");
    config.setEndpointSuffix(endpointSuffix);
    config.setStorageAccountEndpointSuffix(storageAccountEndpointSuffix);
    URL expectedAccountUrl = new URL("https", ACCOUNT + "." + AzureUtils.BLOB + "." + endpointSuffix, "");
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    assertEquals(expectedAccountUrl.toString(), blobServiceClient.getAccountUrl());
  }

  @Test
  public void test_blobServiceClientBuilder_useAzureAccountConfigWithStorageAccountEndpointAndNoEndpoint_clientUsesStorageAccountEndpointSpecified()
      throws MalformedURLException
  {
    String storageAccountEndpointSuffix = "ABC123.blob.storage.azure.net";
    AzureAccountConfig config = new AzureAccountConfig();
    config.setKey("key");
    config.setStorageAccountEndpointSuffix(storageAccountEndpointSuffix);
    URL expectedAccountUrl = new URL("https", ACCOUNT + "." + storageAccountEndpointSuffix, "");
    azureClientFactory = new AzureClientFactory(config);
    BlobServiceClient blobServiceClient = azureClientFactory.getBlobServiceClient(null, ACCOUNT);
    assertEquals(expectedAccountUrl.toString(), blobServiceClient.getAccountUrl());
  }

  @Test
  public void test_concurrent_azureClientFactory_gets() throws Exception
  {
    for (int i = 0; i < 10; i++) {
      concurrentAzureClientFactoryGets();
    }
  }

  private void concurrentAzureClientFactoryGets() throws Exception
  {
    final int threads = 100;
    String endpointSuffix = "core.nonDefault.windows.net";
    String storageAccountEndpointSuffix = "ABC123.blob.storage.azure.net";
    AzureAccountConfig config = new AzureAccountConfig();
    config.setKey("key");
    config.setEndpointSuffix(endpointSuffix);
    config.setStorageAccountEndpointSuffix(storageAccountEndpointSuffix);
    final AzureClientFactory localAzureClientFactory = new AzureClientFactory(config);
    final URL expectedAccountUrl = new URL(
        "https",
        ACCOUNT + "." + storageAccountEndpointSuffix,
        ""
    );

    final CountDownLatch latch = new CountDownLatch(threads);
    ExecutorService executorService = Execs.multiThreaded(threads, "azure-client-fetcher-%d");
    final AtomicReference<Exception> failureException = new AtomicReference<>();
    for (int i = 0; i < threads; i++) {
      final int retry = i % 2;
      executorService.submit(() -> {
        try {
          latch.countDown();
          latch.await();
          BlobServiceClient blobServiceClient = localAzureClientFactory.getBlobServiceClient(retry, ACCOUNT);
          assertEquals(expectedAccountUrl.toString(), blobServiceClient.getAccountUrl());
        }
        catch (Exception e) {
          failureException.compareAndSet(null, e);
        }
      });
    }

    //noinspection ResultOfMethodCallIgnored
    executorService.awaitTermination(1000, TimeUnit.MICROSECONDS);

    if (failureException.get() != null) {
      throw failureException.get();
    }
  }
}
