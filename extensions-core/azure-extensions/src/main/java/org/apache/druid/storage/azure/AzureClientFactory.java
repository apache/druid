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

import com.azure.core.http.policy.ExponentialBackoffOptions;
import com.azure.core.http.policy.RetryOptions;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.batch.BlobBatchClient;
import com.azure.storage.blob.batch.BlobBatchClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.druid.java.util.common.Pair;

import javax.annotation.Nullable;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory class for generating BlobServiceClient objects.
 */
public class AzureClientFactory
{

  protected final AzureAccountConfig config;
  private final Map<Pair<String, Integer>, BlobServiceClient> cachedBlobServiceClients;

  public AzureClientFactory(AzureAccountConfig config)
  {
    this.config = config;
    this.cachedBlobServiceClients = new HashMap<>();
  }

  // It's okay to store clients in a map here because all the configs for specifying azure retries are static, and there are only 2 of them.
  // The 2 configs are AzureAccountConfig.maxTries and AzureOutputConfig.maxRetry.
  // We will only ever have at most 2 clients in cachedBlobServiceClients per storage account.
  public BlobServiceClient getBlobServiceClient(@Nullable Integer retryCount, String storageAccount)
  {
    return cachedBlobServiceClients.computeIfAbsent(Pair.of(storageAccount, retryCount != null ? retryCount : config.getMaxTries()), key -> buildNewClient(key.rhs, key.lhs));
  }

  // Mainly here to make testing easier.
  public BlobBatchClient getBlobBatchClient(BlobContainerClient blobContainerClient)
  {
    return new BlobBatchClientBuilder(blobContainerClient).buildClient();
  }

  protected BlobServiceClient buildNewClient(Integer retryCount, String storageAccount)
  {
    BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder()
        .endpoint("https://" + storageAccount + "." + config.getBlobStorageEndpoint());

    if (config.getKey() != null) {
      clientBuilder.credential(new StorageSharedKeyCredential(storageAccount, config.getKey()));
    } else if (config.getSharedAccessStorageToken() != null) {
      clientBuilder.sasToken(config.getSharedAccessStorageToken());
    } else if (config.getUseAzureCredentialsChain()) {
      // We might not use the managed identity client id in the credential chain but we can just set it here and it will no-op.
      DefaultAzureCredentialBuilder defaultAzureCredentialBuilder = new DefaultAzureCredentialBuilder()
          .managedIdentityClientId(config.getManagedIdentityClientId());
      clientBuilder.credential(defaultAzureCredentialBuilder.build());
    }
    return clientBuilder
        .retryOptions(new RetryOptions(
            new ExponentialBackoffOptions()
                .setMaxRetries(retryCount)
                .setBaseDelay(Duration.ofMillis(1000))
                .setMaxDelay(Duration.ofMillis(60000))
        ))
        .buildClient();
  }
}
