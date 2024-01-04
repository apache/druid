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
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Factory class for generating BlobServiceClient objects.
 */
public class AzureClientFactory
{

  protected final AzureAccountConfig config;
  private final Map<Integer, BlobServiceClient> cachedBlobServiceClients;

  public AzureClientFactory(AzureAccountConfig config)
  {
    this.config = config;
    this.cachedBlobServiceClients = new HashMap<>();
  }

  public String getStorageAccount()
  {
    return config.getAccount();
  }

  // It's okay to store clients in a map here because all the configs for specifying azure retries are static, and there are only 2 of them.
  // The 2 configs are AzureAccountConfig.maxTries and AzureOutputConfig.maxRetrr.
  // We will only ever have at most 2 clients in cachedBlobServiceClients.
  public BlobServiceClient getBlobServiceClient(Integer retryCount)
  {
    return cachedBlobServiceClients.computeIfAbsent(retryCount != null ? retryCount : config.getMaxTries(), this::buildNewClient);
  }

  protected BlobServiceClient buildNewClient(Integer retryCount)
  {
    BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder()
        .endpoint("https://" + getStorageAccount() + ".blob.core.windows.net");

    if (config.getKey() != null) {
      clientBuilder.credential(new StorageSharedKeyCredential(getStorageAccount(), config.getKey()));
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
                .setMaxRetries(retryCount != null ? retryCount : config.getMaxTries())
                .setBaseDelay(Duration.ofMillis(1000))
                .setMaxDelay(Duration.ofMillis(60000))
        ))
        .buildClient();
  }
}
