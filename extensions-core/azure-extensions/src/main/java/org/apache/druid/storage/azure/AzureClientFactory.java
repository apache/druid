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
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.CustomerProvidedKey;
import com.azure.storage.common.StorageSharedKeyCredential;

import java.time.Duration;

/**
 * Factory class for generating BlobServiceClient and BlobContainerClient objects. This is necessary instead of using
 * BlobServiceClient.createBlobContainerIfNotExists because sometimes we need different retryOptions on our container
 * clients and Azure doesn't let us override this setting on the default BlobServiceClient.
 */
public class AzureClientFactory
{

  private final AzureAccountConfig config;

  public AzureClientFactory(AzureAccountConfig config)
  {
    this.config = config;
  }

  public BlobServiceClient getBlobServiceClient()
  {
    BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder()
        .endpoint("https://" + config.getAccount() + ".blob.core.windows.net")
        .retryOptions(new RetryOptions(
            new ExponentialBackoffOptions().setMaxRetries(config.getMaxTries()).setBaseDelay(Duration.ofMillis(1000)).setMaxDelay(Duration.ofMillis(60000))
        ));

    if (config.getKey() != null) {
      clientBuilder.credential(new StorageSharedKeyCredential(config.getAccount(), config.getKey()));
    } else if (config.getSharedAccessStorageToken() != null) {
      clientBuilder.sasToken(config.getSharedAccessStorageToken());
    } else if (config.getUseAzureCredentialsChain()) {
      // We might not use the managed identity client id in the credential chain but we can just set it here and it will no-op.
      DefaultAzureCredentialBuilder defaultAzureCredentialBuilder = new DefaultAzureCredentialBuilder()
          .managedIdentityClientId(config.getManagedIdentityClientId());
      clientBuilder.credential(defaultAzureCredentialBuilder.build());
    }
    return clientBuilder.buildClient();
  }

  public BlobContainerClient getBlobContainerClient(String containerName, Integer maxRetries)
  {
    BlobContainerClientBuilder clientBuilder = new BlobContainerClientBuilder()
        .endpoint("https://" + config.getAccount() + ".blob.core.windows.net")
        .containerName(containerName)
        .retryOptions(new RetryOptions(
            new ExponentialBackoffOptions().setMaxRetries(maxRetries).setBaseDelay(Duration.ofMillis(1000)).setMaxDelay(Duration.ofMillis(60000))
        ));
    if (config.getKey() != null) {
      clientBuilder.customerProvidedKey(new CustomerProvidedKey(config.getKey()));
    } else if (config.getSharedAccessStorageToken() != null) {
      clientBuilder.sasToken(config.getSharedAccessStorageToken());
    } else if (config.getUseAzureCredentialsChain()) {
      // We might not use the managed identity client id in the credential chain but we can just set it here and it will no-op.
      DefaultAzureCredentialBuilder defaultAzureCredentialBuilder = new DefaultAzureCredentialBuilder()
          .managedIdentityClientId(config.getManagedIdentityClientId());
      clientBuilder.credential(defaultAzureCredentialBuilder.build());
    }
    return clientBuilder.buildClient();
  }
}
