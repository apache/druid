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

import javax.annotation.Nonnull;
import java.time.Duration;

/**
 * Factory class for generating BlobServiceClient objects.
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
    return getAuthenticatedBlobServiceClientBuilder().buildClient();
  }

  /**
   * Azure doesn't let us override retryConfigs on BlobServiceClient so we need a second instance.
   * @param retryCount number of retries
   * @return BlobServiceClient with a custom retryCount
   */
  public BlobServiceClient getRetriableBlobServiceClient(@Nonnull Integer retryCount)
  {
    BlobServiceClientBuilder clientBuilder = getAuthenticatedBlobServiceClientBuilder()
        .retryOptions(new RetryOptions(
          new ExponentialBackoffOptions()
              .setMaxRetries(retryCount)
              .setBaseDelay(Duration.ofMillis(1000))
              .setMaxDelay(Duration.ofMillis(60000))
      ));
    return clientBuilder.buildClient();
  }

  private BlobServiceClientBuilder getAuthenticatedBlobServiceClientBuilder()
  {
    BlobServiceClientBuilder clientBuilder = new BlobServiceClientBuilder()
        .endpoint("https://" + config.getAccount() + ".blob.core.windows.net");

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
    return clientBuilder;
  }
}
