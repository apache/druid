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

import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.common.StorageSharedKeyCredential;
import org.apache.druid.data.input.azure.AzureInputSourceConfig;


public class AzureIngestClientFactory extends AzureClientFactory
{
  private final AzureInputSourceConfig azureInputSourceConfig;

  public AzureIngestClientFactory(AzureAccountConfig config, AzureInputSourceConfig azureInputSourceConfig)
  {
    super(config);
    this.azureInputSourceConfig = azureInputSourceConfig;
  }

  @Override
  public BlobServiceClient getBlobServiceClient(String storageAccount)
  {
    BlobServiceClientBuilder clientBuilder = getBlobServiceClientBuilder(storageAccount);

    if (azureInputSourceConfig.getKey() != null) {
      clientBuilder.credential(new StorageSharedKeyCredential(storageAccount, azureInputSourceConfig.getKey()));
    } else if (azureInputSourceConfig.getSharedAccessStorageToken() != null) {
      clientBuilder.sasToken(azureInputSourceConfig.getSharedAccessStorageToken());
    } else if (azureInputSourceConfig.shouldUseAzureCredentialsChain() != null) {
      DefaultAzureCredentialBuilder defaultAzureCredentialBuilder = new DefaultAzureCredentialBuilder()
          .managedIdentityClientId(config.getManagedIdentityClientId());
      clientBuilder.credential(defaultAzureCredentialBuilder.build());
    } else if (azureInputSourceConfig.getAppRegistrationClientId() != null && azureInputSourceConfig.getAppRegistrationClientSecret() != null) {
      clientBuilder.credential(new ClientSecretCredentialBuilder()
          .clientSecret(azureInputSourceConfig.getAppRegistrationClientSecret())
          .clientId(azureInputSourceConfig.getAppRegistrationClientId())
          .tenantId(azureInputSourceConfig.getTenantId())
          .build()
      );
    } else {
      // If no additional auth method is passed, fallback to default factory method
      return super.getBlobServiceClient(storageAccount);
    }

    return clientBuilder.buildClient();
  }

  @Override
  public BlobContainerClient getBlobContainerClient(String storageAccount, String containerName, Integer maxRetries)
  {
    BlobContainerClientBuilder clientBuilder = getBlobContainerClientBuilder(storageAccount, containerName, maxRetries);
    if (azureInputSourceConfig.getKey() != null) {
      clientBuilder.credential(new StorageSharedKeyCredential(storageAccount, azureInputSourceConfig.getKey()));
    } else if (azureInputSourceConfig.getSharedAccessStorageToken() != null) {
      clientBuilder.sasToken(azureInputSourceConfig.getSharedAccessStorageToken());
    } else if (azureInputSourceConfig.shouldUseAzureCredentialsChain() != null) {
      DefaultAzureCredentialBuilder defaultAzureCredentialBuilder = new DefaultAzureCredentialBuilder()
          .managedIdentityClientId(config.getManagedIdentityClientId());
      clientBuilder.credential(defaultAzureCredentialBuilder.build());
    } else if (azureInputSourceConfig.getAppRegistrationClientId() != null && azureInputSourceConfig.getAppRegistrationClientSecret() != null) {
      clientBuilder.credential(new ClientSecretCredentialBuilder()
          .clientSecret(azureInputSourceConfig.getAppRegistrationClientSecret())
          .clientId(azureInputSourceConfig.getAppRegistrationClientId())
          .tenantId(azureInputSourceConfig.getTenantId())
          .build()
      );
    } else {
      // If no additional auth method is passed, fallback to default factory method
      return super.getBlobContainerClient(storageAccount, containerName, maxRetries);
    }
    return clientBuilder.buildClient();
  }
}
