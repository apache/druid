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

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import javax.validation.constraints.Min;

/**
 * Stores the configuration for an Azure account.
 */
public class AzureAccountConfig
{
  @JsonProperty
  @Nullable
  private String account;

  /**
   * @deprecated Use {@link #storageAccountEndpointSuffix} instead.
   */
  @Deprecated
  @Nullable
  @JsonProperty
  private String endpointSuffix = null;

  @JsonProperty
  private String key;

  @JsonProperty
  private String managedIdentityClientId;

  @JsonProperty
  @Min(1)
  private int maxTries = 3;

  @JsonProperty
  private String protocol = "https";

  @JsonProperty
  private String sharedAccessStorageToken;

  @JsonProperty
  private String storageAccountEndpointSuffix = AzureUtils.AZURE_STORAGE_HOST_ADDRESS;

  @JsonProperty
  private boolean useAzureCredentialsChain = false;

  public String getAccount()
  {
    return account;
  }

  public void setAccount(String account)
  {
    this.account = account;
  }

  @Nullable
  @Deprecated
  public String getEndpointSuffix()
  {
    return endpointSuffix;
  }

  public void setEndpointSuffix(String endpointSuffix)
  {
    this.endpointSuffix = endpointSuffix;
  }

  public String getKey()
  {
    return key;
  }

  public void setKey(String key)
  {
    this.key = key;
  }

  public String getManagedIdentityClientId()
  {
    return managedIdentityClientId;
  }

  public void setManagedIdentityClientId(String managedIdentityClientId)
  {
    this.managedIdentityClientId = managedIdentityClientId;
  }

  public int getMaxTries()
  {
    return maxTries;
  }

  public void setMaxTries(int maxTries)
  {
    this.maxTries = maxTries;
  }

  public String getProtocol()
  {
    return protocol;
  }

  public void setProtocol(String protocol)
  {
    this.protocol = protocol;
  }

  public String getSharedAccessStorageToken()
  {
    return sharedAccessStorageToken;
  }

  public void setSharedAccessStorageToken(String sharedAccessStorageToken)
  {
    this.sharedAccessStorageToken = sharedAccessStorageToken;
  }

  public String getStorageAccountEndpointSuffix()
  {
    return storageAccountEndpointSuffix;
  }

  public void setStorageAccountEndpointSuffix(String storageAccountEndpointSuffix)
  {
    this.storageAccountEndpointSuffix = storageAccountEndpointSuffix;
  }

  public Boolean getUseAzureCredentialsChain()
  {
    return useAzureCredentialsChain;
  }

  public void setUseAzureCredentialsChain(Boolean useAzureCredentialsChain)
  {
    this.useAzureCredentialsChain = useAzureCredentialsChain;
  }

  /**
   * Helper to support legacy runtime property. Replace with {@link #getStorageAccountEndpointSuffix()} when
   * deprecated endpointSuffix has been removed.
   */
  public String getBlobStorageEndpoint()
  {
    if (endpointSuffix != null) {
      return AzureUtils.BLOB + "." + endpointSuffix;
    }

    return storageAccountEndpointSuffix;
  }
}
