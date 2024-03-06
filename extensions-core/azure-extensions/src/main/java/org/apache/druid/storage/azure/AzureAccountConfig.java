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
import java.util.Objects;

/**
 * Stores the configuration for an Azure account.
 */
public class AzureAccountConfig
{
  static final String DEFAULT_PROTOCOL = "https";
  static final int DEFAULT_MAX_TRIES = 3;

  @JsonProperty
  private String protocol = DEFAULT_PROTOCOL;

  @JsonProperty
  @Min(1)
  private int maxTries = DEFAULT_MAX_TRIES;

  @JsonProperty
  private String account;

  @JsonProperty
  private String key;

  @JsonProperty
  private String sharedAccessStorageToken;

  @JsonProperty
  private String managedIdentityClientId;

  @JsonProperty
  private Boolean useAzureCredentialsChain = Boolean.FALSE;

  @Deprecated
  @Nullable
  @JsonProperty
  private String endpointSuffix = null;

  @JsonProperty
  private String storageAccountEndpointSuffix = AzureUtils.AZURE_STORAGE_HOST_ADDRESS;

  @SuppressWarnings("unused") // Used by Jackson deserialization?
  public void setProtocol(String protocol)
  {
    this.protocol = protocol;
  }

  @SuppressWarnings("unused") // Used by Jackson deserialization?
  public void setMaxTries(int maxTries)
  {
    this.maxTries = maxTries;
  }

  public void setAccount(String account)
  {
    this.account = account;
  }

  @SuppressWarnings("unused") // Used by Jackson deserialization?
  public void setKey(String key)
  {
    this.key = key;
  }

  @SuppressWarnings("unused") // Used by Jackson deserialization?
  public void setEndpointSuffix(String endpointSuffix)
  {
    this.endpointSuffix = endpointSuffix;
  }

  @SuppressWarnings("unused") // Used by Jackson deserialization?
  public void setStorageAccountEndpointSuffix(String storageAccountEndpointSuffix)
  {
    this.storageAccountEndpointSuffix = storageAccountEndpointSuffix;
  }

  public String getProtocol()
  {
    return protocol;
  }

  public int getMaxTries()
  {
    return maxTries;
  }

  public String getAccount()
  {
    return account;
  }

  public String getKey()
  {
    return key;
  }

  public String getSharedAccessStorageToken()
  {
    return sharedAccessStorageToken;
  }

  public Boolean getUseAzureCredentialsChain()
  {
    return useAzureCredentialsChain;
  }

  public String getManagedIdentityClientId()
  {
    return managedIdentityClientId;
  }


  @SuppressWarnings("unused") // Used by Jackson deserialization?
  public void setSharedAccessStorageToken(String sharedAccessStorageToken)
  {
    this.sharedAccessStorageToken = sharedAccessStorageToken;
  }

  @SuppressWarnings("unused") // Used by Jackson deserialization?
  public void setManagedIdentityClientId(String managedIdentityClientId)
  {
    this.managedIdentityClientId = managedIdentityClientId;
  }

  public void setUseAzureCredentialsChain(Boolean useAzureCredentialsChain)
  {
    this.useAzureCredentialsChain = useAzureCredentialsChain;
  }

  @Nullable
  @Deprecated
  public String getEndpointSuffix()
  {
    return endpointSuffix;
  }

  public String getStorageAccountEndpointSuffix()
  {
    return storageAccountEndpointSuffix;
  }

  public String getBlobStorageEndpoint()
  {
    // this is here to support the legacy runtime property.
    if (endpointSuffix != null) {
      return AzureUtils.BLOB + "." + endpointSuffix;
    }
    return storageAccountEndpointSuffix;
  }
  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    AzureAccountConfig that = (AzureAccountConfig) o;
    return Objects.equals(protocol, that.protocol)
           && Objects.equals(maxTries, that.maxTries)
           && Objects.equals(account, that.account)
           && Objects.equals(key, that.key)
           && Objects.equals(sharedAccessStorageToken, that.sharedAccessStorageToken)
           && Objects.equals(managedIdentityClientId, that.managedIdentityClientId)
           && Objects.equals(useAzureCredentialsChain, that.useAzureCredentialsChain)
           && Objects.equals(endpointSuffix, that.endpointSuffix)
           && Objects.equals(storageAccountEndpointSuffix, that.storageAccountEndpointSuffix);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(protocol, maxTries, account, key, sharedAccessStorageToken, managedIdentityClientId, useAzureCredentialsChain, endpointSuffix, storageAccountEndpointSuffix);
  }

  @Override
  public String toString()
  {
    return "AzureAccountConfig{" +
           "protocol=" + protocol +
           ", maxTries=" + maxTries +
           ", account=" + account +
           ", key=" + key +
           ", sharedAccessStorageToken=" + sharedAccessStorageToken +
           ", managedIdentityClientId=" + managedIdentityClientId +
           ", useAzureCredentialsChain=" + useAzureCredentialsChain +
           ", endpointSuffix=" + endpointSuffix +
           ", storageAccountEndpointSuffix=" + storageAccountEndpointSuffix +
           '}';
  }
}
