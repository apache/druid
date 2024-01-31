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

import javax.validation.constraints.Min;

/**
 * Stores the configuration for an Azure account.
 */
public class AzureAccountConfig
{
  @JsonProperty
  private String protocol = "https";

  @JsonProperty
  @Min(1)
  private int maxTries = 3;

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

  @JsonProperty
  private String endpointSuffix = AzureUtils.DEFAULT_AZURE_ENDPOINT_SUFFIX;

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

  public void setUseAzureCredentialsChain(Boolean useAzureCredentialsChain)
  {
    this.useAzureCredentialsChain = useAzureCredentialsChain;
  }

  public String getEndpointSuffix()
  {
    return endpointSuffix;
  }

  public String getBlobStorageEndpoint()
  {
    return "blob." + endpointSuffix;
  }
}
