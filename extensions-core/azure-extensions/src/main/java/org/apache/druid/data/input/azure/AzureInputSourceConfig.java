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

package org.apache.druid.data.input.azure;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Contains properties for Azure input source.
 * Properties can be specified by ingestionSpec which will override system default.
 */
public class AzureInputSourceConfig
{
  private final String storageAccount;
  private final String sharedAccessStorageToken;
  private final String key;
  private final Boolean useAzureCredentialsChain;

  @JsonCreator
  public AzureInputSourceConfig(
      @JsonProperty("storageAccount") @Nullable String storageAccount,
      @JsonProperty("sharedAccessStorageToken") @Nullable String sharedAccessStorageToken,
      @JsonProperty("key") @Nullable String key,
      @JsonProperty("useAzureCredentialsChain") @Nullable Boolean useAzureCredentialsChain
  )
  {
    this.storageAccount = storageAccount;
    this.sharedAccessStorageToken = sharedAccessStorageToken;
    this.key = key;
    this.useAzureCredentialsChain = useAzureCredentialsChain;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getStorageAccount()
  {
    return storageAccount;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getSharedAccessStorageToken()
  {
    return sharedAccessStorageToken;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getKey()
  {
    return key;
  }

  @Nullable
  @JsonProperty("useAzureCredentialsChain")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Boolean shouldUseAzureCredentialsChain()
  {
    return useAzureCredentialsChain;
  }

  @Override
  public String toString()
  {
    return "AzureInputSourceConfig{" +
        "storageAccount=" + storageAccount +
        ", sharedAccessStorageToken=" + sharedAccessStorageToken +
        ", key=" + key +
        ", useAzureCredentialsChain=" + useAzureCredentialsChain +
        '}';
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
    AzureInputSourceConfig that = (AzureInputSourceConfig) o;
    return Objects.equals(storageAccount, that.storageAccount)
        && Objects.equals(key, that.key)
        && Objects.equals(sharedAccessStorageToken, that.sharedAccessStorageToken)
        && Objects.equals(useAzureCredentialsChain, that.useAzureCredentialsChain);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(storageAccount, sharedAccessStorageToken, key, useAzureCredentialsChain);
  }
}
