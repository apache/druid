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
public class AzureStorageAccountInputSourceConfig
{
  private final String sharedAccessStorageToken;
  private final String key;
  private final String appRegistrationClientId;
  private final String appRegistrationClientSecret;
  private final String tenantId;

  @JsonCreator
  public AzureStorageAccountInputSourceConfig(
      @JsonProperty("sharedAccessStorageToken") @Nullable String sharedAccessStorageToken,
      @JsonProperty("key") @Nullable String key,
      @JsonProperty("appRegistrationClientId") @Nullable String appRegistrationClientId,
      @JsonProperty("appRegistrationClientSecret") @Nullable String appRegistrationClientSecret,
      @JsonProperty("tenantId") @Nullable String tenantId

  )
  {
    this.sharedAccessStorageToken = sharedAccessStorageToken;
    this.key = key;
    this.appRegistrationClientId = appRegistrationClientId;
    this.appRegistrationClientSecret = appRegistrationClientSecret;
    this.tenantId = tenantId;
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
  public String getAppRegistrationClientId()
  {
    return appRegistrationClientId;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getAppRegistrationClientSecret()
  {
    return appRegistrationClientSecret;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getTenantId()
  {
    return tenantId;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getKey()
  {
    return key;
  }

  @Override
  public String toString()
  {
    return "AzureInputSourceConfig{" +
        "sharedAccessStorageToken=" + sharedAccessStorageToken +
        ", key=" + key +
        ", appRegistrationClientId=" + appRegistrationClientId +
        ", appRegistrationClientSecret=" + appRegistrationClientSecret +
        ", tenantId=" + tenantId +
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
    AzureStorageAccountInputSourceConfig that = (AzureStorageAccountInputSourceConfig) o;
    return Objects.equals(key, that.key)
        && Objects.equals(sharedAccessStorageToken, that.sharedAccessStorageToken)
        && Objects.equals(appRegistrationClientId, that.appRegistrationClientId)
        && Objects.equals(appRegistrationClientSecret, that.appRegistrationClientSecret)
        && Objects.equals(tenantId, that.tenantId);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        sharedAccessStorageToken,
        key,
        appRegistrationClientId,
        appRegistrationClientSecret,
        tenantId
    );
  }
}
