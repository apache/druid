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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Test;

public class AzureAccountConfigTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  @Test
  public void test_getBlobStorageEndpoint_endpointSuffixNullAndStorageAccountEndpointSuffixNull_expectedDefault()
      throws JsonProcessingException
  {
    AzureAccountConfig config = new AzureAccountConfig();
    AzureAccountConfig configSerde = MAPPER.readValue("{}", AzureAccountConfig.class);
    Assert.assertEquals(configSerde, config);
    Assert.assertEquals(AzureUtils.AZURE_STORAGE_HOST_ADDRESS, config.getBlobStorageEndpoint());
  }

  @Test
  public void test_getBlobStorageEndpoint_endpointSuffixNotNullAndStorageAccountEndpointSuffixNull_expectEndpoint()
      throws JsonProcessingException
  {
    String endpointSuffix = "core.usgovcloudapi.net";
    AzureAccountConfig config = new AzureAccountConfig();
    config.setEndpointSuffix(endpointSuffix);
    AzureAccountConfig configSerde = MAPPER.readValue(
        "{"
        + "\"endpointSuffix\": \"" + endpointSuffix + "\""
        + "}",
        AzureAccountConfig.class);
    Assert.assertEquals(configSerde, config);
    Assert.assertEquals(AzureUtils.BLOB + "." + endpointSuffix, config.getBlobStorageEndpoint());
  }

  @Test
  public void test_getBlobStorageEndpoint_endpointSuffixNotNullAndStorageAccountEndpointSuffixNotNull_expectEndpoint()
      throws JsonProcessingException
  {
    String endpointSuffix = "core.usgovcloudapi.net";
    String storageAccountEndpointSuffix = "ABCD1234.blob.storage.azure.net";
    AzureAccountConfig config = new AzureAccountConfig();
    config.setEndpointSuffix(endpointSuffix);
    config.setStorageAccountEndpointSuffix(storageAccountEndpointSuffix);
    AzureAccountConfig configSerde = MAPPER.readValue(
        "{"
        + "\"endpointSuffix\": \"" + endpointSuffix + "\","
        + " \"storageAccountEndpointSuffix\": \"" + storageAccountEndpointSuffix + "\""
        + "}",
        AzureAccountConfig.class);
    Assert.assertEquals(configSerde, config);
    Assert.assertEquals(AzureUtils.BLOB + "." + endpointSuffix, config.getBlobStorageEndpoint());
  }

  @Test
  public void test_getBlobStorageEndpoint_endpointSuffixNullAndStorageAccountEndpointSuffixNotNull_expectStorageAccountEndpoint()
      throws JsonProcessingException
  {
    String storageAccountEndpointSuffix = "ABCD1234.blob.storage.azure.net";
    AzureAccountConfig config = new AzureAccountConfig();
    config.setStorageAccountEndpointSuffix(storageAccountEndpointSuffix);
    AzureAccountConfig configSerde = MAPPER.readValue(
        "{"
        + "\"storageAccountEndpointSuffix\": \"" + storageAccountEndpointSuffix + "\""
        + "}",
        AzureAccountConfig.class);
    Assert.assertEquals(configSerde, config);
    Assert.assertEquals(storageAccountEndpointSuffix, config.getBlobStorageEndpoint());
  }

  @Test
  public void test_getManagedIdentityClientId_withValueForManagedIdentityClientId_expectManagedIdentityClientId()
      throws JsonProcessingException
  {
    String managedIdentityClientId = "blah";
    AzureAccountConfig config = new AzureAccountConfig();
    config.setManagedIdentityClientId("blah");
    AzureAccountConfig configSerde = MAPPER.readValue(
        "{"
        + "\"managedIdentityClientId\": \"" + managedIdentityClientId + "\""
        + "}",
        AzureAccountConfig.class);
    Assert.assertEquals(configSerde, config);
    Assert.assertEquals("blah", config.getManagedIdentityClientId());
  }
}
