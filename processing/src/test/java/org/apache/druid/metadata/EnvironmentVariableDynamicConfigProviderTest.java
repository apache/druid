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

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

public class EnvironmentVariableDynamicConfigProviderTest
{
  @Test
  public void testSerde() throws IOException
  {
    final ObjectMapper objectMapper = new ObjectMapper();

    String providerString = "{\"type\": \"environment\", \"variables\" : {\"testKey\":\"testValue\"}}";
    DynamicConfigProvider provider = objectMapper.readValue(providerString, DynamicConfigProvider.class);
    Assert.assertTrue(provider instanceof EnvironmentVariableDynamicConfigProvider);
    Assert.assertEquals("testValue", ((EnvironmentVariableDynamicConfigProvider) provider).getVariables().get("testKey"));
    DynamicConfigProvider serde = objectMapper.readValue(objectMapper.writeValueAsString(provider), DynamicConfigProvider.class);
    Assert.assertEquals(provider, serde);
  }

  @Test
  public void testGetConfig()
  {
    final ImmutableMap<String, String> env = ImmutableMap.of(
        "DRUID_USER",
        "druid",
        "DRUID_PASSWORD",
        "123"
    );

    Map<String, String> config = ImmutableMap.of("user", "DRUID_USER", "password", "DRUID_PASSWORD");
    EnvironmentVariableDynamicConfigProvider provider = new EnvironmentVariableDynamicConfigProvider(config)
    {
      @Override
      protected String getEnv(String var)
      {
        return env.containsKey(var) ? env.get(var) : super.getEnv(var);
      }
    };

    Assert.assertEquals("druid", provider.getConfig().get("user"));
    Assert.assertEquals("123", provider.getConfig().get("password"));
  }
}
