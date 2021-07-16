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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EnvironmentVariableDynamicConfigProviderTest
{
  private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
  private static final Map<String, String> CHANGED_ENV_MAP = new HashMap<>();

  @BeforeClass
  public static void setupTest() throws Exception
  {
    Map<String, String> oldEnvMap = getENVMap();
    Map<String, String> addEnvMap = ImmutableMap.of("DRUID_USER", "druid", "DRUID_PASSWORD", "123");
    for (Map.Entry<String, String> entry : addEnvMap.entrySet()) {
      CHANGED_ENV_MAP.put(entry.getKey(), oldEnvMap.get(entry.getKey()));
      oldEnvMap.put(entry.getKey(), entry.getValue());
    }
  }

  @AfterClass
  public static void tearDownTest() throws Exception
  {
    Map<String, String> oldEnvMap = getENVMap();
    for (Map.Entry<String, String> entry : CHANGED_ENV_MAP.entrySet()) {
      if (entry.getValue() == null) {
        oldEnvMap.remove(entry.getKey());
      } else {
        oldEnvMap.put(entry.getKey(), entry.getValue());
      }
    }
  }

  @Test
  public void testSerde() throws IOException
  {
    String providerString = "{\"type\": \"environment\", \"variables\" : {\"testKey\":\"testValue\"}}";
    DynamicConfigProvider provider = JSON_MAPPER.readValue(providerString, DynamicConfigProvider.class);
    Assert.assertTrue(provider instanceof EnvironmentVariableDynamicConfigProvider);
    Assert.assertEquals("testValue", ((EnvironmentVariableDynamicConfigProvider) provider).getVariables().get("testKey"));
    DynamicConfigProvider serde = JSON_MAPPER.readValue(JSON_MAPPER.writeValueAsString(provider), DynamicConfigProvider.class);
    Assert.assertEquals(provider, serde);
  }

  @Test
  public void testGetConfig() throws Exception
  {
    String providerString = "{\"type\": \"environment\", \"variables\" : {\"user\":\"DRUID_USER\",\"password\":\"DRUID_PASSWORD\"}}";
    DynamicConfigProvider provider = JSON_MAPPER.readValue(providerString, DynamicConfigProvider.class);
    Assert.assertTrue(provider instanceof EnvironmentVariableDynamicConfigProvider);
    Assert.assertEquals("druid", ((EnvironmentVariableDynamicConfigProvider) provider).getConfig().get("user"));
    Assert.assertEquals("123", ((EnvironmentVariableDynamicConfigProvider) provider).getConfig().get("password"));
  }

  /**
   * This method use reflection to get system environment variables map in runtime JVM
   * which can be changed.
   *
   * @return system environment variables map.
   */
  private static Map<String, String> getENVMap() throws Exception
  {
    Map<String, String> envMap = null;
    Class[] classes = Collections.class.getDeclaredClasses();
    Map<String, String> systemEnv = System.getenv();
    for (Class cl : classes) {
      if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
        Field field = cl.getDeclaredField("m");
        field.setAccessible(true);
        Object object = field.get(systemEnv);
        envMap = (Map<String, String>) object;
      }
    }
    if (envMap == null) {
      throw new RuntimeException("Failed to get environment map.");
    }
    return envMap;
  }
}
