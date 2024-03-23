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

package org.apache.druid.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.metadata.DynamicConfigProvider;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DynamicConfigProviderUtils
{
  public static Map<String, String> extraConfigAndSetStringMap(Map<String, Object> config, String dynamicConfigProviderKey, ObjectMapper mapper)
  {
    HashMap<String, String> newConfig = new HashMap<>();
    if (config != null) {
      for (Map.Entry<String, Object> entry : config.entrySet()) {
        if (!dynamicConfigProviderKey.equals(entry.getKey())) {
          newConfig.put(entry.getKey(), entry.getValue().toString());
        }
      }
      Map<String, String> dynamicConfig = extraConfigFromProvider(config.get(dynamicConfigProviderKey), mapper);
      newConfig.putAll(dynamicConfig);
    }
    return newConfig;
  }

  public static Map<String, Object> extraConfigAndSetObjectMap(Map<String, Object> config, String dynamicConfigProviderKey, ObjectMapper mapper)
  {
    HashMap<String, Object> newConfig = new HashMap<>();
    if (config != null) {
      for (Map.Entry<String, Object> entry : config.entrySet()) {
        if (!dynamicConfigProviderKey.equals(entry.getKey())) {
          newConfig.put(entry.getKey(), entry.getValue());
        }
      }
      Map<String, String> dynamicConfig = extraConfigFromProvider(config.get(dynamicConfigProviderKey), mapper);
      newConfig.putAll(dynamicConfig);
    }
    return newConfig;
  }

  private static Map<String, String> extraConfigFromProvider(Object dynamicConfigProviderJson, ObjectMapper mapper)
  {
    if (dynamicConfigProviderJson != null) {
      DynamicConfigProvider dynamicConfigProvider = mapper.convertValue(dynamicConfigProviderJson, DynamicConfigProvider.class);
      return dynamicConfigProvider.getConfig();
    }
    return Collections.emptyMap();
  }
}
