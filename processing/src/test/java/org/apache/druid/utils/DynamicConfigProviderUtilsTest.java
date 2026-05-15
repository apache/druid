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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.metadata.DynamicConfigProvider;
import org.apache.druid.metadata.MapStringDynamicConfigProvider;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class DynamicConfigProviderUtilsTest
{
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final String DYNAMIC_CONFIG_PROVIDER = "druid.dynamic.config.provider";

  @Test
  public void testExtraConfigAndSetStringMap()
  {
    DynamicConfigProvider dynamicConfigProvider = new MapStringDynamicConfigProvider(
        ImmutableMap.of("prop2", "value2")
    );

    Map<String, Object> properties = ImmutableMap.of(
        "prop1", "value1",
        "prop2", "value3",
        DYNAMIC_CONFIG_PROVIDER, OBJECT_MAPPER.convertValue(dynamicConfigProvider, Map.class)
    );
    Map<String, String> res = DynamicConfigProviderUtils.extraConfigAndSetStringMap(properties, DYNAMIC_CONFIG_PROVIDER, OBJECT_MAPPER);

    Assertions.assertEquals(2, res.size());
    Assertions.assertEquals("value1", res.get("prop1"));
    Assertions.assertEquals("value2", res.get("prop2"));
  }

  @Test
  public void testExtraConfigAndSetObjectMap()
  {
    DynamicConfigProvider dynamicConfigProvider = new MapStringDynamicConfigProvider(
        ImmutableMap.of("prop2", "value2")
    );

    Map<String, Object> properties = ImmutableMap.of(
        "prop1", "value1",
        "prop2", "value3",
        DYNAMIC_CONFIG_PROVIDER, OBJECT_MAPPER.convertValue(dynamicConfigProvider, Map.class)
    );
    Map<String, Object> res = DynamicConfigProviderUtils.extraConfigAndSetObjectMap(properties, DYNAMIC_CONFIG_PROVIDER, OBJECT_MAPPER);

    Assertions.assertEquals(2, res.size());
    Assertions.assertEquals("value1", res.get("prop1").toString());
    Assertions.assertEquals("value2", res.get("prop2").toString());
  }
}
