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

package org.apache.druid.testsEx.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import java.util.Map;

/**
 * Utility functions related to test configuration.
 */
public class TestConfigs
{
  /**
   * Converts a YAML-aware object to a YAML string, primarily
   * for use in @{code toString()} methods.
   */
  public static String toYaml(Object obj)
  {
    ObjectMapper mapper = new ObjectMapper(
        new YAMLFactory()
          .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    try {
      return mapper.writeValueAsString(obj);
    }
    catch (JsonProcessingException e) {
      return "<conversion failed>";
    }
  }

  public static void putProperty(Map<String, Object> properties, String key, Object value)
  {
    if (value == null) {
      return;
    }
    properties.put(key, value);
  }

  public static void putProperty(Map<String, Object> properties, String base, String key, Object value)
  {
    if (value == null) {
      return;
    }
    properties.put(base + "." + key, value);
  }
}
