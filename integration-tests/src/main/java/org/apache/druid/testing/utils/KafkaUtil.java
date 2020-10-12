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

package org.apache.druid.testing.utils;

import org.apache.druid.testing.IntegrationTestingConfig;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaUtil
{
  private static final String TEST_PROPERTY_PREFIX = "kafka.test.property.";
  private static final String TEST_CONFIG_PROPERTY_PREFIX = "kafka.test.config.";

  public static final String TEST_CONFIG_TRANSACTION_ENABLED = "transactionEnabled";

  public static void addPropertiesFromTestConfig(IntegrationTestingConfig config, Properties properties)
  {
    for (Map.Entry<String, String> entry : config.getProperties().entrySet()) {
      if (entry.getKey().startsWith(TEST_PROPERTY_PREFIX)) {
        properties.setProperty(entry.getKey().substring(TEST_PROPERTY_PREFIX.length()), entry.getValue());
      }
    }
  }

  public static Map<String, String> getAdditionalKafkaTestConfigFromProperties(IntegrationTestingConfig config)
  {
    Map<String, String> theMap = new HashMap<>();
    for (Map.Entry<String, String> entry : config.getProperties().entrySet()) {
      if (entry.getKey().startsWith(TEST_CONFIG_PROPERTY_PREFIX)) {
        theMap.put(entry.getKey().substring(TEST_CONFIG_PROPERTY_PREFIX.length()), entry.getValue());
      }
    }
    return theMap;
  }
}
