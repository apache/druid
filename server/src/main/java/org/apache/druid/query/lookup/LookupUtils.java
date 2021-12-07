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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 * Utility class for lookup related things
 */
public class LookupUtils
{

  private static final EmittingLogger LOG = new EmittingLogger(LookupUtils.class);

  private LookupUtils()
  {

  }

  /**
   * Takes a map of String to Object, representing lookup name to generic lookup config, and attempts to construct
   * a map from String to {@link LookupExtractorFactoryContainer}. Any lookup configs that are not able to be converted
   * to {@link LookupExtractorFactoryContainer}, will be logged as warning, and will not be included in the map
   * returned.
   *
   * @param lookupNameToGenericConfig The lookup generic config map.
   * @param objectMapper The object mapper to use to convert bytes to {@link LookupExtractorFactoryContainer}
   * @return
   */
  public static Map<String, LookupExtractorFactoryContainer> tryConvertObjectMapToLookupConfigMap(
      Map<String, Object> lookupNameToGenericConfig,
      ObjectMapper objectMapper
  )
  {
    Map<String, LookupExtractorFactoryContainer> lookupNameToConfig =
        Maps.newHashMapWithExpectedSize(lookupNameToGenericConfig.size());
    for (Map.Entry<String, Object> lookupNameAndConfig : lookupNameToGenericConfig.entrySet()) {
      String lookupName = lookupNameAndConfig.getKey();
      LookupExtractorFactoryContainer lookupConfig = tryConvertObjectToLookupConfig(
          lookupName,
          lookupNameAndConfig.getValue(),
          objectMapper
      );
      if (lookupConfig != null) {
        lookupNameToConfig.put(lookupName, lookupConfig);
      }

    }
    return lookupNameToConfig;
  }

  @Nullable
  private static LookupExtractorFactoryContainer tryConvertObjectToLookupConfig(
      String lookupName,
      Object o,
      ObjectMapper objectMapper)
  {
    try {
      byte[] lookupConfigBytes = objectMapper.writeValueAsBytes(o);
      return objectMapper.readValue(
          lookupConfigBytes,
          LookupExtractorFactoryContainer.class
      );
    }
    catch (IOException e) {
      LOG.warn("Lookup [%s] could not be serialized properly. Please check its configuration. Error: %s",
               lookupName,
               e.getMessage()
      );
    }
    return null;
  }
}
