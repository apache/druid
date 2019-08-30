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

package org.apache.druid.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Key;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigTesterBase;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

/**
 *
 */
public class ZkPathsConfigTest extends JsonConfigTesterBase<ZkPathsConfig>
{
  @Test
  public void testOverrideBaseOnlyConfig()
      throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, IOException
  {
    JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();

    JsonConfigProvider<ZkPathsConfig> zkPathsConfig = JsonConfigProvider.of(CONFIG_PREFIX, ZkPathsConfig.class);
    testProperties.clear();
    String base = UUID.randomUUID().toString();
    testProperties.put(StringUtils.format("%s.base", CONFIG_PREFIX), base);
    zkPathsConfig.inject(testProperties, configurator);

    propertyValues.clear();
    propertyValues.put(StringUtils.format("%s.base", CONFIG_PREFIX), base);
    propertyValues.put(StringUtils.format("%s.propertiesPath", CONFIG_PREFIX), ZKPaths.makePath(base, "properties"));
    propertyValues.put(StringUtils.format("%s.announcementsPath", CONFIG_PREFIX), ZKPaths.makePath(base, "announcements"));
    propertyValues.put(StringUtils.format("%s.servedSegmentsPath", CONFIG_PREFIX), ZKPaths.makePath(base, "servedSegments"));
    propertyValues.put(StringUtils.format("%s.liveSegmentsPath", CONFIG_PREFIX), ZKPaths.makePath(base, "segments"));
    propertyValues.put(StringUtils.format("%s.coordinatorPath", CONFIG_PREFIX), ZKPaths.makePath(base, "coordinator"));
    propertyValues.put(StringUtils.format("%s.loadQueuePath", CONFIG_PREFIX), ZKPaths.makePath(base, "loadQueue"));
    propertyValues.put(StringUtils.format("%s.connectorPath", CONFIG_PREFIX), ZKPaths.makePath(base, "connector"));

    ZkPathsConfig zkPathsConfigObj = zkPathsConfig.get().get();
    validateEntries(zkPathsConfigObj);
    Assert.assertEquals(propertyValues.size(), assertions);

    ObjectMapper jsonMapper = injector.getProvider(Key.get(ObjectMapper.class, Json.class)).get();
    String jsonVersion = jsonMapper.writeValueAsString(zkPathsConfigObj);

    ZkPathsConfig zkPathsConfigObjDeSer = jsonMapper.readValue(jsonVersion, ZkPathsConfig.class);

    Assert.assertEquals(zkPathsConfigObj, zkPathsConfigObjDeSer);
  }
}
