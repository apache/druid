/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.initialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Key;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigTesterBase;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.annotations.Json;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;
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

    JsonConfigProvider<ZkPathsConfig> zkPathsConfig = JsonConfigProvider.of(configPrefix, ZkPathsConfig.class);
    testProperties.clear();
    String base = UUID.randomUUID().toString();
    testProperties.put(String.format("%s.base", configPrefix), base);
    zkPathsConfig.inject(testProperties, configurator);

    propertyValues.clear();
    propertyValues.put(String.format("%s.base", configPrefix), base);
    propertyValues.put(String.format("%s.propertiesPath", configPrefix), ZKPaths.makePath(base, "properties"));
    propertyValues.put(String.format("%s.announcementsPath", configPrefix), ZKPaths.makePath(base, "announcements"));
    propertyValues.put(String.format("%s.servedSegmentsPath", configPrefix), ZKPaths.makePath(base, "servedSegments"));
    propertyValues.put(String.format("%s.liveSegmentsPath", configPrefix), ZKPaths.makePath(base, "segments"));
    propertyValues.put(String.format("%s.coordinatorPath", configPrefix), ZKPaths.makePath(base, "coordinator"));
    propertyValues.put(String.format("%s.loadQueuePath", configPrefix), ZKPaths.makePath(base, "loadQueue"));
    propertyValues.put(String.format("%s.connectorPath", configPrefix), ZKPaths.makePath(base, "connector"));

    ZkPathsConfig zkPathsConfigObj = zkPathsConfig.get().get();
    validateEntries(zkPathsConfigObj);
    Assert.assertEquals(propertyValues.size(), assertions);

    ObjectMapper jsonMapper = injector.getProvider(Key.get(ObjectMapper.class, Json.class)).get();
    String jsonVersion = jsonMapper.writeValueAsString(zkPathsConfigObj);

    ZkPathsConfig zkPathsConfigObjDeSer = jsonMapper.readValue(jsonVersion, ZkPathsConfig.class);

    Assert.assertEquals(zkPathsConfigObj, zkPathsConfigObjDeSer);
  }
}
