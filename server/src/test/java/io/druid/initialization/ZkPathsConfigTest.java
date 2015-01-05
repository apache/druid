/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
