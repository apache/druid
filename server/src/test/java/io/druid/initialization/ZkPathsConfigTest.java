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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.curator.CuratorConfig;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Smile;
import io.druid.server.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 *
 */
public class ZkPathsConfigTest
{

  private static final Module simpleZkConfigModule = new Module()
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
      binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
      JsonConfigProvider.bind(binder, configPrefix, ZkPathsConfig.class);
    }
  };


  private static final Collection<String> clobberableProperties = new ArrayList<>();
  private static Injector injector;

  @BeforeClass
  public static void setup()
  {
    for (Field field : ZkPathsConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        clobberableProperties.add(String.format("%s.%s", configPrefix, field.getName()));
      }
    }
    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Object>of(simpleZkConfigModule)
    );
  }

  private Map<String, String> propertyValues = new HashMap<>();
  private int assertions = 0;
  private Properties testProperties = new Properties();

  @Before
  public void setupTest()
  {
    propertyValues.clear();
    for (String property : clobberableProperties) {
      propertyValues.put(property, UUID.randomUUID().toString());
    }
    testProperties.putAll(System.getProperties());
    testProperties.putAll(propertyValues);
    assertions = 0;
  }

  private void validateEntries(ZkPathsConfig zkPathsConfig)
      throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    for (Field field : ZkPathsConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        String property = String.format("%s.%s", configPrefix, field.getName());
        String getter = String.format(
            "get%s%s",
            field.getName().substring(0, 1).toUpperCase(),
            field.getName().substring(1)
        );
        Method method = ZkPathsConfig.class.getDeclaredMethod(getter);
        Assert.assertEquals(propertyValues.get(property), method.invoke(zkPathsConfig));
        ++assertions;
      }
    }
  }

  private static final String configPrefix = "druid.test.zk.path";

  @Test
  public void testSimpleConfig() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();

    JsonConfigProvider<ZkPathsConfig> zkPathsConfig = JsonConfigProvider.of(configPrefix, ZkPathsConfig.class);
    zkPathsConfig.inject(testProperties, configurator);

    validateEntries(zkPathsConfig.get().get());
    Assert.assertEquals(clobberableProperties.size(), assertions);
  }

  @Test
  public void testSimpleJsonSerDe() throws IOException
  {
    JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
    JsonConfigProvider<ZkPathsConfig> zkPathsConfig = JsonConfigProvider.of(configPrefix, ZkPathsConfig.class);
    testProperties.clear();
    zkPathsConfig.inject(testProperties, configurator);

    ZkPathsConfig zkPathsConfigObj = zkPathsConfig.get().get();

    ObjectMapper jsonMapper = injector.getProvider(Key.get(ObjectMapper.class, Json.class)).get();
    String jsonVersion = jsonMapper.writeValueAsString(zkPathsConfigObj);

    ZkPathsConfig zkPathsConfigObjDeSer = jsonMapper.readValue(jsonVersion, ZkPathsConfig.class);

    Assert.assertEquals(zkPathsConfigObj, zkPathsConfigObjDeSer);
  }


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
    propertyValues.put(String.format("%s.propertiesPath",configPrefix), ZKPaths.makePath(base,"properties"));
    propertyValues.put(String.format("%s.announcementsPath",configPrefix), ZKPaths.makePath(base,"announcements"));
    propertyValues.put(String.format("%s.servedSegmentsPath",configPrefix), ZKPaths.makePath(base,"servedSegments"));
    propertyValues.put(String.format("%s.liveSegmentsPath",configPrefix), ZKPaths.makePath(base,"segments"));
    propertyValues.put(String.format("%s.coordinatorPath",configPrefix), ZKPaths.makePath(base,"coordinator"));
    propertyValues.put(String.format("%s.loadQueuePath",configPrefix), ZKPaths.makePath(base,"loadQueue"));
    propertyValues.put(String.format("%s.connectorPath",configPrefix), ZKPaths.makePath(base,"connector"));

    ZkPathsConfig zkPathsConfigObj = zkPathsConfig.get().get();
    validateEntries(zkPathsConfigObj);
    Assert.assertEquals(clobberableProperties.size(), assertions);

    ObjectMapper jsonMapper = injector.getProvider(Key.get(ObjectMapper.class, Json.class)).get();
    String jsonVersion = jsonMapper.writeValueAsString(zkPathsConfigObj);

    ZkPathsConfig zkPathsConfigObjDeSer = jsonMapper.readValue(jsonVersion, ZkPathsConfig.class);

    Assert.assertEquals(zkPathsConfigObj, zkPathsConfigObjDeSer);
  }
}
