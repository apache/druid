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

package io.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import io.druid.curator.CuratorConfig;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.JsonConfigurator;
import io.druid.initialization.Initialization;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 *
 */
public class IndexerZkConfigTest
{
  private static final String indexerPropertyString = "test.druid.zk.paths.indexer";
  private static final String zkServiceConfigString = "test.druid.zk.paths";
  private static final Collection<String> clobberableProperties = new ArrayList<>();

  private static final Module simpleZkConfigModule = new Module()
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
      binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
      // See IndexingServiceModuleHelper
      JsonConfigProvider.bind(binder, indexerPropertyString, IndexerZkConfig.class);
      JsonConfigProvider.bind(
          binder, zkServiceConfigString,
          CuratorConfig.class
      );
    }
  };

  private static final Map<String, String> priorValues = new HashMap<>();

  @BeforeClass
  public static void setup()
  {
    for (Field field : IndexerZkConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        clobberableProperties.add(String.format("%s.%s", indexerPropertyString, field.getName()));
      }
    }
    for (Field field : ZkPathsConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        clobberableProperties.add(String.format("%s.%s", zkServiceConfigString, field.getName()));
      }
    }
    for (String clobberableProperty : clobberableProperties) {
      priorValues.put(clobberableProperty, System.getProperty(clobberableProperty));
    }
  }

  @AfterClass
  public static void cleanup()
  {
    for (Map.Entry<String, String> entry : priorValues.entrySet()) {
      if (null != entry.getKey() && null != entry.getValue()) {
        System.setProperty(entry.getKey(), entry.getValue());
      }
    }
  }

  private Map<String, String> propertyValues = new HashMap<>();
  private int assertions = 0;

  @Before
  public void setupTest()
  {
    for (String property : clobberableProperties) {
      propertyValues.put(property, UUID.randomUUID().toString());
    }
    System.getProperties().putAll(propertyValues);
    assertions = 0;
  }


  private void validateEntries(ZkPathsConfig zkPathsConfig)
      throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    for (Field field : ZkPathsConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        String property = String.format("%s.%s", zkServiceConfigString, field.getName());
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

  private void validateEntries(IndexerZkConfig indexerZkConfig)
      throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    for (Field field : IndexerZkConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        String property = String.format("%s.%s", indexerPropertyString, field.getName());
        String getter = String.format(
            "get%s%s",
            field.getName().substring(0, 1).toUpperCase(),
            field.getName().substring(1)
        );
        Method method = IndexerZkConfig.class.getDeclaredMethod(getter);
        Assert.assertEquals(propertyValues.get(property), method.invoke(indexerZkConfig));
        ++assertions;
      }
    }
  }

  @Test
  public void testSimpleConfig() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(simpleZkConfigModule)
    );
    JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();

    JsonConfigProvider<ZkPathsConfig> zkPathsConfig = JsonConfigProvider.of(zkServiceConfigString, ZkPathsConfig.class);
    zkPathsConfig.inject(System.getProperties(), configurator);

    JsonConfigProvider<IndexerZkConfig> indexerZkConfig = JsonConfigProvider.of(
        indexerPropertyString,
        IndexerZkConfig.class
    );
    indexerZkConfig.inject(System.getProperties(), configurator);

    validateEntries(indexerZkConfig.get().get());
    validateEntries(zkPathsConfig.get().get());
    Assert.assertEquals(clobberableProperties.size(), assertions);
  }
}
