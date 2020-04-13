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

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

/**
 *
 */
public class IndexerZkConfigTest
{
  private static final String INDEXER_PROPERTY_STRING = "test.druid.zk.paths.indexer";
  private static final String ZK_SERVICE_CONFIG_STRING = "test.druid.zk.paths";
  private static final Collection<String> CLOBBERABLE_PROPERTIES = new HashSet<>();

  private static final Module SIMPLE_ZK_CONFIG_MODULE = new Module()
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/test");
      binder.bindConstant().annotatedWith(Names.named("servicePort")).to(0);
      binder.bindConstant().annotatedWith(Names.named("tlsServicePort")).to(-1);
      // See IndexingServiceModuleHelper
      JsonConfigProvider.bind(binder, INDEXER_PROPERTY_STRING, IndexerZkConfig.class);
      JsonConfigProvider.bind(binder, ZK_SERVICE_CONFIG_STRING, ZkPathsConfig.class);
    }
  };

  @BeforeClass
  public static void setup()
  {
    for (Field field : IndexerZkConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        CLOBBERABLE_PROPERTIES.add(StringUtils.format("%s.%s", INDEXER_PROPERTY_STRING, field.getName()));
      }
    }
    for (Field field : ZkPathsConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        CLOBBERABLE_PROPERTIES.add(StringUtils.format("%s.%s", ZK_SERVICE_CONFIG_STRING, field.getName()));
      }
    }
  }

  private Properties propertyValues = new Properties();
  private int assertions = 0;

  @Before
  public void setupTest()
  {
    for (String property : CLOBBERABLE_PROPERTIES) {
      propertyValues.put(property, UUID.randomUUID().toString());
    }
    assertions = 0;
  }


  private void validateEntries(ZkPathsConfig zkPathsConfig)
      throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    for (Field field : ZkPathsConfig.class.getDeclaredFields()) {
      if (null != field.getAnnotation(JsonProperty.class)) {
        String property = StringUtils.format("%s.%s", ZK_SERVICE_CONFIG_STRING, field.getName());
        String getter = StringUtils.format(
            "get%s%s",
            StringUtils.toUpperCase(field.getName().substring(0, 1)),
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
        String property = StringUtils.format("%s.%s", INDEXER_PROPERTY_STRING, field.getName());
        String getter = StringUtils.format(
            "get%s%s",
            StringUtils.toUpperCase(field.getName().substring(0, 1)),
            field.getName().substring(1)
        );
        Method method = IndexerZkConfig.class.getDeclaredMethod(getter);
        Assert.assertEquals(propertyValues.get(property), method.invoke(indexerZkConfig));
        ++assertions;
      }
    }
  }

  @Test
  public void testNullConfig()
  {
    propertyValues.clear();

    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(SIMPLE_ZK_CONFIG_MODULE)
    );
    JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();

    JsonConfigProvider<ZkPathsConfig> zkPathsConfig = JsonConfigProvider.of(ZK_SERVICE_CONFIG_STRING, ZkPathsConfig.class);
    zkPathsConfig.inject(propertyValues, configurator);

    JsonConfigProvider<IndexerZkConfig> indexerZkConfig = JsonConfigProvider.of(
        INDEXER_PROPERTY_STRING,
        IndexerZkConfig.class
    );
    indexerZkConfig.inject(propertyValues, configurator);

    Assert.assertEquals("/druid/indexer/tasks", indexerZkConfig.get().get().getTasksPath());
  }

  @Test
  public void testSimpleConfig() throws IllegalAccessException, NoSuchMethodException, InvocationTargetException
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(SIMPLE_ZK_CONFIG_MODULE)
    );
    JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();

    JsonConfigProvider<ZkPathsConfig> zkPathsConfig = JsonConfigProvider.of(ZK_SERVICE_CONFIG_STRING, ZkPathsConfig.class);
    zkPathsConfig.inject(propertyValues, configurator);

    JsonConfigProvider<IndexerZkConfig> indexerZkConfig = JsonConfigProvider.of(
        INDEXER_PROPERTY_STRING,
        IndexerZkConfig.class
    );
    indexerZkConfig.inject(propertyValues, configurator);


    IndexerZkConfig zkConfig = indexerZkConfig.get().get();
    ZkPathsConfig zkPathsConfig1 = zkPathsConfig.get().get();

    validateEntries(zkConfig);
    validateEntries(zkPathsConfig1);
    Assert.assertEquals(CLOBBERABLE_PROPERTIES.size(), assertions);
  }



  @Test
  public void testIndexerBaseOverride()
  {
    final String overrideValue = "/foo/bar/baz";
    final String indexerPropertyKey = INDEXER_PROPERTY_STRING + ".base";
    final String priorValue = System.getProperty(indexerPropertyKey);
    System.setProperty(indexerPropertyKey, overrideValue); // Set it here so that the binding picks it up
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(SIMPLE_ZK_CONFIG_MODULE)
    );
    propertyValues.clear();
    propertyValues.setProperty(indexerPropertyKey, overrideValue); // Have to set it here as well annoyingly enough


    JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();

    JsonConfigProvider<IndexerZkConfig> indexerPathsConfig = JsonConfigProvider.of(
        INDEXER_PROPERTY_STRING,
        IndexerZkConfig.class
    );
    indexerPathsConfig.inject(propertyValues, configurator);
    IndexerZkConfig indexerZkConfig = indexerPathsConfig.get().get();


    // Rewind value before we potentially fail
    if (priorValue == null) {
      System.clearProperty(indexerPropertyKey);
    } else {
      System.setProperty(indexerPropertyKey, priorValue);
    }

    Assert.assertEquals(overrideValue, indexerZkConfig.getBase());
    Assert.assertEquals(overrideValue + "/announcements", indexerZkConfig.getAnnouncementsPath());
  }

  @Test
  public void testExactConfig()
  {
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.of(SIMPLE_ZK_CONFIG_MODULE)
    );
    propertyValues.setProperty(ZK_SERVICE_CONFIG_STRING + ".base", "/druid/metrics");


    JsonConfigurator configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();

    JsonConfigProvider<ZkPathsConfig> zkPathsConfig = JsonConfigProvider.of(
        ZK_SERVICE_CONFIG_STRING,
        ZkPathsConfig.class
    );

    zkPathsConfig.inject(propertyValues, configurator);

    ZkPathsConfig zkPathsConfig1 = zkPathsConfig.get().get();

    IndexerZkConfig indexerZkConfig = new IndexerZkConfig(zkPathsConfig1, null, null, null, null);

    Assert.assertEquals("/druid/metrics/indexer", indexerZkConfig.getBase());
    Assert.assertEquals("/druid/metrics/indexer/announcements", indexerZkConfig.getAnnouncementsPath());
  }

  @Test
  public void testFullOverride() throws Exception
  {
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    final ZkPathsConfig zkPathsConfig = new ZkPathsConfig();

    IndexerZkConfig indexerZkConfig = new IndexerZkConfig(
        zkPathsConfig,
        "/druid/prod",
        "/druid/prod/a",
        "/druid/prod/t",
        "/druid/prod/s"
    );

    Map<String, String> value = mapper.readValue(
        mapper.writeValueAsString(indexerZkConfig), JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING
    );
    IndexerZkConfig newConfig = new IndexerZkConfig(
        zkPathsConfig,
        value.get("base"),
        value.get("announcementsPath"),
        value.get("tasksPath"),
        value.get("statusPath")
    );

    Assert.assertEquals(indexerZkConfig, newConfig);
  }
}
