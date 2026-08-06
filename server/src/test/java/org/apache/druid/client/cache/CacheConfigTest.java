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

package org.apache.druid.client.cache;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.ProvisionException;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.Query;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;


/**
 *
 */
public class CacheConfigTest
{
  static Injector injector;
  static JsonConfigurator configurator;
  JsonConfigProvider<CacheConfig> configProvider;
  private static final String PROPERTY_PREFIX = "org.apache.druid.collections.test.cache";

  @BeforeAll
  public static void populateStatics()
  {
    injector = GuiceInjectors.makeStartupInjectorWithModules(ImmutableList.<com.google.inject.Module>of(new CacheConfigTestModule()));
    configurator = injector.getBinding(JsonConfigurator.class).getProvider().get();
  }

  private static class CacheConfigTestModule implements DruidModule
  {

    @Override
    public List<? extends Module> getJacksonModules()
    {
      return ImmutableList.<Module>of(new SimpleModule());
    }

    @Override
    public void configure(Binder binder)
    {
      JsonConfigProvider.bind(binder, PROPERTY_PREFIX, CacheConfig.class);
    }
  }

  private Properties properties = new Properties();

  @BeforeEach
  public void setupTest()
  {
    properties.clear();
    configProvider = JsonConfigProvider.of(PROPERTY_PREFIX, CacheConfig.class);
  }

  @Test
  public void testInjection1()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "5");
    properties.put(PROPERTY_PREFIX + ".populateCache", "true");
    properties.put(PROPERTY_PREFIX + ".useCache", "true");
    properties.put(PROPERTY_PREFIX + ".unCacheable", "[\"a\",\"b\"]");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get();

    injector.injectMembers(config);
    Assertions.assertEquals(5, config.getNumBackgroundThreads());
    Assertions.assertEquals(true, config.isPopulateCache());
    Assertions.assertEquals(true, config.isUseCache());
  }

  @Test
  public void testInjection2()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "99");
    properties.put(PROPERTY_PREFIX + ".populateCache", "false");
    properties.put(PROPERTY_PREFIX + ".useCache", "false");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get();

    Assertions.assertEquals(99, config.getNumBackgroundThreads());
    Assertions.assertEquals(false, config.isPopulateCache());
    Assertions.assertEquals(false, config.isUseCache());
  }

  @Test
  public void testValidationError()
  {
    org.junit.jupiter.api.Assertions.assertThrows(ProvisionException.class, () -> {
      properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "-1");

      configProvider.inject(properties, configurator);
      CacheConfig config = configProvider.get();
      Assertions.assertNotEquals(-1, config.getNumBackgroundThreads());
    });
  }


  @Test
  public void testValidationInsaneError()
  {
    org.junit.jupiter.api.Assertions.assertThrows(ProvisionException.class, () -> {
      properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "BABBA YAGA");
      configProvider.inject(properties, configurator);
      CacheConfig config = configProvider.get();
      throw new IllegalStateException("Should have already failed");
    });
  }

  @Test
  public void testTRUE()
  {
    properties.put(PROPERTY_PREFIX + ".populateCache", "TRUE");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get();
    Assertions.assertTrue(config.isPopulateCache());
  }

  @Test
  public void testUppercaseFalse()
  {
    properties.put(PROPERTY_PREFIX + ".populateCache", "FALSE");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get();
    Assertions.assertFalse(config.isPopulateCache());
  }


  @Test
  public void testMixedCaseFalseIsRejected()
  {
    org.junit.jupiter.api.Assertions.assertThrows(ProvisionException.class, () -> {
      properties.put(PROPERTY_PREFIX + ".populateCache", "FaLse");
      configProvider.inject(properties, configurator);
      CacheConfig config = configProvider.get();
      throw new IllegalStateException("Should have already failed");
    });
  }

  @Test
  public void testDefaultUnCacheableList()
  {
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get();
    injector.injectMembers(config);

    Assertions.assertFalse(config.isQueryCacheable(Query.SCAN));

    properties.clear();
    configProvider = JsonConfigProvider.of(PROPERTY_PREFIX, CacheConfig.class);

    properties.put(PROPERTY_PREFIX + ".unCacheable", "[]");
    configProvider.inject(properties, configurator);
    CacheConfig config2 = configProvider.get();
    injector.injectMembers(config2);
    Assertions.assertTrue(config2.isQueryCacheable(Query.SCAN));
  }
}
