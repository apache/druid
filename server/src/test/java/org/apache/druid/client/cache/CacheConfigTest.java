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
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 *
 */
class CacheConfigTest
{
  static Injector injector;
  static JsonConfigurator configurator;
  private JsonConfigProvider<CacheConfig> configProvider;
  private static final String PROPERTY_PREFIX = "org.apache.druid.collections.test.cache";

  @BeforeAll
  static void populateStatics()
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

  private final Properties properties = new Properties();

  @BeforeEach
  void setupTest()
  {
    properties.clear();
    configProvider = JsonConfigProvider.of(PROPERTY_PREFIX, CacheConfig.class);
  }

  @Test
  void testInjection1()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "5");
    properties.put(PROPERTY_PREFIX + ".populateCache", "true");
    properties.put(PROPERTY_PREFIX + ".useCache", "true");
    properties.put(PROPERTY_PREFIX + ".unCacheable", "[\"a\",\"b\"]");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get();

    injector.injectMembers(config);
    assertEquals(5, config.getNumBackgroundThreads());
    assertTrue(config.isPopulateCache());
    assertTrue(config.isUseCache());
  }

  @Test
  void testInjection2()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "99");
    properties.put(PROPERTY_PREFIX + ".populateCache", "false");
    properties.put(PROPERTY_PREFIX + ".useCache", "false");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get();

    assertEquals(99, config.getNumBackgroundThreads());
    assertFalse(config.isPopulateCache());
    assertFalse(config.isUseCache());
  }

  @Test
  void testValidationError()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "-1");

    configProvider.inject(properties, configurator);

    ProvisionException exception = assertThrows(ProvisionException.class, () -> {
      CacheConfig config = configProvider.get();
      assertNotEquals(-1, config.getNumBackgroundThreads());
    });

    assertEquals("Unable to provision, see the following errors:\n"
                 + "\n"
                 + "1) org.apache.druid.collections.test.cache.numBackgroundThreads - must be greater than or equal to 0\n"
                 + "\n"
                 + "1 error", exception.getMessage());
  }

  @Test
  public void testValidationInsaneError()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "BABBA YAGA");
    configProvider.inject(properties, configurator);
    ProvisionException exception = assertThrows(ProvisionException.class, () -> configProvider.get());
    assertEquals("Unable to provision, see the following errors:\n"
                 + "\n"
                 + "1) Problem parsing object at prefix[org.apache.druid.collections.test.cache]: Cannot deserialize value of type `int` from String \"BABBA YAGA\": not a valid Integer value\n"
                 + " at [Source: UNKNOWN; line: -1, column: -1] (through reference chain: org.apache.druid.client.cache.CacheConfig[\"numBackgroundThreads\"]).\n"
                 + "\n"
                 + "1 error", exception.getMessage());
  }

  @Test
  void testTRUE()
  {
    properties.put(PROPERTY_PREFIX + ".populateCache", "TRUE");
    configProvider.inject(properties, configurator);
    ProvisionException exception = assertThrows(ProvisionException.class, () -> configProvider.get());
    assertEquals("Unable to provision, see the following errors:\n"
                 + "\n"
                 + "1) Problem parsing object at prefix[org.apache.druid.collections.test.cache]: Cannot deserialize value of type `boolean` from String \"TRUE\": only \"true\" or \"false\" recognized\n"
                 + " at [Source: UNKNOWN; line: -1, column: -1] (through reference chain: org.apache.druid.client.cache.CacheConfig[\"populateCache\"]).\n"
                 + "\n"
                 + "1 error", exception.getMessage());
  }

  @Test
  void testFALSE()
  {
    properties.put(PROPERTY_PREFIX + ".populateCache", "FALSE");
    configProvider.inject(properties, configurator);
    ProvisionException exception = assertThrows(ProvisionException.class, () -> configProvider.get());
    assertEquals("Unable to provision, see the following errors:\n"
                 + "\n"
                 + "1) Problem parsing object at prefix[org.apache.druid.collections.test.cache]: Cannot deserialize value of type `boolean` from String \"FALSE\": only \"true\" or \"false\" recognized\n"
                 + " at [Source: UNKNOWN; line: -1, column: -1] (through reference chain: org.apache.druid.client.cache.CacheConfig[\"populateCache\"]).\n"
                 + "\n"
                 + "1 error", exception.getMessage());
  }


  @Test
  public void testFaLse()
  {
    properties.put(PROPERTY_PREFIX + ".populateCache", "FaLse");
    configProvider.inject(properties, configurator);
    ProvisionException exception = assertThrows(ProvisionException.class, () -> configProvider.get());
    assertEquals("Unable to provision, see the following errors:\n"
                 + "\n"
                 + "1) Problem parsing object at prefix[org.apache.druid.collections.test.cache]: Cannot deserialize value of type `boolean` from String \"FaLse\": only \"true\" or \"false\" recognized\n"
                 + " at [Source: UNKNOWN; line: -1, column: -1] (through reference chain: org.apache.druid.client.cache.CacheConfig[\"populateCache\"]).\n"
                 + "\n"
                 + "1 error", exception.getMessage());
  }
}
