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
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

  @BeforeClass
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

  @Before
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
    CacheConfig config = configProvider.get().get();

    injector.injectMembers(config);
    Assert.assertEquals(5, config.getNumBackgroundThreads());
    Assert.assertEquals(true, config.isPopulateCache());
    Assert.assertEquals(true, config.isUseCache());
  }
  @Test
  public void testInjection2()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "99");
    properties.put(PROPERTY_PREFIX + ".populateCache", "false");
    properties.put(PROPERTY_PREFIX + ".useCache", "false");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();

    Assert.assertEquals(99, config.getNumBackgroundThreads());
    Assert.assertEquals(false, config.isPopulateCache());
    Assert.assertEquals(false, config.isUseCache());
  }

  @Test(expected = ProvisionException.class)
  public void testValidationError()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "-1");

    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    Assert.assertNotEquals(-1, config.getNumBackgroundThreads());
  }


  @Test(expected = ProvisionException.class)
  public void testValidationInsaneError()
  {
    properties.put(PROPERTY_PREFIX + ".numBackgroundThreads", "BABBA YAGA");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    throw new IllegalStateException("Should have already failed");
  }

  @Test(expected = ProvisionException.class)
  public void testTRUE()
  {
    properties.put(PROPERTY_PREFIX + ".populateCache", "TRUE");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    throw new IllegalStateException("Should have already failed");
  }

  @Test(expected = ProvisionException.class)
  public void testFALSE()
  {
    properties.put(PROPERTY_PREFIX + ".populateCache", "FALSE");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    throw new IllegalStateException("Should have already failed");
  }


  @Test(expected = ProvisionException.class)
  public void testFaLse()
  {
    properties.put(PROPERTY_PREFIX + ".populateCache", "FaLse");
    configProvider.inject(properties, configurator);
    CacheConfig config = configProvider.get().get();
    throw new IllegalStateException("Should have already failed");
  }


}
